#!/usr/bin/env python3
"""
Milvus active-standby disaster recovery (DR) reference script.

This is a *reference* you can adapt to your own environment. It wires together
two open-source tools to give a Milvus cluster a warm standby:

  * milvus-backup  -- takes a consistent snapshot of the primary and streams it
                      into the standby ("restore secondary").
  * Milvus native cluster replication -- after the snapshot is loaded, the
                      primary continuously replicates incremental writes to the
                      standby. Replication direction is controlled by the
                      UpdateReplicateConfiguration gRPC API (the same call the
                      `inittarget` helper in this repo makes), invoked here
                      directly from Python via pymilvus' generated stubs.

It implements two operations:

  rebuild     Seed/re-seed the standby from the primary (backup snapshot) and
              then turn on incremental replication primary -> standby. This is
              the "备库重建 / standby rebuild" flow and mirrors, step for step,
              the `test-backup-restore-secondary` job in
              .github/workflows/main.yaml:

                  1. milvus-backup create                 (snapshot primary)
                  2. UpdateReplicateConfiguration(down)   (= inittarget --target downstream)
                  3. milvus-backup restore secondary      (stream snapshot into standby)
                  4. UpdateReplicateConfiguration(up)     (= inittarget --target upstream)
                                                          -> incremental replication starts

  switchover  Promote the standby to primary by reversing the replication
              topology (standby -> old primary), then point your clients at the
              new primary. Like the managed control-plane switchover, this simply
              applies the new topology -- native replication is checkpoint-
              coordinated, so there is no separate "wait for the standby to catch
              up" step.

Prerequisites
-------------
  * Two Milvus clusters (>= v2.6.11, which is where cluster replication landed),
    each with a DISTINCT cluster id. The cluster id must match, on each side:
        - common.chanNamePrefix.cluster   (milvus.yaml)
        - etcd.rootPath                   (milvus.yaml)
        - the cluster id you pass to this script / to milvus-backup.
    See deployment/secondary/{upstream,downstream}.yaml for a working example.
  * A `milvus-backup` binary plus a backup.yaml for each side. The `create`
    config must point milvus.* at the PRIMARY; the `restore secondary` config
    must point milvus.* at the STANDBY. (The CI edits a single configs/backup.yaml
    in place between the two steps; here you pass --backup-config / --backup-config-secondary.)
  * pymilvus installed (`pip install pymilvus`). No other Python dependencies.

This script does NOT deploy Milvus, manage storage, or stop your application's
writes. Those are environment-specific and left to you; the script prints clear
instructions where manual action is required.

Usage examples
--------------
  # Rebuild the standby (snapshot + start replication), then sanity-check it:
  python milvus_dr.py rebuild \
      --upstream-uri   http://10.0.0.1:19530 --upstream-cluster-id   prod-primary \
      --downstream-uri http://10.0.0.2:19530 --downstream-cluster-id prod-standby \
      --backup-bin ./milvus-backup \
      --backup-config backup-primary.yaml \
      --backup-config-secondary backup-standby.yaml \
      --verify

  # Promote the standby to primary (reverses the replication direction):
  python milvus_dr.py switchover \
      --upstream-uri   http://10.0.0.1:19530 --upstream-cluster-id   prod-primary \
      --downstream-uri http://10.0.0.2:19530 --downstream-cluster-id prod-standby

  # Diagnostics only: dump replication checkpoints, or compare row counts:
  python milvus_dr.py status --upstream-uri ... --downstream-uri ...
  python milvus_dr.py verify --upstream-uri ... --downstream-uri ...
"""

import argparse
import base64
import os
import subprocess
import sys
import time
from urllib.parse import urlparse

import grpc
from pymilvus import Collection, connections, utility
from pymilvus.grpc_gen import common_pb2, milvus_pb2, milvus_pb2_grpc


# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #
def log(msg):
    print(f"[milvus-dr] {msg}", flush=True)


def grpc_addr(uri):
    """Turn http://host:port (or bare host:port) into the host:port gRPC dials."""
    if "://" in uri:
        parsed = urlparse(uri)
        host = parsed.hostname or "127.0.0.1"
        port = parsed.port or 19530
        return f"{host}:{port}"
    return uri


def pchannels_of(cluster_id, num):
    """Milvus names its DML pchannels `<cluster_id>-rootcoord-dml_<i>`.

    This naming is what `inittarget` uses and what the replicate configuration
    expects; it must match the real channel count of the deployment.
    """
    return [f"{cluster_id}-rootcoord-dml_{i}" for i in range(num)]


def auth_metadata(token):
    """Milvus gRPC auth: a single `authorization` header carrying base64(token).

    `token` is either `user:password` or an API key, matching pymilvus' own
    convention (see pymilvus/client/grpc_handler.py).
    """
    if not token:
        return []
    return [("authorization", base64.b64encode(token.encode()).decode())]


def status_ok(status):
    """common.Status is success when code==0 (new field) / error_code==Success."""
    code = getattr(status, "code", 0) or 0
    err = getattr(status, "error_code", 0) or 0
    return code == 0 and err in (0, common_pb2.ErrorCode.Success)


# --------------------------------------------------------------------------- #
# Cluster description + gRPC plumbing
# --------------------------------------------------------------------------- #
class Cluster:
    """Everything the script needs to know about one Milvus cluster.

    dial_addr  -- host:port this script connects to (admin/control plane).
    inter_uri  -- address the OTHER cluster uses to reach this one for
                  replication. On a single host this equals dial_addr; across
                  hosts/k8s it is the routable service address and may differ.
    """

    def __init__(self, role, uri, cluster_id, pchannel_num, token, inter_uri=None, grpc_override=None):
        self.role = role  # "upstream" or "downstream"
        self.uri = uri
        self.cluster_id = cluster_id
        self.pchannel_num = pchannel_num
        self.token = token
        self.dial_addr = grpc_override or grpc_addr(uri)
        self.inter_uri = inter_uri or grpc_addr(uri)

    def milvus_cluster(self):
        return common_pb2.MilvusCluster(
            cluster_id=self.cluster_id,
            connection_param=common_pb2.ConnectionParam(uri=self.inter_uri, token=self.token),
            pchannels=pchannels_of(self.cluster_id, self.pchannel_num),
        )

    def stub(self):
        # Insecure channel mirrors `inittarget`. For TLS-enabled Milvus, swap in
        # grpc.secure_channel(...) with the appropriate credentials.
        channel = grpc.insecure_channel(self.dial_addr)
        return milvus_pb2_grpc.MilvusServiceStub(channel), channel


def build_replicate_config(upstream, downstream, source, target):
    """Both clusters are always declared; only the topology edge points one way.

    `source`/`target` are Cluster objects: replication flows source -> target.
    """
    topology = common_pb2.CrossClusterTopology(
        source_cluster_id=source.cluster_id,
        target_cluster_id=target.cluster_id,
    )
    return common_pb2.ReplicateConfiguration(
        clusters=[upstream.milvus_cluster(), downstream.milvus_cluster()],
        cross_cluster_topology=[topology],
    )


def apply_replicate_config(target_cluster, config):
    """Push a ReplicateConfiguration to one cluster (= one `inittarget` call).

    The SAME configuration is applied to every cluster that participates; each
    side needs to know the full topology. `rebuild` applies it to the downstream
    first (so it is ready to receive) and then the upstream (which starts
    sending); `switchover` applies the reversed config to both.
    """
    stub, channel = target_cluster.stub()
    try:
        resp = stub.UpdateReplicateConfiguration(
            milvus_pb2.UpdateReplicateConfigurationRequest(replicate_configuration=config),
            metadata=auth_metadata(target_cluster.token),
            timeout=30,
        )
        if not status_ok(resp):
            raise RuntimeError(
                f"UpdateReplicateConfiguration on {target_cluster.role} "
                f"({target_cluster.dial_addr}) failed: {resp.reason or resp}"
            )
        log(f"replicate configuration applied on {target_cluster.role} ({target_cluster.dial_addr})")
    finally:
        channel.close()


def get_replicate_checkpoints(observer, source_cluster_id, pchannels):
    """Best-effort GetReplicateInfo per pchannel; returns {pchannel: time_tick}.

    `observer` is the Cluster we query. Semantics of GetReplicateInfo can vary by
    Milvus version, so callers must tolerate this returning {} (unimplemented or
    not yet started). We only rely on time_tick as a monotonic progress signal.
    """
    stub, channel = observer.stub()
    out = {}
    try:
        for pch in pchannels:
            try:
                resp = stub.GetReplicateInfo(
                    milvus_pb2.GetReplicateInfoRequest(
                        source_cluster_id=source_cluster_id, target_pchannel=pch
                    ),
                    metadata=auth_metadata(observer.token),
                    timeout=10,
                )
                out[pch] = int(resp.checkpoint.time_tick)
            except grpc.RpcError:
                # Channel not replicating yet, or API unavailable on this build.
                continue
    finally:
        channel.close()
    return out


# --------------------------------------------------------------------------- #
# milvus-backup CLI wrappers
# --------------------------------------------------------------------------- #
def run_backup(args, argv):
    """Invoke the milvus-backup binary, streaming its output through."""
    cmd = [os.path.abspath(args.backup_bin)] + argv
    log("$ " + " ".join(cmd))
    result = subprocess.run(cmd, cwd=args.backup_workdir)
    if result.returncode != 0:
        raise RuntimeError(f"milvus-backup exited with code {result.returncode}: {' '.join(argv)}")


def backup_create(args):
    argv = ["--config", args.backup_config, "create", "-n", args.backup_name]
    if args.backup_index_extra:
        argv.append("--backup_index_extra")
    argv += args.backup_create_extra
    run_backup(args, argv)


def restore_secondary(args, upstream, downstream):
    argv = [
        "--config", args.backup_config_secondary,
        "restore", "secondary",
        "-n", args.backup_name,
        "--source_cluster_id", upstream.cluster_id,
        "--target_cluster_id", downstream.cluster_id,
    ]
    run_backup(args, argv)


# --------------------------------------------------------------------------- #
# pymilvus-based verification
# --------------------------------------------------------------------------- #
def _connect(alias, cluster):
    connections.connect(alias, uri=cluster.uri, token=cluster.token)


def list_collections(cluster):
    alias = f"_dr_{cluster.role}"
    _connect(alias, cluster)
    try:
        return utility.list_collections(using=alias)
    finally:
        connections.disconnect(alias)


def row_counts(cluster, collection_names):
    alias = f"_dr_{cluster.role}"
    _connect(alias, cluster)
    counts = {}
    try:
        for name in collection_names:
            try:
                counts[name] = Collection(name, using=alias).num_entities
            except Exception as exc:  # noqa: BLE001 - report, do not abort
                counts[name] = f"ERR: {exc}"
    finally:
        connections.disconnect(alias)
    return counts


def verify(args, upstream, downstream, retries=6, interval=5):
    """Compare row counts on both clusters; retry to absorb replication lag."""
    names = args.collections.split(",") if args.collections else list_collections(upstream)
    names = [n for n in names if n]
    if not names:
        log("no collections found on the primary; nothing to verify")
        return True

    last_up, last_down = {}, {}
    for attempt in range(1, retries + 1):
        last_up = row_counts(upstream, names)
        last_down = row_counts(downstream, names)
        if all(last_up.get(n) == last_down.get(n) for n in names):
            break
        if attempt < retries:
            log(f"row counts differ (attempt {attempt}/{retries}); waiting {interval}s for replication...")
            time.sleep(interval)

    ok = True
    print()
    print(f"{'collection':32} {'primary':>14} {'standby':>14}   result")
    print("-" * 80)
    for n in names:
        up_c, down_c = last_up.get(n), last_down.get(n)
        match = up_c == down_c
        ok = ok and match
        print(f"{n:32} {str(up_c):>14} {str(down_c):>14}   {'OK' if match else 'MISMATCH'}")
    print()
    log("verify PASSED" if ok else "verify FAILED (counts above)")
    return ok


# --------------------------------------------------------------------------- #
# Operations
# --------------------------------------------------------------------------- #
def do_rebuild(args, upstream, downstream):
    log(f"REBUILD standby '{downstream.cluster_id}' from primary '{upstream.cluster_id}'")

    # 1. Snapshot the primary.
    log("step 1/4: create backup of the primary")
    backup_create(args)

    # 2. Prime the standby to receive replicate messages (inittarget downstream).
    log("step 2/4: configure replication topology on the standby")
    up2down = build_replicate_config(upstream, downstream, source=upstream, target=downstream)
    apply_replicate_config(downstream, up2down)

    # 3. Stream the snapshot into the standby.
    log("step 3/4: restore backup into the standby (restore secondary)")
    restore_secondary(args, upstream, downstream)

    # 4. Turn on incremental replication on the primary (inittarget upstream).
    log("step 4/4: enable incremental replication on the primary")
    apply_replicate_config(upstream, up2down)

    log("standby rebuild complete; primary is now replicating to the standby")

    if args.verify:
        log("verifying replicated data (row counts)...")
        verify(args, upstream, downstream)


def do_switchover(args, upstream, downstream):
    """Promote the standby by reversing the replication topology.

    This mirrors the managed control-plane switchover, whose workflow simply
    updates the replicate configuration and instance roles -- there is no
    "wait for the standby to catch up" step. Native replication is
    checkpoint-coordinated, so reversing the direction is itself the consistent
    handoff: applying the new topology is all that is needed at the data layer.
    """
    log(f"SWITCHOVER: promote standby '{downstream.cluster_id}' to primary")

    # Reverse the topology (standby -> old primary) and apply it to BOTH
    # clusters. Apply to the new target (old primary) first so it is ready to
    # receive, then to the new source (old standby) so it starts sending.
    log("step 1/2: apply reversed replication topology (standby -> old primary)")
    down2up = build_replicate_config(upstream, downstream, source=downstream, target=upstream)
    apply_replicate_config(upstream, down2up)
    apply_replicate_config(downstream, down2up)

    log("step 2/2: switchover complete at the data layer")
    log(f"NEW PRIMARY: {downstream.uri}  (cluster id: {downstream.cluster_id})")
    log("Point your application's writes at the new primary now. The old primary "
        "is now a standby receiving replication from it.")


def do_status(args, upstream, downstream):
    log("replication checkpoints reported by the standby (source=primary):")
    pchannels = pchannels_of(upstream.cluster_id, upstream.pchannel_num)
    ticks = get_replicate_checkpoints(downstream, upstream.cluster_id, pchannels)
    if not ticks:
        log("no replication info available (replication not started, or API unsupported on this build)")
        return
    for pch in pchannels:
        print(f"  {pch:40} time_tick={ticks.get(pch, 'n/a')}")


def do_replicate_config(args, upstream, downstream):
    """Raw `inittarget` equivalent: set the topology in a chosen direction."""
    if args.direction == "up2down":
        source, target = upstream, downstream
    else:
        source, target = downstream, upstream
    config = build_replicate_config(upstream, downstream, source=source, target=target)
    targets = {"upstream": [upstream], "downstream": [downstream], "both": [downstream, upstream]}[args.target]
    for cluster in targets:
        apply_replicate_config(cluster, config)


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def add_common(p):
    g = p.add_argument_group("clusters")
    g.add_argument("--upstream-uri", required=True, help="primary Milvus uri, e.g. http://host:19530")
    g.add_argument("--downstream-uri", required=True, help="standby Milvus uri, e.g. http://host:19530")
    g.add_argument("--upstream-cluster-id", default="backup-test-upstream")
    g.add_argument("--downstream-cluster-id", default="backup-test-downstream")
    g.add_argument("--upstream-inter-uri", default=None,
                   help="address the standby uses to reach the primary (default: derived from --upstream-uri)")
    g.add_argument("--downstream-inter-uri", default=None,
                   help="address the primary uses to reach the standby (default: derived from --downstream-uri)")
    g.add_argument("--upstream-grpc", default=None, help="override host:port this script dials for the primary")
    g.add_argument("--downstream-grpc", default=None, help="override host:port this script dials for the standby")
    g.add_argument("--pchannel-num", type=int, default=16, help="DML pchannel count (must match the deployment)")
    g.add_argument("--token", default="root:Milvus", help="auth token user:password or API key (both clusters)")


def add_backup(p):
    g = p.add_argument_group("milvus-backup")
    g.add_argument("--backup-bin", default="./milvus-backup", help="path to the milvus-backup binary")
    g.add_argument("--backup-workdir", default=".", help="cwd for milvus-backup (where configs/ live)")
    g.add_argument("--backup-name", default="my_backup")
    g.add_argument("--backup-config", default="backup.yaml",
                   help="config for `create`; its milvus.* must point at the PRIMARY")
    g.add_argument("--backup-config-secondary", default="backup.yaml",
                   help="config for `restore secondary`; its milvus.* must point at the STANDBY")
    g.add_argument("--no-backup-index-extra", dest="backup_index_extra", action="store_false",
                   help="drop the --backup_index_extra flag on create")
    g.add_argument("--backup-create-extra", nargs=argparse.REMAINDER, default=[],
                   help="extra args passed verbatim to `milvus-backup create`")


def clusters_from_args(args):
    upstream = Cluster("upstream", args.upstream_uri, args.upstream_cluster_id, args.pchannel_num,
                       args.token, inter_uri=args.upstream_inter_uri, grpc_override=args.upstream_grpc)
    downstream = Cluster("downstream", args.downstream_uri, args.downstream_cluster_id, args.pchannel_num,
                         args.token, inter_uri=args.downstream_inter_uri, grpc_override=args.downstream_grpc)
    return upstream, downstream


def main():
    parser = argparse.ArgumentParser(
        description="Milvus active-standby DR: backup-seeded standby + native replication.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_rebuild = sub.add_parser("rebuild", help="seed the standby from a backup and start replication")
    add_common(p_rebuild)
    add_backup(p_rebuild)
    p_rebuild.add_argument("--verify", action="store_true", help="compare row counts afterwards")
    p_rebuild.add_argument("--collections", default=None, help="comma-separated names for --verify (default: all)")

    p_switch = sub.add_parser("switchover", help="promote the standby to primary")
    add_common(p_switch)

    p_status = sub.add_parser("status", help="dump replication checkpoints")
    add_common(p_status)

    p_verify = sub.add_parser("verify", help="compare row counts between primary and standby")
    add_common(p_verify)
    p_verify.add_argument("--collections", default=None, help="comma-separated names (default: all on primary)")

    p_repl = sub.add_parser("replicate-config", help="raw inittarget equivalent: set replication topology")
    add_common(p_repl)
    p_repl.add_argument("--direction", choices=["up2down", "down2up"], default="up2down")
    p_repl.add_argument("--target", choices=["upstream", "downstream", "both"], default="both")

    args = parser.parse_args()
    upstream, downstream = clusters_from_args(args)

    try:
        if args.command == "rebuild":
            do_rebuild(args, upstream, downstream)
        elif args.command == "switchover":
            do_switchover(args, upstream, downstream)
        elif args.command == "status":
            do_status(args, upstream, downstream)
        elif args.command == "verify":
            ok = verify(args, upstream, downstream)
            sys.exit(0 if ok else 1)
        elif args.command == "replicate-config":
            do_replicate_config(args, upstream, downstream)
    except RuntimeError as exc:
        log(f"ERROR: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
