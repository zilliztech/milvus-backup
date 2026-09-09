# Milvus Active-Standby DR (backup + native replication)

`milvus_dr.py` is a self-contained reference for running a Milvus cluster with a
warm standby, using only open-source pieces:

- **milvus-backup** seeds the standby with a consistent snapshot of the primary
  (`restore secondary`).
- **Milvus native cluster replication** (Milvus >= v2.6.11) keeps the standby in
  sync with incremental writes after the snapshot. Replication direction is set
  with the `UpdateReplicateConfiguration` gRPC API — the same call this repo's
  `cmd/inittarget` makes — invoked here straight from Python via pymilvus.

It is the open-source equivalent of the managed "global cluster / standby
rebuild + switchover" flow. Adapt it; do not run it blind in production.

## What you must provide

1. **Two Milvus clusters, each with a DISTINCT cluster id.** On every cluster the
   id must be consistent across all three of:
   - `common.chanNamePrefix.cluster` (milvus.yaml)
   - `etcd.rootPath` (milvus.yaml)
   - the cluster id you pass to this script and to milvus-backup.

   A working dual-cluster example lives in
   [`deployment/secondary/`](../../deployment/secondary) (`upstream.yaml` /
   `downstream.yaml` / `docker-compose.yml`).

2. **A `milvus-backup` binary and two backup configs:**
   - the **create** config (`--backup-config`) must point `milvus.*` at the **primary**;
   - the **restore secondary** config (`--backup-config-secondary`) must point
     `milvus.*` at the **standby**.
   - both need `milvus.rpcChannelName` and the storage section set as usual.

   (CI keeps a single `configs/backup.yaml` and rewrites `milvus.port` between the
   two steps — see `.github/workflows/main.yaml`. Two files is just cleaner.)

3. **pymilvus** (`pip install pymilvus`). No other Python dependency.

## Rebuild the standby

Snapshot the primary, stream it into the standby, then start replication:

```bash
python milvus_dr.py rebuild \
    --upstream-uri   http://PRIMARY_HOST:19530 --upstream-cluster-id   prod-primary \
    --downstream-uri http://STANDBY_HOST:19530 --downstream-cluster-id prod-standby \
    --backup-bin ./milvus-backup --backup-workdir . \
    --backup-config backup-primary.yaml \
    --backup-config-secondary backup-standby.yaml \
    --verify
```

Steps performed (identical to the `test-backup-restore-secondary` CI job):

1. `milvus-backup create` — snapshot the primary.
2. `UpdateReplicateConfiguration` on the standby (= `inittarget --target downstream`).
3. `milvus-backup restore secondary` — stream the snapshot into the standby.
4. `UpdateReplicateConfiguration` on the primary (= `inittarget --target upstream`)
   — incremental replication primary -> standby begins.

`--verify` compares per-collection row counts on both sides afterwards (with
retries to absorb replication lag).

## Switch over (promote the standby)

```bash
python milvus_dr.py switchover \
    --upstream-uri   http://PRIMARY_HOST:19530 --upstream-cluster-id   prod-primary \
    --downstream-uri http://STANDBY_HOST:19530 --downstream-cluster-id prod-standby
```

This reverses the replication topology to standby -> old primary and prints the
new primary URI. Like the managed control-plane switchover (whose workflow just
updates the replicate configuration and instance roles), it simply applies the
new topology — native replication is checkpoint-coordinated, so there is **no**
separate "wait for the standby to catch up" step.

Then **point your clients at the new primary.** The old primary now trails it as
a standby.

## Diagnostics

```bash
# Replication checkpoints (time_tick per pchannel) reported by the standby:
python milvus_dr.py status     --upstream-uri ... --downstream-uri ...

# Per-collection row-count comparison:
python milvus_dr.py verify     --upstream-uri ... --downstream-uri ... [--collections a,b]

# Raw inittarget equivalent (set topology directly):
python milvus_dr.py replicate-config --upstream-uri ... --downstream-uri ... \
    --direction up2down --target both
```

## Notes & limits

- `--pchannel-num` (default 16) must match the deployment's DML channel count.
- Cross-host / k8s: set `--upstream-inter-uri` / `--downstream-inter-uri` to the
  address each cluster uses to reach the *other* (may differ from the address you
  dial from). Use `--upstream-grpc` / `--downstream-grpc` to override the dial host:port.
- Channels are insecure (matching `inittarget`). For TLS-enabled Milvus, switch
  `grpc.insecure_channel` to `grpc.secure_channel` in the script.
- `switchover` does not wait for the standby to catch up — it applies the
  reversed topology directly, matching the managed control-plane behavior. Use
  the `status` subcommand first if you want to eyeball replication checkpoints
  before promoting.
- Requires Milvus with cluster-replication support (>= v2.6.11).
