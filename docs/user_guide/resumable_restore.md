# Resumable Restore

On slow or unreliable object storage, a large restore can fail partway through when a
single import job times out — and a normal re-run starts over from zero. **Resumable
restore** makes a restore **failure-isolated and continuable**: each run makes forward
progress, and re-running picks up exactly where it left off — with **no duplicated and
no lost data**.

It is fully **opt-in**: the feature activates only when you pass `--breakpoint`. Without
that flag, `restore` behaves exactly as before. It applies to the `--use_v2_restore`
(restful) path.

## Getting the tool

This feature ships in a beta build. Download the pre-built binary for your platform from
the release page:

> 📦 https://github.com/czs007/milvus-backup/releases/tag/v0.5.17-beta.1

Pick the asset matching your OS / CPU architecture (run `uname -s` and `uname -m` to check):

| Platform | `uname -s` / `-m` | Asset |
| --- | --- | --- |
| Linux x86-64 | Linux / x86_64 | `milvus-backup_0.5.17-beta.1_Linux_x86_64.tar.gz` |
| Linux ARM64 | Linux / aarch64 | `milvus-backup_0.5.17-beta.1_Linux_arm64.tar.gz` |
| macOS Intel | Darwin / x86_64 | `milvus-backup_0.5.17-beta.1_Darwin_x86_64.tar.gz` |
| macOS Apple Silicon | Darwin / arm64 | `milvus-backup_0.5.17-beta.1_Darwin_arm64.tar.gz` |

Then verify, extract and run (example for Linux x86-64):

```bash
# download the tarball + checksums from the release page, then:
sha256sum -c milvus-backup_0.5.17-beta.1_checksums.txt   # verify integrity (optional)
tar xzf milvus-backup_0.5.17-beta.1_Linux_x86_64.tar.gz
./milvus-backup --help                                   # the new flags appear under `restore`
```

The binary is statically linked (no dependencies to install). Point it at your Milvus /
object-storage with your existing `backup.yaml`, exactly as with any other milvus-backup
release.

## Parameters

| Flag | What it does | Default | Recommended (slow / unreliable storage) |
| --- | --- | --- | --- |
| `--breakpoint <path>` | Local JSON file that records restore progress. **Supplying it enables resumable mode.** Use the *same path* on every re-run. | — (off) | A stable path on the host, e.g. `/data/restore_x.json` |
| `--resume` | Continue from the breakpoint file: skip segments already imported and reconcile any in-flight import jobs. **Omit on the first run; add it on every re-run.** | off | Add on every re-run |
| `--segments_per_batch <N>` | Maximum number of segments packed into one import job. Smaller value = smaller "blast radius" if a job fails. | 256 | Lower it, e.g. `50` |
| `--max_retry <K>` | Extra retries for a failed import job, with exponential backoff. | 0 (no retry) | `5` |
| `--retry_backoff_sec <s>` | Base backoff between import-job retries (seconds). | 5 | 5 |
| `--retry_max_backoff_sec <s>` | Cap on the backoff (seconds) — avoids hammering a throttling store. | 60 | 60 |
| `backup.parallelism.importJob` *(config file, not a CLI flag)* | Number of import jobs issued concurrently. The default is high and can overwhelm a slow object store. | 768 | Lower it, e.g. `3`–`10` |

## Using it from the CLI

### 1. First run

Run the restore as usual, adding `--breakpoint` (do **not** add `--resume` the first time):

```bash
milvus-backup restore -n <backup_name> --use_v2_restore \
  --breakpoint /data/restore_x.json \
  --segments_per_batch 50 --max_retry 5
```

### 2. If it is interrupted or reports a failure — resume

Re-run the *same command* with the *same* `--breakpoint` path, adding `--resume`. It skips
everything already imported and continues:

```bash
milvus-backup restore -n <backup_name> --use_v2_restore \
  --breakpoint /data/restore_x.json --resume \
  --segments_per_batch 50 --max_retry 5
```

**Repeat step 2 until the command exits with code 0.** Every run makes forward progress; a
restore that hits transient storage failures will complete after one or more resumes.

> **Exit code:** `0` = fully complete. Non-zero = some segments did not finish (the message
> says *"re-run with --resume to continue"*). Wrap it in a loop on non-zero with `--resume`
> if you want it fully unattended.

## Using it over the REST API

When you drive `milvus-backup` through its HTTP server (`POST /api/v1/restore`) the same
capability is available — and the breakpoint ledger is stored in **object storage** (not a local
file), so it survives a backup-server pod restart and works across replicas. The `breakpoint`
field is a stable label you reuse across runs (it keys the ledger object); `resume` continues.

First run (omit `resume`):

```bash
curl -X POST http://<backup-server>:8080/api/v1/restore \
  -H 'Content-Type: application/json' -d '{
    "backup_name": "<backup_name>",
    "useV2Restore": true,
    "breakpoint": "my-restore-1",
    "segments_per_batch": 50,
    "max_retry": 5
  }'
```

Resume (same `breakpoint` label, add `resume`):

```bash
curl -X POST http://<backup-server>:8080/api/v1/restore \
  -H 'Content-Type: application/json' -d '{
    "backup_name": "<backup_name>",
    "useV2Restore": true,
    "breakpoint": "my-restore-1",
    "resume": true,
    "segments_per_batch": 50,
    "max_retry": 5
  }'
```

> **Reuse the same `breakpoint` label on every run.** A different label points at a different
> (empty) ledger; the server refuses such a resume when the target collection already holds data,
> rather than risk duplicating it. The field is `useV2Restore` (camelCase), and the ledger is
> stored under `<backup-root>/_restore_ledger/` in the backup bucket.

### REST request fields

| JSON field | Meaning | Default |
| --- | --- | --- |
| `backup_name` | Name of the backup to restore (required). | — |
| `useV2Restore` *(camelCase)* | Use the v2 restore path. Resumable restore is on this path — set `true`. | false |
| `breakpoint` | Stable label identifying this restore; enables resumable mode. Reuse it on every run. | — (off) |
| `resume` | Skip already-imported segments and reconcile in-flight jobs. Omit on the first run; set `true` on re-runs. | false |
| `segments_per_batch` | Max segments per import job (smaller = smaller blast radius). | 256 |
| `max_retry` | Extra retries for a failed import job, with exponential backoff. | 0 |
| `retry_backoff_sec` / `retry_max_backoff_sec` | Base / cap of the retry backoff (seconds). | 5 / 60 |
| `async` | Return immediately instead of blocking until the restore finishes. | false |

## REST vs CLI

The same resumable restore is available from the CLI; the only real difference is **where the
progress ledger is stored** and how you identify the restore.

| Aspect | CLI | REST server |
| --- | --- | --- |
| Ledger storage | **Local JSON file** at the `--breakpoint` path | **Object in object storage** (`<backup-root>/_restore_ledger/`) |
| Identity | The `--breakpoint /path` (a file path) | The `breakpoint` label (a JSON string) |
| Survives host / pod restart | Only if the path is on persistent disk | **Yes** — it lives in the bucket |
| Works across replicas | No (file is local to one host) | **Yes** (any replica reads the same object) |
| Trigger resume | `--resume` flag | `"resume": true` field |
| Best fit | An operator running the CLI on a stable host | A long-running server in Kubernetes / a pod |

**Why REST uses object storage:** a server typically runs in an ephemeral, possibly replicated
pod. A local file would be lost on restart and would not be shared across replicas, so resume
could not work. Storing the ledger in the bucket removes that dependency. The CLI keeps the
simpler local-file behavior because it runs on a stable host.

## Recommended settings for slow / unreliable object storage

- **Lower the concurrency:** set `backup.parallelism.importJob` to a small number (e.g. `3`–`10`)
  so you are not issuing hundreds of simultaneous reads against a struggling store.
- **Smaller batches:** `--segments_per_batch 50` keeps the cost of redoing any one failed job low.
- **Enable retries:** `--max_retry 5` absorbs short transient blips without needing a resume.
- **Persist the ledger:** the **CLI** keeps the breakpoint as a local file, so run it on a stable
  host (or a mounted volume), not an ephemeral filesystem. The **REST server** stores the ledger
  in object storage, so no stable host is needed there — it survives a pod restart.
- **Do not drop the target collection between resume runs** — that would discard the progress
  already made.

## How it works (in brief)

The breakpoint file tracks two things: the set of segments confirmed imported, and the import
jobs that were issued but not yet confirmed. On `--resume`, the tool first asks Milvus for the
real state of each previously in-flight job (`GetImportState`): a job Milvus already finished
is claimed — so it is **not** re-imported — and anything else goes back into the to-do set. It
then re-issues only the segments still missing.

Correctness rests on a Milvus guarantee: a failed or interrupted import job commits **no visible
data** (its segments stay invisible and are garbage-collected by Milvus). So re-issuing a failed
or unknown job can never produce duplicates, and the leftovers of interrupted jobs are cleaned up
by Milvus automatically — there is nothing to clean up by hand.

## Notes

- This feature is on the `--use_v2_restore` (restful) restore path.
- The ledger is human-readable JSON. The CLI keeps it in the local `--breakpoint` file; the REST
  server keeps it as an object under `<backup-root>/_restore_ledger/`. Either is safe to inspect,
  and removing it starts a fresh (non-resumable) restore.
- Two guards protect against duplication on resume: the server **refuses** a resume whose ledger
  shows no progress while the target collection already holds data (most often a wrong breakpoint
  label), and after a restore it **fails loudly** if a collection ends up with more rows than the
  backup inserted.
