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

## How to use it

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

## Recommended settings for slow / unreliable object storage

- **Lower the concurrency:** set `backup.parallelism.importJob` to a small number (e.g. `3`–`10`)
  so you are not issuing hundreds of simultaneous reads against a struggling store.
- **Smaller batches:** `--segments_per_batch 50` keeps the cost of redoing any one failed job low.
- **Enable retries:** `--max_retry 5` absorbs short transient blips without needing a resume.
- **Run on a stable host:** the breakpoint file is on local disk and must survive between runs.
  Use a persistent path (or a mounted volume), not an ephemeral container filesystem.
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
- The breakpoint file is human-readable JSON; it is safe to inspect, and you may delete it to
  start a fresh (non-resumable) restore.
