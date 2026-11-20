# Resume a failed restore

Each selected collection mapping in a restore request must have a unique target database and collection name. The server checks the effective targets after filtering and renaming, before registering the job. One source can still map to several distinct targets.

Set `"enableResume": true` in the original `POST /api/v1/restore` request to enable Resume for that task. It defaults to false. By default each binlog import attempts to clean its staged files on exit, including after a partial copy or a failed import. With Resume enabled, a failed import keeps its files for the next attempt. For ordinary restores, automatic cleanup in both modes honors `restore.keepTempFiles`.

The running backup server supports Resume for binlog restores (gRPC, REST or two-phase REST imports), snapshot imports (including two-phase imports with secondaries), snapshot restores, and metadata-only restores. A resumed metadata-only restore continues the original collection DDL and skips collections whose DDL already completed. Independent secondary restores also support Resume with the same opt-in on `POST /api/v1/restore_secondary`. See [Secondary restore](secondary_restore.md#resuming-a-failed-secondary-restore) for the replication requirements.

`GET /api/v1/get_restore?id=<id>` reports `resumable: true` throughout the lifetime of a task created with Resume enabled. The field is omitted when false. Only a failed task with a prepared plan can start another attempt. Failure during shared preparation requires a new restore request.

A synchronous restore returns the outcome of its own execution. If the job fails after registration, the response includes its task ID in `data.id`. A later Resume updates the state reported by `get_restore`.

Send the original task ID to resume asynchronously. Persisted state loading, state checks and the required stain write finish before the request is accepted; failures in those steps return directly without starting execution. Acceptance does not wait for background restore completion. Poll the existing task ID for its current result; execution failures appear in the task error message. Only a failed, resumable job accepts the request. The plan, options, import batches and partition/L0 ordering are fixed. The in-memory store holds the resolved configuration, per-collection import batches and current progress. Resume creates new clients and tasks from those records. Failed attempts save them for cross-process recovery as described below.

```sh
curl -X POST http://localhost:8080/api/v1/resume_restore \
  -H 'Content-Type: application/json' -d '{"id":"<restore-id>"}'
```

Each collection resumes its DDL before importing. Once DDL completes, Resume skips directly to the import jobs. If schema creation stopped after the target ID was recorded, Resume verifies the ID and adds only missing fields in backup order. Conflicting field definitions stop the restore. When creating a collection, the restore records a checkpoint as soon as it finds the target absent or successfully drops it. An interruption after this checkpoint can leave the target ID unknown. Resume creates the target if it is absent. If the name exists, Resume logs a warning, records its ID, and continues schema creation. This assumes exclusive use of the target name: a collection another writer created in that interval can be mistaken for this restore’s target. An interrupted index phase can use the original drop-index option; partition creation checks which partitions exist. Keep the backup and target collection under the same restore's control. Recreating a collection under the same name can direct pending imports to the replacement.

Without Resume, job submission uses the original execution context and follows its cancellation and deadline. With Resume enabled, when a sibling fails, an in-flight job submission has up to two minutes from the start of its request to return a Milvus job ID. The restore records any returned ID before it reports failure.

Resume skips each import already marked complete locally. For an outstanding import with a saved Milvus ID, ordinary restores use the initial lookup result to decide whether to adopt or replace it. Two-phase imports follow the separate policy below.

- If the job is running, Resume keeps polling it. A failure encountered during this wait ends the current execution and retains the ID; only a later explicit Resume can replace it. If it completed, Resume records completion and moves to the next batch.
- If the initial lookup reports a terminal failure, Resume submits the original batch again. This includes a job that Milvus marks failed because it timed out.
- If Milvus reports that the saved ID no longer exists, Resume fails and retains the ID. Later attempts check the same ID. If the record was garbage-collected permanently, verify the target data before deciding how to recover manually. Milvus can report this in the lookup error or in the job's failure reason if garbage collection races with the lookup.
- If the lookup itself times out, access is denied, or the response is ambiguous, the attempt fails and retains the ID for the next Resume.

When a binlog batch needs staging, the restore records one temporary directory for that batch before copying. Before resubmitting the batch, it copies all its binlogs into the same directory again and verifies the copy. Polling an existing import job requires no copy.

Each Resume can submit at most one replacement per batch. Once Milvus returns its new ID, the restore tracks that ID. If the replacement fails, another explicit Resume is required.

Resume while Milvus still retains the import jobs whose final status the backup server has yet to record. For example, a sibling failure can interrupt polling after the server has saved the job ID. Milvus can then finish the import while the restore is idle. In Milvus 2.6, `dataCoord.import.taskRetention` defaults to 10,800 seconds (3 hours), counted separately from the time each import job enters Completed or Failed. Size this retention window to cover the longest expected delay between a job finishing and an operator resuming the restore. If necessary, increase it in Milvus in advance, including on each secondary whose import status must be confirmed. Increasing retention after GC cannot recover a deleted job record. [Milvus configuration reference](https://milvus.io/docs/configure_datacoord.md#dataCoordimporttaskRetention)

**Import replay has at-least-once semantics.** A submission whose reply was lost before its ID was recorded can still be submitted again on Resume. If Milvus accepted both submissions, data can be duplicated. Replacing a confirmed failed job can also leave data or effects from the old job on the target.

For the separate snapshot restore API (`restore_snap`), Resume deletes a leftover target collection before resubmission only when the old job explicitly reports `Failed` and the original restore enabled `drop_exist_collection` (`DropExistCollection`). The option still controls deletion of a pre-existing collection during initial preparation. Without it, even a collection left by this restore is preserved; resubmission can fail because the collection already exists, requiring manual cleanup before another Resume. If checking or deleting the leftover collection fails, the old job ID is retained so the next Resume can check it again. Running jobs continue to be polled. Query errors, a missing job, or a lost submission response do not authorize deletion, even with the option enabled. A failure discovered during polling ends that attempt without cleanup or resubmission; cleanup is considered only on a later explicit Resume. Once the remote snapshot completes, a local properties or description error retries only the postprocessing and never deletes the collection.

Independent secondary restores use their own replay policy: a saved terminal failure or a real Failed result on the initial Resume lookup permits one replacement, including after commit. A failure encountered later while waiting ends the execution until the next Resume. Missing job records do not authorize replacement. These replays are at-least-once and may retain duplicate data or effects from the failed job. See [secondary restore recovery](secondary_restore.md#resuming-a-failed-secondary-restore) for lookup retries and the full policy.

For two-phase imports, Resume checks the primary and the saved secondary endpoints using the same job ID. Before the first commit request, replacement requires an initial primary lookup confirming terminal failure and a successful abort. A missing primary ID fails the attempt and retains the ID. A mirror that stays missing throughout the replication wait also fails the attempt and leaves the primary available for another lookup. A failure encountered later while waiting ends this execution without replacement; the existing abort rules still apply. Once commit has been requested, a missing or failed primary ID is retained to avoid replaying potentially committed data. A failed mirror after the primary commits leaves the restore failed and requires resolving the divergence on the clusters. See [two-phase commit restore](two_phase_commit_restore.md).

To abandon the job, clean its staging directories. Cleanup immediately disables further Resume. For both ordinary and independent secondary restores, explicit Cleanup ignores `restore.keepTempFiles` and attempts to delete all staging directories recorded for the task, including those retained by completed batches. Cleanup does not wait for or cancel Milvus imports. A running import may lose its inputs or may still finish after cleanup. A storage error leaves the job in FailedCleanupPending; repeat the same request to finish. FailedCleaned reports completed local cleanup. Completed imports normally remove their own temporary files; a deletion failure is logged and does not undo a successful import.

```sh
curl -X POST http://localhost:8080/api/v1/cleanup_failed_restore \
  -H 'Content-Type: application/json' -d '{"id":"<restore-id>"}'
```

## Durable recovery state

Persisted state (`PersistedState`) is the saved task progress used for Resume and Cleanup; it is distinct from a Milvus snapshot backup. The in-memory store is authoritative. Get, Resume and Cleanup use memory first and load a persisted state record only when that ID is absent. When persistence is enabled, failed attempts with a complete plan save one versioned persisted state record after their workers and replication streams stop. This includes DDL checkpoints, import IDs and batches, staging directories, snapshot postprocessing, two-phase commit observations, and independent secondary plans and replication checkpoints. Pending secondary messages retain their original IDs, headers, payloads and per-channel order.

Terminal saves are best effort: at most three attempts within a 15-second context deadline. A save failure does not change the restore's execution result. Best-effort persistence failures are logged and are not added to task responses or CLI result messages. The latest in-memory failed task remains usable. Without a successfully saved persisted state, recovery after process exit is not guaranteed.

When persistence is enabled, before Resume makes any remote changes it must successfully replace the complete persisted state with a `stain` marker. This applies even when the task is already in memory. A failed write prevents execution and allows another same-process attempt. A normally failed attempt replaces the marker with a fresh `available` persisted state. Successful execution deletes the persisted state, best effort; successful task history remains only in memory.

The marker applies only to persisted progress. If the task is already in memory, Resume uses that current progress and the task state, even when the stored persisted state is marked `stain`. If memory has no task, Get and Resume reject a `stain` persisted state as unusable and do not register it in memory. Cleanup can still read its resource records; it registers them as FailedCleanupPending before proceeding, so concurrent Resume cannot adopt them. Confirm that the previous process has exited before loading a task in another process. There is no distributed lease or protection against two processes executing the same task. Running-process crashes are outside the recovery guarantee; a first attempt may have no persisted state at all.

Cleanup first changes memory to FailedCleanupPending, then must save that state with `stain` before deleting any staged files. If that write fails, Resume remains disabled in this process and Cleanup can be retried. Partial cleanup retains the remaining directories and saves them best effort. Missing files are harmless on retry. Once cleanup completes, it deletes the persisted state. If persisted state deletion fails, repeat Cleanup in the same process to retry record deletion only, including for successful tasks. A crash may leave unrecorded staging directories that this mechanism cannot discover.

### Storage configuration

Server and CLI use the same `--config` and `--set` loader. The Store receives persistence configuration at startup and owns one shared, task-independent key/value backend. Restore-specific persisted state encoding, keys and recovery rules remain in the Store’s restore logic. Restore tasks pass only their ID and Resume setting; they do not keep separate persistence configurations or clients. Cold loads and lifecycle operations use the shared backend, while saving, deletion and the pre-Resume marker remain serialized under each task lock.

When persistence is enabled, Server and CLI startup check the configured backend by writing a unique probe, reading it back, comparing the bytes, and deleting it. The probe uses the same directory or S3 prefix as task records without touching existing persisted state records. The check has a 15-second context deadline; cleanup has a separate 5-second deadline so it can still run after cancellation. A cleanup failure can leave a probe object behind and is included in the warning. Failures are logged and do not block startup or disable later persistence attempts. Starting the server through the CLI checks the same configuration only once. A successful check does not guarantee later storage availability; the pre-Resume stain write remains mandatory.

`restore.persistence.enabled` defaults to true. Set it to false for memory-only Resume: the Store skips all persisted state reads, writes and deletions, and no cross-process recovery is available. Persistence is independent of the per-task `enableResume` / `--enable_resume` setting, which still defaults to false. Existing persisted state records are left untouched when persistence is disabled.

The default backend is local, with files under `restore-state` relative to the process working directory. To use a persistent local volume:

```yaml
configVersion: v2
restore:
  persistence:
    enabled: true
    type: local
    local:
      directory: /var/lib/milvus-backup/restore
```

For S3:

```yaml
configVersion: v2
restore:
  persistence:
    enabled: true
    type: s3
    s3:
      endpoint: s3.us-east-1.amazonaws.com  # host[:port], no scheme
      region: us-east-1
      useSSL: true
      bucket: backup-state
      prefix: restore/
      accessKeyID: ""
      secretAccessKey: ""
      sessionToken: ""
```

The bucket must already exist. These are separate persistence settings; they do not inherit `backup.storage`. The default endpoint is `localhost:9000`, bucket `backup-state`, prefix `restore/`, with TLS enabled. Set the endpoint and credentials for your deployment. Empty access keys use the SDK IAM credential provider. Credentials can also come from `RESTORE_PERSISTENCE_S3_ACCESS_KEY_ID`, `RESTORE_PERSISTENCE_S3_SECRET_ACCESS_KEY` and `RESTORE_PERSISTENCE_S3_SESSION_TOKEN`.

Persisted state records contain the original execution configuration, including credentials and external source specifications. Restrict access to the directory or bucket. Local files use mode 0600 and atomic replacement; S3 writes one complete object per task. Object/file names are SHA-256 hashes of task IDs with a `.json` suffix. New tasks register only in memory and reject IDs already present there. Creation does not read or modify persisted state records and is not blocked by an inaccessible store. When supplying task IDs yourself, do not reuse an ID with a persisted state record you still need: a later save or deletion for the new task can replace or remove that persisted state.

The persistence backend and location come exclusively from startup configuration, shared by ordinary and independent secondary restores. Restore, Resume and Cleanup requests do not accept a persistence override; Get, Resume and Cleanup identify the task by its ID.

Use the same persistence location after restarting the server or starting a new CLI process. Cold recovery uses startup configuration to access the persisted state and the saved original configuration to execute the restore. Existing in-memory tasks retain their backend and are never replaced by persisted state. Unsupported versions and malformed records are errors, not missing tasks.

### Optional encryption
Local and S3 store plaintext by default. To enable application-level encryption, add the following settings under the existing `restore.persistence` section in the YAML used by both the server and CLI:

```yaml
encryption:
  enabled: true
  provider: config
  config:
    keyID: restore-key-1
    key: "<Base64 of 32 random bytes>"
```

To provide the key through the environment, keep the switch and provider in YAML:

```yaml
encryption:
  enabled: true
  provider: env
```

Supply both variables to every server or CLI process that uses this persistence location:

- `RESTORE_PERSISTENCE_ENCRYPTION_KEY_ID` holds the stable master key ID.
- `RESTORE_PERSISTENCE_ENCRYPTION_KEY` holds the Base64 encoding of the 32-byte master key.

The env provider reads both values exclusively from the environment. An unset or empty variable produces a startup check warning naming the missing variable. With valid settings the existing startup probe writes, decrypts, verifies and deletes a test record. Setting the variables alone does not enable encryption.

Generate the key once with `openssl rand -base64 32`. Protect the configuration file and keep a separate, secure backup of the key. Every write, including a stain, uses a fresh data key and AES-256-GCM. Both providers wrap the data key with the master key using AES-256-GCM. The record contains the key ID, wrapped data key and authenticated ciphertext. The payload is bound to its storage key. The master key is excluded from the task plan even when encryption is disabled.

Use the same encryption mode, key ID and master key to resume or clean up across processes. Each provider supports one master key at a time. Finish or clean up outstanding tasks before changing modes or rotating keys, or keep their original configuration and use a separate directory or prefix for new tasks. Existing records are not migrated. The startup probe tests the configured mode on a new record. A successful probe cannot verify that old records are readable. Encryption failures follow the usual persistence error handling and never fall back to writing plaintext.

### Inspecting persisted task progress

For encrypted records, use Get with the matching startup configuration to inspect progress. The direct JSON inspection below applies to plaintext records.

Ordinary and independent secondary restores use the same naming rule: the full SHA-256 hash of the task ID, encoded as 64 lowercase hexadecimal characters, followed by `.json`. Hashing the filename does not encrypt the record: it is JSON, and `Status.ID` contains the original task ID. Some embedded protobuf data is stored as base64-encoded bytes.

To locate a local record and inspect its status with `jq`:

```sh
restore_id='your-restore-task-id'
state_dir='/data/restore-state' # restore.persistence.local.directory
state_hash=$(printf '%s' "$restore_id" | shasum -a 256 | awk '{print $1}')
state_file="$state_dir/$state_hash.json"

printf '%s\n' "$state_file"
jq '{Version, Marker, Status}' "$state_file"
```

Use the exact task ID, including its original case. `printf '%s'` adds no newline; hashing a trailing newline produces a different filename. On Linux, `sha256sum` can replace `shasum -a 256`. If the configured directory is relative, resolve it from the backup process's working directory. For S3, use the same filename under the configured `restore.persistence.s3.prefix` in `restore.persistence.s3.bucket`; download that object to inspect it with `jq`.

`Status` contains the task ID, error, timestamps and collection/job status. In the current format, `Status.State` is `0` Initial, `1` Executing, `2` Success, `3` Fail, `4` FailedCleanupPending or `5` FailedCleaned. These values apply to the task state; nested DDL and job states have separate enums. To inspect the saved import IDs, batches and staging directories, use:

```sh
jq '.Collections' "$state_file"
```

`Marker` is `available` or `stain`, as described above. The record is saved recovery progress, not live progress; use Get for the latest in-memory state. A missing file does not by itself mean failure: persistence may be disabled, the task may not have saved a record yet, saving may have failed, or successful execution/Cleanup may have deleted it. Inspect records without editing their markers or contents; use Resume and Cleanup for task operations. The status-only command above omits the saved configuration, which can contain credentials; avoid sharing the full record without redaction.

### CLI

```sh
milvus-backup restore --config backup.yaml -n my_backup --enable_resume
milvus-backup restore secondary --config backup.yaml -n my_backup \
  --source_cluster_id source --target_cluster_id target --enable_resume

milvus-backup restore resume --config backup.yaml --id <restore-id>
milvus-backup restore cleanup --config backup.yaml --id <restore-id>
```

Creation prints the task ID before execution. Each command waits for its execution and bounded terminal persistence attempts before exiting. The Resume CLI polls task status locally after admission and exits with an error if execution fails; the HTTP Resume endpoint returns after admission. Resume and Cleanup work for both ordinary and independent secondary restores. Execution/admission errors produce a nonzero exit; best-effort persistence failures are only logged and do not change the execution outcome. A failure to write the pre-Resume marker still returns an error and prevents execution.

Use the same persistence location across CLI processes. For example:

```sh
milvus-backup restore resume --config backup.yaml --id <restore-id> \
  --set restore.persistence.type=local \
  --set restore.persistence.local.directory=/data/restore-state
```

Persistence does not change import replay semantics. A missing remote job may already have completed, so Resume reports the lookup failure and retains the ID. Resume within Milvus's job retention period, keep the backup immutable, and keep CDC stopped for independent secondary recovery.
