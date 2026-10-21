# Two thin restore-import task types over a shared unexported importer, with per-job 2PC commit

Status: accepted

Context: restore needs Milvus's two-phase import (`auto_commit=false` → Uncommitted →
CommitImport) so a restore into a replicating cluster can verify each secondary's mirrored
job before committing. `importViaRESTFulTask` is already extracted as a self-contained
tasklet; the question was how 2PC capability should be added to it.

Decisions:

- **Per-job commit, no restore-level barrier for now.** Milvus's `CommitImport` is per-job
  (datacoord services.go): the only criterion is that the job itself reached Uncommitted.
  A restore-level barrier that stages every job before committing any would only shrink
  the mixed-visibility window — and that window already exists today with
  `auto_commit=true`, where each job becomes visible as it completes. Per-job 2PC commit
  therefore regresses nothing; it buys secondary verification, a clean abort path, and the
  option of a manual hold. A barrier remains a possible future enhancement, not a
  prerequisite of 2PC.
- **Two complete task types, not one task with branches.** `importViaRESTFulTask` keeps
  its exact current behavior (submit → wait Completed) for targets without 2PC. The new
  `importViaRESTFul2PCTask` owns the whole 2PC lifecycle in its own `Execute`: stage →
  `submit2PC` → `waitUncommitted` → check each secondary's mirrored
  job (same jobID) → CommitImport on the local cluster, then the secondaries. Failure after
  staging aborts both ends.
- **Mechanism sinks into an unexported collaborator.** Staging, request submission, the
  wait loop, and temp-file cleanup move onto an unexported `restfulImporter` value that
  each task holds and drives explicitly. Neither task shares step *methods*; they share a
  device. The importer speaks Milvus's vocabulary so the tasks don't have to: instead of
  parameterized `submit(opts)` / `wait(terminal)`, it exposes intent-named methods —
  `submit` / `submit2PC` (which builds the `auto_commit=false` option internally) and
  `waitCompleted` / `waitUncommitted` — keeping protocol details like option keys and
  terminal states inside the mechanism layer, out of the task shells.
  The secondary check is not an injected closure: the 2PC task holds the secondaries'
  `milvus.Restful` clients (`secondaries []importSecondary`) as a plain field — being
  two-phase is the type's declared job, so the dependency is data, not hidden behavior.
  Client construction stays in the orchestration layer, which is the only place that knows
  which address is local and which are secondaries.

Considered options, rejected:

- **An `if` inside `importViaRESTFulTask`**: fuses two shapes into one type; every future
  change has to reason about both.
- **A new task wrapping `*importViaRESTFulTask` and calling its `Execute`**: the inner
  Execute is an indivisible stage→submit→wait-Completed pipeline; reusing it reuses the
  wrong unit. Composition must be over mechanism, not over a whole behavior.
- **Shared step functions called by both tasks**: leaks scope — steps become
  package-callable in any order, and inter-step state travels through mutated struct
  fields the compiler can't sequence.
- **Embedded base struct with hook overrides**: inheritance-by-embedding, already
  rejected once in this codebase; the template's self-references hide the real call graph.
- **Injected `secondaryCheck`/`commitBoth` closures**: wraps a simple dependency (a second
  client) in indirection; a typed field is more honest and easier to mock.
- **Restore-level commit barrier**: deferred; see above.

Consequences:

- The general rule for "two shapes of one operation" in this repo: sink the mechanism
  into an unexported collaborator, lift the flow into each type's own `Execute`, express
  differences as data (fields), not overrides.
- The known residual risk is the divergence window between the local commit and the
  secondary commits (local committed, a secondary failed). CommitImport is idempotent, so
  retry is safe; errors from a secondary's commit must surface the fact that the local end
  already committed.
- Retrying a whole 2PC task after a crash must reuse the recorded jobID from the jobstate
  store rather than submitting a duplicate import; this requirement exists regardless of
  the structural choice above.
