# Agent Development Rules

Rules for AI coding agents (Claude Code, Codex, Cursor, ...) working in this repository.
Human contributors should read [CONTRIBUTING.md](CONTRIBUTING.md) — these rules are additions
that agents tend to get wrong, not a replacement for it.

## Testing

Use [`testify`](https://github.com/stretchr/testify) assertions. Do not hand-write `if` checks
followed by `t.Errorf` / `t.Fatalf`.

```go
// Good
assert.Equal(t, "ab****ef", maskSecret("abcdef"))
assert.NoError(t, err)
assert.Len(t, backups, 1)
assert.Contains(t, out, "milvus.port")

// Bad — reimplements what assert already does, with worse failure messages
if got := maskSecret("abcdef"); got != "ab****ef" {
    t.Errorf("maskSecret() = %s, want %s", got, "ab****ef")
}
if err != nil {
    t.Fatalf("Load: %v", err)
}
```

Why: testify prints a diff of expected vs actual on failure, so a broken test tells you what
went wrong without reading the test source. Hand-written checks each invent their own message
format, and the assertion logic itself becomes something to review.

**`assert` vs `require`** — both are in use here, and the difference matters:

- `assert` reports the failure and lets the test continue. Use it for the checks you are
  actually making, so one run reports every mismatch rather than only the first.
- `require` reports and aborts the test immediately. Use it for preconditions where continuing
  would panic or produce meaningless noise — setup that must succeed, or a value you are about
  to dereference.

```go
require.NoError(t, Write(ctx, cli, backupDir, info)) // setup must succeed to test anything
got, err := Read(ctx, cli, backupDir)
require.NoError(t, err)                              // nil got would panic below
assert.Len(t, got.GetCollectionBackups(), 1)         // the actual check
```

Use PascalCase for sub-test names in `t.Run()`, e.g. `t.Run("SecretValueIsMasked", ...)`.
See `internal/cfg/value_test.go` for a table-driven test in the intended style.

## Commit titles

Use etcd-style subjects — `scope: short description in lowercase`:

```
backup: scope index extra etcd scan to backed-up collections
restore: add support for partial collection restore
storage: replace per-file copy verification with batched prefix verify
ci: skip workflow runs for non-code path changes
```

- **scope** is the component or area touched: `backup`, `restore`, `storage`, `milvus`,
  `stream`, `migrate`, `cfg`, `client`, `ci`, `docs`, `test`, ...
- description starts lowercase, imperative mood ("add", not "added"), no trailing period
- keep the subject under 72 characters

Do not use Conventional Commits (`feat:`, `fix(storage):`). A few exist in history, but etcd
style is the convention here by a wide margin. `build(deps):` commits are dependabot's and are
exempt — they are generated, not written.

**PRs are squash-merged, so the PR title is what lands in git history — not your local commit
subjects.** A 21-commit PR merges as one commit whose subject is the PR title with ` (#NNNN)`
appended by GitHub. Apply this rule to the PR title above all, and leave room for the suffix.
