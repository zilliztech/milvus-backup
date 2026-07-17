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
