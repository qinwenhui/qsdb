# Contributing

Thanks for considering a contribution to QSDB.

## Development environment

- Rust **1.85+** (the workspace uses edition 2024)
- Any of Linux / Windows / macOS; CI covers Linux and Windows

## Before you open a PR

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

All three must pass. The CI runs the same commands on both OSes, plus a
release-binary size guard.

## Ground rules

1. **Regression tests are not optional.** Bug fixes land with a test that
   fails without the fix. New behavior lands with a test pinning it —
   including the error cases.
2. **Error codes have a single source of truth.** If your change adds or
   alters an error code, update `docs/error-codes.md`; the drift between the
   runtime catalog and that document is enforced by a test.
3. **Explicit beats silent.** An unsupported feature must fail loudly with a
   code pointing at the fix, never fall back to a wrong-but-plausible answer.
   Half-implemented semantics are treated as bugs, not features.
4. **Minimal dependencies.** New dependencies need a justification in the PR
   description — what it buys, what it costs in build time and supply chain.
5. **Docs travel with behavior.** If you change a documented behavior, update
   the matching document in `docs/` in the same PR.

## Commit messages

Use conventional prefixes (`feat:`, `fix:`, `docs:`, `perf:`, `test:`,
`refactor:`) with a short imperative summary. Write the body in whichever
language you are comfortable with.

