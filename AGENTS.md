# cord - Agent Instructions

## Project overview

cord is a Go library. Keep its public API small, idiomatic, documented, and backward compatible.

## Required validation

After code changes, run:

```bash
vfox exec golang -- go test ./...
vfox exec golang bun -- go tool task build
vfox exec golang bun -- go tool task ci
```

Use `vfox exec` for pinned Go and Bun runtimes. Task, Lefthook, golangci-lint,
and deadcode are pinned as Go tool dependencies and run with `go tool`.

## Common commands

```bash
vfox exec golang bun -- go tool task build          # build all packages
vfox exec golang bun -- go tool task test           # tests with race detection and shuffled order
vfox exec golang bun -- go tool task test-coverage  # coverage profile
vfox exec golang bun -- go tool task lint           # golangci-lint
vfox exec golang bun -- go tool task fmt            # auto-format and auto-fix lint issues
vfox exec golang bun -- go tool task fmt-check      # check formatting without modifying files
vfox exec golang bun -- go tool task ci             # non-mutating full CI pipeline
```

## Project structure

Keep packages at the repository root when the library remains small. Add focused subpackages only when they represent clear, independently useful concepts. Use `internal/` for implementation that consumers must not import.

## Engineering principles

- Preserve documented public behavior and extension contracts unless a breaking change is explicitly authorized.
- Choose the simplest implementation that fully satisfies current requirements.
- Keep components modular and concerns clearly separated.
- Prefer the standard library and existing dependencies when they meet the requirements.
- Avoid exposing implementation details through public APIs.
- Co-locate tests with the package they test and add executable examples for important public APIs.

## Code style

- Follow idiomatic Go naming and formatting.
- Add godoc comments for every exported identifier.
- Never ignore errors.
- Prefer table-driven tests for behavior with multiple cases.
- Avoid `//nolint`; fix the underlying issue instead.
