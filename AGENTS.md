# cord - Agent Instructions

## Project overview

cord is a Go library. Keep its public API small, idiomatic, documented, and backward compatible.

## Required validation

After code changes, run:

```bash
mise exec -- go test ./...
mise exec -- task build
mise exec -- task ci
```

Use `mise exec --` for Task and Go tooling in this repository.

## Common commands

```bash
mise exec -- task build          # build all packages
mise exec -- task test           # tests with race detection and shuffled order
mise exec -- task test-coverage  # coverage profile
mise exec -- task lint           # golangci-lint
mise exec -- task fmt            # auto-format and auto-fix lint issues
mise exec -- task fmt-check      # check formatting without modifying files
mise exec -- task ci             # non-mutating full CI pipeline
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
