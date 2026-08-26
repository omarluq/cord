# Cord

[![CI](https://github.com/omarluq/cord/actions/workflows/ci.yml/badge.svg)](https://github.com/omarluq/cord/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/omarluq/cord/branch/main/graph/badge.svg)](https://codecov.io/gh/omarluq/cord)
![Go Version](https://img.shields.io/badge/Go-%3E%3D%201.27-%23007d9c)
[![Go Reference](https://pkg.go.dev/badge/github.com/omarluq/cord.svg)](https://pkg.go.dev/github.com/omarluq/cord)
[![Version](https://img.shields.io/github/v/release/omarluq/cord?label=version&logo=semver)](https://github.com/omarluq/cord/releases)
[![License](https://img.shields.io/github/license/omarluq/cord)](./LICENSE.txt)

Cord is a durable workflow library for Go. Compose ordinary typed functions into
linear or branching workflows; Cord persists each run in SQLite or PostgreSQL
and resumes available work after a process restart.

<img src="assets/banner.png" alt="Cord banner" height="400">

## Install

```bash
go get github.com/omarluq/cord
go get modernc.org/sqlite # or github.com/jackc/pgx/v5
```

Cord works with a caller-owned `*sql.DB` backed by SQLite (including remote
libSQL) or PostgreSQL. The examples use the pure-Go
[`modernc.org/sqlite`](https://pkg.go.dev/modernc.org/sqlite) driver. PostgreSQL
applications use pgx's [`database/sql`](https://pkg.go.dev/github.com/jackc/pgx/v5/stdlib)
driver; Cord does not expose a driver, dialect option, or PostgreSQL-specific API.

## Quick start

Workflow steps are package-level functions. Each step receives a context and a
typed input, and returns a typed output or an error.

```go
package main

import (
    "context"
    "database/sql"
    "errors"
    "fmt"
    "log"

    "github.com/omarluq/cord"
    _ "modernc.org/sqlite"
)

func double(_ context.Context, value int) (int, error) {
    return value * 2, nil
}

func format(_ context.Context, value int) (string, error) {
    return fmt.Sprintf("result: %d", value), nil
}

func run(ctx context.Context) (_ string, err error) {
    db, err := sql.Open(
        "sqlite",
        "file:workflow.db?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)",
    )
    if err != nil {
        return "", err
    }

    runtime, err := cord.New(ctx, db)
    if err != nil {
        return "", errors.Join(err, db.Close())
    }
    defer func() {
        err = errors.Join(err, runtime.Close(), db.Close())
    }()

    flow := runtime.From("double-and-format", double).Then(format)
    return flow.Run(ctx, 21)
}

func main() {
    result, err := run(context.Background())
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println(result) // result: 42
}
```

The name passed to `From` is the durable workflow identity. Keep it stable when
renaming or refactoring the root function.

## Submit and retrieve runs asynchronously

`Submit` persists the complete run plan and returns its generated UUIDv7
`cord.RunID` without waiting for execution. `Get` blocks until that run reaches a
durable terminal state and decodes the typed result:

```go
runID, err := flow.Submit(ctx, input)
if err != nil {
    return err
}

// Persist runID in application state or pass it to another process.
result, err := flow.Get(waitCtx, runID)
if err != nil {
    return err
}
_ = result
```

`RunID` has `string` as its underlying type, so applications can store, transfer,
and convert known IDs to it. Such conversion does not create a durable run; only
Cord generates IDs for submitted runs. To make submission retries converge on
one retained run, pass one caller-chosen idempotency key:

```go
runID, err := flow.Submit(ctx, input, "order:1234")
```

The variadic argument is optional but accepts at most one non-empty key. Reusing
a key with the same workflow definition and encoded input returns the original
run ID; reusing it for different work returns an error matching
`cord.ErrRunConflict`. An unkeyed retry creates a new run. The caller must retain
the key until submission is unambiguously resolved. Key reservations last only
as long as the associated run row—and therefore its idempotency data—is retained.

`Get` is typed and definition-compatibility checked. Reconstruct the workflow's
persisted name, input type, reachable topology, function identities and
signatures, and terminal node before retrieving a run, including in a different
process. Cord recomputes that definition with the run's persisted retry policy;
a mismatch returns an error matching `cord.ErrRunIncompatible`.
Canceling `waitCtx` stops only that `Get`; it does not cancel the workflow.
Use `Cancel` for explicit durable cancellation:

```go
if err := flow.Cancel(ctx, runID); err != nil &&
    !errors.Is(err, cord.ErrRunFinished) {
    return err
}
```

`Cancel` is keyed only by run ID and does not perform `Get`'s workflow
compatibility check. It is idempotent for an already canceled run. Missing IDs
return an error matching `cord.ErrRunNotFound`, and cancellation that loses to
successful or failed completion returns one matching `cord.ErrRunFinished`.
If the cancellation response is ambiguous, Cord rereads durable state before
returning a definitive result; unresolved errors remain safe to retry with the
same run ID. Cancellation durably fences future Cord writes. An active attempt
in the runtime that calls `Cancel` is signaled promptly; attempts in other
runtimes observe the lost lease through heartbeat and are then canceled
cooperatively. `Cancel` cannot forcibly stop arbitrary Go code, guarantee when
an external attempt observes cancellation, or undo external side effects
already in progress.

These sentinel outcomes describe durable state, not authorization: for example,
`ErrRunNotFound` can reveal whether an ID exists. A `RunID` is not an
authorization credential. Applications must authenticate callers and enforce
tenant ownership before passing an untrusted run ID to `Get` or `Cancel`.

## Inspect a run

Use `InspectRun` to read a run's current durable state without waiting for it to
finish. `ListRunNodes` returns its nodes in stable `NodeID` order:

```go
report, err := runtime.InspectRun(ctx, runID)
if err != nil {
    return err
}
fmt.Printf("%s: %s (%s)\n", report.ID, report.State, report.Reason)

page, err := runtime.ListRunNodes(ctx, runID, cord.NodeQuery{PageSize: 50})
if err != nil {
    return err
}
for _, node := range page.Nodes {
    fmt.Printf("%s: %s attempt %d/%d\n",
        node.NodeID, node.State, node.Attempt, node.MaxAttempts)
}
```

Run states are `running`, `canceling`, `completed`, `failed`, and `canceled`.
Node states are `pending`, `ready`, `running`, `retry_wait`, `completed`,
`failed`, and `canceled`. Terminal reports also include a reason such as
`succeeded`, `canceled_by_request`, `failure_non_retryable`, or
`failure_attempts_exhausted`.

Reports include lifecycle timestamps and runner information for diagnostics.
`runtime.RunnerID()` identifies the current runtime instance; it is not a
hostname, credential, or lease token.

Use `NodeQuery` to filter by state or terminal reason and to paginate large
runs. The default page size is 50 and the maximum is 200. Pass each page's
continuation token to the next query. Pages are read separately, so a running
workflow may change between requests.

Snapshots omit inputs, outputs, idempotency keys, and user error messages. Use
`Workflow.Get` for the typed result or terminal error. Run IDs and continuation
tokens do not grant access: authenticate callers and check ownership before
exposing inspection APIs.

When upgrading Cord, stop old processes before starting processes that may
apply a database migration. Running mixed Cord versions against the same
database is not supported.

## PostgreSQL with pgx

Register pgx's `database/sql` driver in the application, configure and health-check
the caller-owned pool, then pass it to the same constructor:

```go
import _ "github.com/jackc/pgx/v5/stdlib"

db, err := sql.Open("pgx", os.Getenv("DATABASE_URL"))
if err != nil {
    return err
}
db.SetMaxOpenConns(10)
db.SetMaxIdleConns(10)
if err := db.PingContext(ctx); err != nil {
    return errors.Join(err, db.Close())
}

runtime, err := cord.New(ctx, db)
```

Cord detects supported SQL capabilities internally, including through wrapped
drivers. An unsupported database makes `cord.New` return an error matching
`cord.ErrMigrationFailed`; there is no public backend selector. Size the pool for
application traffic, concurrent Cord workers, and startup migration work, and
monitor `sql.DB.Stats` for waits. `Close` and `Shutdown` never close the pool; the
application must close it. See the pgx commands for the
[`linear`](./examples/linear/pg) and [`join`](./examples/join/pg) examples.

Supported driver families are modernc, mattn, and ncruces SQLite, remote
Turso/libSQL, and pgx v5 stdlib for PostgreSQL 14–18.

## Turso and remote libSQL

Use Turso's maintained Go libSQL driver when the database is hosted remotely:

```bash
go get github.com/tursodatabase/go-libsql
```

```go
import _ "github.com/tursodatabase/go-libsql"

func openTurso(ctx context.Context) (*sql.DB, error) {
    databaseURL, err := url.Parse(os.Getenv("TURSO_DATABASE_URL"))
    if err != nil {
        return nil, fmt.Errorf("parse Turso database URL: %w", err)
    }

    query := databaseURL.Query()
    query.Set("authToken", os.Getenv("TURSO_AUTH_TOKEN"))
    databaseURL.RawQuery = query.Encode()

    db, err := sql.Open("libsql", databaseURL.String())
    if err != nil {
        return nil, fmt.Errorf("open Turso database: %w", err)
    }
    // Remote migration locking reserves one connection for lease renewal while
    // Goose holds another for migration work.
    db.SetMaxOpenConns(2)

    if _, err := db.ExecContext(ctx, "PRAGMA foreign_keys = ON"); err != nil {
        return nil, errors.Join(err, db.Close())
    }

    return db, nil
}
```

Pass the returned database to `cord.New(ctx, db)`. The application owns the
credentials, pool configuration, and `db.Close`. Remote migration locking
requires at least two open connections so lease renewal cannot self-block; Cord
returns an error instead of migrating when `MaxOpenConns` is one. Do not
configure local-file WAL or locking pragmas for a remote URL. Cord uses an
internal database lock to serialize migrations rather than SQLite's file
locking.

## Register workflows at startup

Define every workflow a process may execute immediately after creating its Cord
runtime, before marking the process ready or serving requests. Calling `From` and
`Then` registers the step implementations; it does not submit a run.

```go
func defineOrderWorkflow(runtime *cord.Cord) cord.Workflow[OrderID, Receipt] {
    return runtime.
        From("fulfill-order", loadOrder).
        Then(validateOrder).
        Then(chargeOrder)
}

func runService(ctx context.Context, db *sql.DB) (err error) {
    runtime, err := cord.New(ctx, db)
    if err != nil {
        return err
    }
    defer func() {
        if closeErr := runtime.Close(); err == nil {
            err = closeErr
        }
    }()

    orders := defineOrderWorkflow(runtime) // register before readiness

    server := newServer(orders)
    return server.Serve(ctx)
}
```

After a restart, persisted nodes are claimed only when their exact function keys
and signatures have been registered in the new process. A workflow defined lazily
inside a request handler can therefore leave existing work dormant until that
handler runs again. Keep workflow definitions in startup code and pass the
resulting immutable handles to handlers and services.

## Compose workflows

### Linear pipelines

`Then` connects the current output to the next step. Go checks every connection
at compile time.

```go
func validate(ctx context.Context, order Order) (ValidatedOrder, error) { /* ... */ }
func reserve(ctx context.Context, order ValidatedOrder) (Reservation, error) { /* ... */ }
func confirm(ctx context.Context, reservation Reservation) (Receipt, error) { /* ... */ }

flow := runtime.
    From("fulfill-order", validate).
    Then(reserve).
    Then(confirm)

receipt, err := flow.Run(ctx, order)
```

Workflow handles are immutable, so the same node can be the root of multiple
branches.

### Branch and join

Build each branch from a shared handle, then combine their outputs with `Join`.
The joined step runs after both branches complete.

```go
func loadOrder(ctx context.Context, id OrderID) (Order, error) { /* ... */ }
func priceItems(ctx context.Context, order Order) (Money, error) { /* ... */ }
func priceShipping(ctx context.Context, order Order) (Money, error) { /* ... */ }
func add(_ context.Context, items, shipping Money) (Money, error) {
    return items.Add(shipping), nil
}

root := runtime.From("order-total", loadOrder)
items := root.Then(priceItems)
shipping := root.Then(priceShipping)
total := cord.Join(items, shipping).Then(add)

amount, err := total.Run(ctx, orderID)
```

See the reusable [`linear`](./examples/linear) and
[`join`](./examples/join) workflow packages. Each includes executable
[`sqlite`](./examples/linear/sqlite) and [`pg`](./examples/linear/pg) commands;
the PostgreSQL commands read `CORD_POSTGRES_DSN`.

## Retries and terminal failures

A step returning an error is retried up to three attempts by default. Mark an
error permanent when another attempt cannot succeed:

```go
func charge(ctx context.Context, payment Payment) (Receipt, error) {
    if err := payment.Validate(); err != nil {
        return Receipt{}, cord.Permanent(err)
    }
    return gateway.Charge(ctx, payment)
}
```

Configure the retry policy when creating the runtime:

```go
runtime, err := cord.New(ctx, database, cord.Options{
    MaxAttempts:    5,
    RetryBaseDelay: 250 * time.Millisecond,
    RetryMaxDelay:  20 * time.Second,
})
```

Each retry field is optional and defaults independently: `MaxAttempts` to 3,
`RetryBaseDelay` to 500 milliseconds, and `RetryMaxDelay` to 30 seconds. For
example, setting only `MaxAttempts` retains both default delays. Values must be
positive after defaulting, and the maximum delay must be at least the base
delay.

The policy is stored with the run, so every worker applies the same retry
semantics. Existing runs retain the policy with which they were submitted.

## Architecture

Cord separates workflow definition, durable state, and execution:

```text
Go functions
     │  From / Then / Join
     ▼
typed workflow graph
     │  Run(ctx, input) or Submit(ctx, input[, key])
     ▼
SQL: run + nodes + edges + retry policy + optional idempotency key
     │
     ├──────────────┬──────────────┐
     ▼              ▼              ▼
 runtime A       runtime B      runtime C
 leased worker   leased worker  leased worker
     │              │              │
     └──────────────┴──────────────┘
                    │
                    ▼
        durable output or failure
```

1. **Define** — `From`, `Then`, and `Join` build an immutable typed graph.
2. **Submit** — `Run` compiles the reachable graph and atomically stores its
   topology, input, and execution policy.
3. **Execute** — schedulers claim ready nodes with leases. A completed node
   persists its output and makes its dependents eligible to run.
4. **Recover** — expired claims become available to another runtime. Recreating
   the workflow definitions registers the functions needed to continue pending
   runs.
5. **Complete** — `Run` or `Get` decodes the terminal node's persisted output
   into the workflow's Go result type.

Multiple compatible Cord runtimes may coordinate through the same database.
`Submit` wakes its local scheduler, while other runtimes discover work by normal
polling. Any runtime with matching function registrations may execute the run;
a different compatible runtime may later call `Get` or `Cancel`. Closing the
submitter does not cancel durable work. Durable state—not the process that
submitted or executed a run—is authoritative. Workflow inputs, intermediate
values, outputs, and failures cross the storage boundary as JSON.

## Side effects and idempotency

Cord executes nodes at least once. A node can finish an external side effect and
then lose its lease before Cord persists completion, so a later attempt may run
the node again. Workflows are therefore not automatically idempotent:
side-effecting steps must use business identifiers, destination-supported
idempotency keys, unique constraints, or another application-level deduplication
mechanism.

Workflow-submission deduplication is separate from node retry identity and
external side-effect idempotency. A caller-provided `Submit` idempotency key can
deduplicate creation of a retained run, but it does not make node execution or
external effects exactly once. Cord generates run IDs; callers do not select
them.

A database commit can succeed even when the submitting or completing process
receives an error. After any ambiguous unkeyed `Submit`, retrying can create a
second run. For retry-safe submission, choose and durably retain an idempotency
key before the first attempt, then retry with the same workflow definition,
exact encoded input, and key until Cord returns a run ID or a definitive
conflict. Do not discard or reuse the key merely because an attempt returned a
transport, timeout, context, or other persistence error: the commit may have
succeeded. Node functions must still make their own external effects idempotent
because Cord executes nodes at least once.

## Runtime lifecycle

`New` applies pending schema migrations and starts the scheduler. Cord owns its
scheduler goroutines; the application continues to own the database.

```go
runtime, err := cord.New(ctx, db)
if err != nil {
    return err
}
defer runtime.Close() // close the runtime before db
```

Canceling the context passed to `Run` stops submission or waiting, but does not
cancel a run that was already persisted. A `Submit` context controls validation
and persistence only; after a successful return, its cancellation does not
cancel the run. A `Get` context controls only that caller's wait. Use `Cancel`
for durable cancellation. The durable workflow can otherwise complete in any
compatible Cord process. Closing a runtime similarly stops its local workers
without closing the database or canceling persisted runs.

For scheduler tuning, pass an `Options` value to `New`:

```go
runtime, err := cord.New(ctx, db, cord.Options{
    Concurrency:       8,
    PollInterval:      100 * time.Millisecond,
    LeaseTTL:          30 * time.Second,
    HeartbeatInterval: 10 * time.Second,
    OnSchedulerError:  func(err error) { logger.Error("cord scheduler", "error", err) },
})
```

Omit `Options` to use Cord's defaults; any zero-valued field in a supplied
`Options` also uses its own default. The context passed to `New` controls schema
migration and is bounded by Cord's migration timeout.

`Concurrency` limits executing node functions, not SQL connections. Size the
caller-owned pool for application queries, Cord's concurrent scheduler and
worker transitions, migration overhead, and blocking `Get` polling. Remote
Turso migrations require at least two open connections. In every deployment,
monitor `sql.DB.Stats`—especially waits—and increase the pool or reduce runtime
concurrency when the database is saturated.

See the [package reference](https://pkg.go.dev/github.com/omarluq/cord) for the
complete API.

## Development

This project uses [mise](https://mise.jdx.dev/) to pin Go, Task, and Bun. A
fresh checkout can provision those tools and the lockfile-pinned browser
packages with:

```bash
mise install
mise exec -- task playground-install
```

Validation is split by portability and purpose:

| Gate | Requirements | Coverage |
|---|---|---|
| `mise exec -- task build-library` | Go only; CGO disabled | Portable Cord library build |
| `mise exec -- go test ./...` | Go, CGO toolchain, Docker | Library tests, including mandatory mattn and Turso/libSQL integration |
| `CORD_POSTGRES_DSN=... mise exec -- task test-postgres` | Go and PostgreSQL | Mandatory PostgreSQL integration/conformance tests and executable pgx example |
| `mise exec -- task build` | Go and CGO toolchain | All native workspace packages |
| `mise exec -- task ci` | Go, Task, Bun, CGO toolchain, Docker | Formatting, lint, dead code, race-tested drivers (including Turso), portable and native builds, and playground assets |
| `mise exec -- task playground-test` | CI prerequisites, Chromium system libraries, and network access for the first browser install | Browser smoke test |

Docker is a hard prerequisite for the mandatory Turso tests; they do not
silently skip. `test-postgres` likewise fails if `CORD_POSTGRES_DSN` is absent or
PostgreSQL is unreachable. PR CI uses PostgreSQL 18; scheduled and release gates
run the PostgreSQL 14–18 matrix. Task installs frozen Bun dependencies, and the browser test
installs pinned Playwright Chromium automatically. Linux is the hosted-CI reference platform;
the pure-Go library build is the portability gate for other supported Go
platforms. Cord never changes caller-owned database pool settings. Remote
Turso migrations need at least two available connections, and applications
should monitor `sql.DB.Stats` pool waits under scheduler load.

## License

[MIT](./LICENSE.txt)

<a href="https://sonarcloud.io/summary/new_code?id=omarluq_cord"><img src="https://sonarcloud.io/images/project_badges/sonarcloud-dark.svg" alt="SonarCloud Quality Gate"></a>
