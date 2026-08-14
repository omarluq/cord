<p align="center">
  <img height="300" src="assets/banner.png" alt="Cord banner">
</p>

<h1 align="center">cord</h1>

<p align="center">
  <a href="https://github.com/omarluq/cord/actions/workflows/ci.yml"><img src="https://github.com/omarluq/cord/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://codecov.io/gh/omarluq/cord"><img src="https://codecov.io/gh/omarluq/cord/branch/main/graph/badge.svg" alt="codecov"></a>
  <a href="https://pkg.go.dev/github.com/omarluq/cord"><img src="https://pkg.go.dev/badge/github.com/omarluq/cord.svg" alt="Go Reference"></a>
  <a href="https://github.com/omarluq/cord/releases"><img src="https://img.shields.io/github/v/release/omarluq/cord?label=version&logo=semver" alt="Version"></a>
  <a href="./LICENSE.txt"><img src="https://img.shields.io/github/license/omarluq/cord" alt="License"></a>
</p>

Cord is a durable workflow library for Go. Compose ordinary typed functions into
linear or branching workflows; Cord persists each run in SQLite and resumes
available work after a process restart.

```go
flow := runtime.From("order-total", loadOrder)
items := flow.Then(priceItems)
shipping := flow.Then(priceShipping)
orderTotal := cord.Join(items, shipping).Then(add)

total, err := orderTotal.Run(ctx, orderID)
```

## Install

```bash
go get github.com/omarluq/cord
go get modernc.org/sqlite
```

Cord works with a caller-owned `*sql.DB` backed by SQLite. The examples use the
pure-Go [`modernc.org/sqlite`](https://pkg.go.dev/modernc.org/sqlite) driver.

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

    runtime, err := cord.New(db)
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

func runService(ctx context.Context, db *sql.DB) error {
    runtime, err := cord.New(db)
    if err != nil {
        return err
    }
    defer runtime.Close()

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

See [`examples/linear`](./examples/linear) and
[`examples/join`](./examples/join) for complete programs.

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

Configure the policy used by future runs before submitting them:

```go
err := runtime.SetRetryPolicy(cord.RetryPolicy{
    MaxAttempts: 5,
    BaseDelay:   250 * time.Millisecond,
    MaxDelay:    20 * time.Second,
})
```

The policy is stored with the run, so every worker applies the same retry
semantics. Existing runs retain the policy with which they were submitted.

## Architecture

Cord separates workflow definition, durable state, and execution:

```text
Go functions
     │  From / Then / Join
     ▼
typed workflow graph
     │  Run(ctx, input)
     ▼
SQLite: run + nodes + edges + retry policy
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
5. **Complete** — the terminal node's persisted output is decoded into the
   workflow's Go result type.

Multiple Cord runtimes may coordinate through the same SQLite database. Durable
state—not the process that submitted or executed a run—is authoritative.
Workflow inputs, intermediate values, outputs, and failures cross the storage
boundary as JSON.

## Side effects and idempotency

Cord executes nodes at least once. A node can finish an external side effect and
then lose its lease before Cord persists completion, so a later attempt may run
the node again. Workflows are therefore not automatically idempotent:
side-effecting steps must use business identifiers, destination-supported
idempotency keys, unique constraints, or another application-level deduplication
mechanism.

Workflow-submission deduplication is a separate concern from retry identity and
external side-effect idempotency. Cord does not currently provide either a
caller-selected run ID or exactly-once external effects.

## Runtime lifecycle

`New` applies pending schema migrations and starts the scheduler. Cord owns its
scheduler goroutines; the application continues to own the database.

```go
runtime, err := cord.New(db)
if err != nil {
    return err
}
defer runtime.Close() // close the runtime before db
```

Canceling the context passed to `Run` stops submission or waiting, but does not
cancel a run that was already persisted. The durable workflow continues and can
complete in any compatible Cord process. Closing a runtime similarly stops its
local workers without closing the database or canceling persisted runs.

For scheduler tuning, use `NewWithOptions`:

```go
runtime, err := cord.NewWithOptions(db, cord.RuntimeOptions{
    Concurrency:       8,
    PollInterval:      100 * time.Millisecond,
    LeaseTTL:          30 * time.Second,
    HeartbeatInterval: 10 * time.Second,
    OnSchedulerError:  func(err error) { logger.Error("cord scheduler", "error", err) },
})
```

Use `NewWithOptionsContext` when migration should observe a startup context.
See the [package reference](https://pkg.go.dev/github.com/omarluq/cord) for the
complete API.

## Development

This project uses [mise](https://mise.jdx.dev/) and [Task](https://taskfile.dev/):

```bash
mise install
mise exec -- task ci
```

## License

[MIT](./LICENSE.txt)

<a href="https://sonarcloud.io/summary/new_code?id=omarluq_cord"><img src="https://sonarcloud.io/images/project_badges/sonarcloud-dark.svg" alt="SonarCloud Quality Gate"></a>
