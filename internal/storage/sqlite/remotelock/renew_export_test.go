package remotelock

import (
	"context"
	"database/sql"
	"time"
)

// RenewForTest runs lease renewal and reports each successful renewal.
func RenewForTest(
	ctx context.Context,
	connection *sql.Conn,
	owner string,
	interval time.Duration,
	renewed chan<- struct{},
) error {
	return renew(ctx, connection, owner, interval, renewed)
}
