package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

func nullPayload(payload storage.EncodedPayload) any {
	if payload == nil {
		return nil
	}

	return []byte(payload)
}

func nullStringPointer(value *string) any {
	if value == nil {
		return nil
	}

	return *value
}

func nullString(value string) any {
	if value == "" {
		return nil
	}

	return value
}

func nullTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}

	return formatTime(value)
}

func nullTimePointer(value *time.Time) any {
	if value == nil || value.IsZero() {
		return nil
	}

	return formatTime(*value)
}

func formatTime(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}

func databaseInstant(ctx context.Context, transaction *sql.Tx) (time.Time, error) {
	var value string
	if err := transaction.QueryRowContext(
		ctx,
		`SELECT strftime('%Y-%m-%dT%H:%M:%fZ', 'now')`,
	).Scan(&value); err != nil {
		return time.Time{}, fmt.Errorf("read database time: %w", err)
	}

	instant, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse database time: %w", err)
	}

	return instant.UTC(), nil
}
