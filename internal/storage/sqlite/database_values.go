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

func parseRequiredTime(value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse RFC3339 timestamp: %w", err)
	}

	return parsed.UTC(), nil
}

func parseOptionalTime(value sql.NullString, destination **time.Time) error {
	if !value.Valid {
		return nil
	}

	parsed, err := parseRequiredTime(value.String)
	if err != nil {
		return err
	}

	*destination = &parsed

	return nil
}

func setOptionalRunnerID(destination **storage.RunnerID, value sql.NullString) {
	if value.Valid {
		runnerID := storage.RunnerID(value.String)
		*destination = &runnerID
	}
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
