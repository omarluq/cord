package postgres

import (
	"database/sql"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

func nullablePayload(value storage.EncodedPayload) any {
	if value == nil {
		return nil
	}

	return []byte(value)
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}

	return value
}

func nullableStringPointer(value *string) any {
	if value == nil {
		return nil
	}

	return *value
}

func nullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}

	return value
}

func nullableTimePointer(value *time.Time) any {
	if value == nil || value.IsZero() {
		return nil
	}

	return *value
}

func utcTime(value sql.NullTime) *time.Time {
	if !value.Valid {
		return nil
	}

	instant := value.Time.UTC()

	return &instant
}

func runnerID(value sql.NullString) *storage.RunnerID {
	if !value.Valid || value.String == "" {
		return nil
	}

	id := storage.RunnerID(value.String)

	return &id
}
