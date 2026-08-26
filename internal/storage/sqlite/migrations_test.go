package sqlite_test

import (
	"database/sql"
	"errors"
	ncruces "github.com/ncruces/go-sqlite3"
	"github.com/omarluq/cord/internal/storage/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"sync"
	"testing"
)

func TestMigrateConcurrentConnections(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "concurrent.db")
	first := openZeroTimeoutDatabase(t, path)
	second := openZeroTimeoutDatabase(t, path)

	start := make(chan struct{})
	results := make(chan error, 2)

	var waitGroup sync.WaitGroup
	for _, database := range []*sql.DB{first, second} {
		waitGroup.Go(func() {
			<-start

			results <- sqlite.Migrate(t.Context(), database)
		})
	}

	close(start)
	waitGroup.Wait()
	close(results)

	for err := range results {
		require.NoError(t, err)
	}

	require.NoError(t, sqlite.Verify(t.Context(), first))
	require.NoError(t, sqlite.Verify(t.Context(), second))

	rows, err := first.QueryContext(t.Context(), `SELECT version_id, COUNT(*)
		FROM cord_schema_migrations WHERE is_applied = 1 AND version_id > 0
		GROUP BY version_id ORDER BY version_id`)

	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()

	var versions []int64

	for rows.Next() {
		var (
			version int64
			applied int
		)

		require.NoError(t, rows.Scan(&version, &applied))
		assert.Equal(t, 1, applied)

		versions = append(versions, version)
	}

	require.NoError(t, rows.Err())
	assert.Equal(t, []int64{1, 2, 3, 4, 5}, versions)
}

func TestMigrationRetryableClassification(t *testing.T) {
	t.Parallel()

	tests := []struct {
		err  error
		name string
		want bool
	}{
		{name: "no error", err: nil, want: false},
		{name: "busy", err: ncruces.BUSY, want: true},
		{
			name: "remote interrupt",
			err:  errors.New(`Hrana stream error: code: "SQLITE_INTERRUPT"`),
			want: true,
		},
		{name: "unstructured interrupt", err: errors.New("interrupted"), want: false},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, testCase.want, sqlite.IsMigrationRetryableForTest(testCase.err))
		})
	}
}
