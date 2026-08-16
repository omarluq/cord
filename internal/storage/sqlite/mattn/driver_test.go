package mattn_test

import (
	"database/sql"
	"net/url"
	"strconv"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/omarluq/cord/internal/storage/conformance"
)

func TestDriverConformance(t *testing.T) {
	t.Parallel()
	conformance.Run(t, openDatabase)
}

func openDatabase(tb testing.TB, path string, timeout time.Duration) *sql.DB {
	tb.Helper()

	query := url.Values{}
	query.Set("_busy_timeout", strconv.FormatInt(timeout.Milliseconds(), 10))
	query.Set("_foreign_keys", "on")
	query.Set("_journal_mode", "WAL")

	database, err := sql.Open("sqlite3", "file:"+path+"?"+query.Encode())
	if err != nil {
		tb.Fatal(err)
	}

	tb.Cleanup(func() {
		if err := database.Close(); err != nil {
			tb.Errorf("close database: %v", err)
		}
	})

	return database
}
