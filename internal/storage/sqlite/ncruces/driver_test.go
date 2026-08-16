package ncruces_test

import (
	"database/sql"
	"fmt"
	"net/url"
	"testing"
	"time"

	_ "github.com/ncruces/go-sqlite3/driver"
	"github.com/omarluq/cord/internal/storage/conformance"
)

func TestDriverConformance(t *testing.T) {
	t.Parallel()
	conformance.Run(t, openDatabase)
}

func openDatabase(tb testing.TB, path string, timeout time.Duration) *sql.DB {
	tb.Helper()

	query := url.Values{}
	query.Add("_pragma", fmt.Sprintf("busy_timeout(%d)", timeout.Milliseconds()))
	query.Add("_pragma", "foreign_keys(1)")
	query.Add("_pragma", "journal_mode(WAL)")

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
