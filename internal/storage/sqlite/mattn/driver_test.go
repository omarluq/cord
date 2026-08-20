package mattn_test

import (
	"testing"

	// Register the mattn sqlite3 database/sql driver for this conformance test binary.
	_ "github.com/mattn/go-sqlite3"
	"github.com/omarluq/cord/internal/storage/conformance"
)

func TestDriverConformance(t *testing.T) {
	t.Parallel()
	conformance.Run(t, conformance.Driver{
		DataSource:          conformance.UnderscoreDataSource,
		Name:                "sqlite3",
		Open:                nil,
		SkipWriteContention: false,
	})
}
