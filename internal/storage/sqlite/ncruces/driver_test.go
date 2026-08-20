package ncruces_test

import (
	"testing"

	// Register the ncruces sqlite3 database/sql driver for this conformance test binary.
	_ "github.com/ncruces/go-sqlite3/driver"
	"github.com/omarluq/cord/internal/storage/conformance"
)

func TestDriverConformance(t *testing.T) {
	t.Parallel()
	conformance.Run(t, conformance.Driver{
		DataSource:          conformance.RepeatedPragmaDataSource,
		Name:                "sqlite3",
		Open:                nil,
		SkipWriteContention: false,
	})
}
