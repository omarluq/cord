package modernc_test

import (
	"testing"

	"github.com/omarluq/cord/internal/storage/conformance"
	// Register the modernc sqlite database/sql driver for this conformance test binary.
	_ "modernc.org/sqlite"
)

func TestDriverConformance(t *testing.T) {
	t.Parallel()
	conformance.Run(t, conformance.Driver{
		Name:       "sqlite",
		DataSource: conformance.RepeatedPragmaDataSource,
	})
}
