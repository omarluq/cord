package postgres

import (
	"fmt"

	"github.com/omarluq/cord/internal/storage"
)

func incompatible(format string, arguments ...any) error {
	return fmt.Errorf("%w: %s", storage.ErrRunIncompatible, fmt.Sprintf(format, arguments...))
}
