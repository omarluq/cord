package sqlite

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/omarluq/cord/internal/storage"
)

func validateReason(
	destination *storage.TerminalReason,
	reason sql.NullString,
	terminal bool,
	state string,
	allowsReason func(storage.TerminalReason) bool,
) error {
	if reason.Valid {
		*destination = storage.TerminalReason(reason.String)
	}

	if terminal != reason.Valid {
		return errors.New("terminal state and reason disagree")
	}

	if !allowsReason(*destination) || (*destination != "" && !destination.IsKnown()) {
		return fmt.Errorf("reason %q is invalid for state %q", *destination, state)
	}

	return nil
}
