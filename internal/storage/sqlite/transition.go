package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

var errFenceRejected = errors.New("lease fence rejected")

func (s *Store) fencedTerminalTransition(
	ctx context.Context,
	transition func(*sql.Tx) error,
) (accepted bool, err error) {
	transaction, err := s.database.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin fenced transition: %w", err)
	}

	defer func() {
		if rollbackErr := transaction.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			err = errors.Join(err, fmt.Errorf("rollback fenced transition: %w", rollbackErr))
		}
	}()

	transitionErr := transition(transaction)
	if errors.Is(transitionErr, errFenceRejected) {
		return false, nil
	}

	if transitionErr != nil {
		return false, transitionErr
	}

	if err = transaction.Commit(); err != nil {
		return false, fmt.Errorf("commit fenced transition: %w", err)
	}

	return true, nil
}

func (s *Store) updateNodes(ctx context.Context, query, operation string, arguments ...any) (int64, error) {
	result, err := s.database.ExecContext(ctx, query, arguments...)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", operation, err)
	}

	count, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("inspect %s: %w", operation, err)
	}

	return count, nil
}

func affectedOne(result sql.Result) (bool, error) {
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read affected rows: %w", err)
	}

	return rows == 1, nil
}
