package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

var errFenceRejected = errors.New("lease fence rejected")

func (s *Store) fencedTerminalTransition(
	ctx context.Context,
	leaseExpiresAt time.Time,
	transition func(context.Context, *sql.Tx) error,
) (accepted bool, err error) {
	err = retryFencedContention(ctx, "retry fenced transition", leaseExpiresAt, func(attemptCtx context.Context) error {
		accepted, err = s.fencedTerminalTransitionOnce(attemptCtx, transition)

		return err
	})

	return accepted, err
}

func (s *Store) fencedTerminalTransitionOnce(
	ctx context.Context,
	transition func(context.Context, *sql.Tx) error,
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

	transitionErr := transition(ctx, transaction)
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

func (s *Store) updateNodes(ctx context.Context, query, operation string, arguments ...any) (count int64, err error) {
	err = retryContention(ctx, "retry "+operation, func(attemptCtx context.Context) error {
		result, execErr := s.database.ExecContext(attemptCtx, query, arguments...)
		if execErr != nil {
			return fmt.Errorf("%s: %w", operation, execErr)
		}

		count, execErr = result.RowsAffected()
		if execErr != nil {
			return fmt.Errorf("inspect %s: %w", operation, execErr)
		}

		return nil
	})

	return count, err
}

func affectedOne(result sql.Result) (bool, error) {
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read affected rows: %w", err)
	}

	return rows == 1, nil
}
