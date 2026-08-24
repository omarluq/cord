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
	leaseRemaining time.Duration,
	transition func(context.Context, *sql.Tx) error,
) (accepted bool, err error) {
	err = retryFencedContention(ctx, "retry fenced transition", leaseRemaining, func(attemptCtx context.Context) error {
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

func joinRollbackError(rollbackErr error, operation string, operationErr error) error {
	if rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
		return errors.Join(operationErr, fmt.Errorf("%s: %w", operation, rollbackErr))
	}

	return operationErr
}

func affectedOne(result sql.Result) (bool, error) {
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read affected rows: %w", err)
	}

	return rows == 1, nil
}
