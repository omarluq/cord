package sqlite_test

import (
	"github.com/omarluq/cord/internal/storage"
	"github.com/omarluq/cord/internal/storage/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"testing"
	"time"
)

func TestStore_CreateRunAttachesCompatibleIdempotentSubmission(t *testing.T) {
	t.Parallel()

	database, store := newStore(t, true)
	key, fingerprint := "order-42", testSubmissionFingerprint
	first := validPlan(time.Now().UTC(), "first-run")
	first.Run.IdempotencyKey = &key
	first.Run.SubmissionFingerprint = &fingerprint

	runID, created := requireCreateOrAttachRun(t.Context(), t, store, &first)
	assert.Equal(t, first.Run.ID, runID)
	assert.True(t, created)

	second := validPlan(time.Now().UTC(), "second-run")
	second.Run.IdempotencyKey = &key
	second.Run.SubmissionFingerprint = &fingerprint
	runID, created = requireCreateOrAttachRun(t.Context(), t, store, &second)
	assert.Equal(t, first.Run.ID, runID)
	assert.False(t, created)
	assert.Equal(t, 1, rowCount(t, database, runsTable))

	var input []byte
	require.NoError(t, database.QueryRowContext(t.Context(),
		"SELECT input_payload FROM cord_runs WHERE id = ?", first.Run.ID,
	).Scan(&input))
	assert.Equal(t, []byte(first.Run.Input), input)
}

func TestStore_CreateRunRejectsConflictingIdempotentSubmission(t *testing.T) {
	t.Parallel()

	database, store := newStore(t, true)
	key, fingerprint := "order-42", testSubmissionFingerprint
	first := validPlan(time.Now().UTC(), "first-run")
	first.Run.IdempotencyKey = &key
	first.Run.SubmissionFingerprint = &fingerprint
	requireCreateOrAttachRun(t.Context(), t, store, &first)

	second := validPlan(time.Now().UTC(), "second-run")
	second.Run.IdempotencyKey = &key
	otherFingerprint := "submission-v1:different"
	second.Run.SubmissionFingerprint = &otherFingerprint
	_, _, err := store.CreateOrAttachRun(t.Context(), &second)
	require.ErrorIs(t, err, storage.ErrRunConflict)
	assert.Equal(t, 1, rowCount(t, database, runsTable))
}

func TestStore_CreateRunConcurrentIdempotentSubmissionsAttach(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "idempotent.db")
	firstDatabase := openDatabaseAtPath(t, path, true)
	secondDatabase := openDatabaseAtPath(t, path, true)
	require.NoError(t, sqlite.Migrate(t.Context(), firstDatabase))
	firstStore, err := sqlite.New(firstDatabase)
	require.NoError(t, err)
	secondStore, err := sqlite.New(secondDatabase)
	require.NoError(t, err)

	key, fingerprint := "concurrent-key", testSubmissionFingerprint

	plans := []storage.RunPlan{
		validPlan(time.Now().UTC(), "concurrent-first"),
		validPlan(time.Now().UTC(), "concurrent-second"),
	}
	for index := range plans {
		plans[index].Run.IdempotencyKey = &key
		plans[index].Run.SubmissionFingerprint = &fingerprint
	}

	start := make(chan struct{})

	type result struct {
		err     error
		id      storage.RunID
		created bool
	}

	results := make(chan result, 2)

	for index, store := range []*sqlite.Store{firstStore, secondStore} {
		go func() {
			<-start

			id, created, createErr := store.CreateOrAttachRun(t.Context(), &plans[index])
			results <- result{id: id, created: created, err: createErr}
		}()
	}

	close(start)

	firstResult, secondResult := <-results, <-results
	require.NoError(t, firstResult.err)
	require.NoError(t, secondResult.err)
	assert.Equal(t, firstResult.id, secondResult.id)
	assert.NotEqual(t, firstResult.created, secondResult.created)
	assert.Equal(t, 1, rowCount(t, firstDatabase, runsTable))
}

func TestStore_CreateRunAllowsSameKeyForDifferentWorkflows(t *testing.T) {
	t.Parallel()

	_, store := newStore(t, true)
	key, fingerprint := "same-key", testSubmissionFingerprint
	first := validPlan(time.Now().UTC(), "first-run")
	first.Run.IdempotencyKey = &key
	first.Run.SubmissionFingerprint = &fingerprint
	requireCreateOrAttachRun(t.Context(), t, store, &first)

	second := validPlan(time.Now().UTC(), "second-run")
	second.Run.WorkflowName = "other-workflow"
	second.Run.IdempotencyKey = &key
	second.Run.SubmissionFingerprint = &fingerprint
	_, created := requireCreateOrAttachRun(t.Context(), t, store, &second)
	assert.True(t, created)
}

func TestStore_CreateRunDuplicatePreservesOriginal(t *testing.T) {
	t.Parallel()

	database, store := newStore(t, true)
	plan := validPlan(time.Now().UTC(), "duplicate-run")
	requireCreateRun(t.Context(), t, store, &plan)

	duplicate := validPlan(time.Now().UTC(), "duplicate-run")
	duplicate.Run.WorkflowName = "replacement"
	requireCreateRunError(t.Context(), t, store, &duplicate)

	var workflowName string

	err := database.QueryRowContext(
		t.Context(),
		"SELECT workflow_name FROM cord_runs WHERE id = ?",
		plan.Run.ID,
	).Scan(&workflowName)
	require.NoError(t, err)
	assert.Equal(t, plan.Run.WorkflowName, workflowName)

	var (
		maxAttempts, retryPolicyVersion   int
		retryBaseDelayNS, retryMaxDelayNS int64
	)

	err = database.QueryRowContext(t.Context(), `SELECT max_attempts, retry_base_delay_ns,
		retry_max_delay_ns, retry_policy_version FROM cord_runs WHERE id = ?`, plan.Run.ID).Scan(
		&maxAttempts,
		&retryBaseDelayNS,
		&retryMaxDelayNS,
		&retryPolicyVersion,
	)
	require.NoError(t, err)
	assert.Equal(t, plan.Run.MaxAttempts, maxAttempts)
	assert.Equal(t, plan.Run.RetryBaseDelay.Nanoseconds(), retryBaseDelayNS)
	assert.Equal(t, plan.Run.RetryMaxDelay.Nanoseconds(), retryMaxDelayNS)
	assert.Equal(t, plan.Run.RetryPolicyVersion, retryPolicyVersion)
	assert.Equal(t, 1, rowCount(t, database, runsTable))
}
