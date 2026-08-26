package cord_test

import (
	"testing"

	"github.com/omarluq/cord"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWorkflow_ParentOrderChangesDefinitionHash(t *testing.T) {
	t.Parallel()

	database, runtime := newRuntime(t)
	root := runtime.From("parent-order-definition", passThrough)

	_, err := cord.Join(root.Then(timesTwo), root.Then(addOne)).Then(subtract).Run(t.Context(), 1)
	require.NoError(t, err)

	_, err = cord.Join(root.Then(addOne), root.Then(timesTwo)).Then(subtract).Run(t.Context(), 1)
	require.NoError(t, err)

	rows, err := database.QueryContext(
		t.Context(),
		"SELECT definition_hash FROM cord_runs ORDER BY created_at, id",
	)
	require.NoError(t, err)

	defer func() { require.NoError(t, rows.Close()) }()

	var hashes []string

	for rows.Next() {
		var hash string

		require.NoError(t, rows.Scan(&hash))
		hashes = append(hashes, hash)
	}

	require.NoError(t, rows.Err())
	require.Len(t, hashes, 2)
	assert.NotEqual(t, hashes[0], hashes[1])
}
