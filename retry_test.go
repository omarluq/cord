package cord_test

import (
	"errors"
	"testing"

	"github.com/omarluq/cord"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPermanent(t *testing.T) {
	t.Parallel()

	cause := errors.New("stop")
	marked := cord.Permanent(cause)

	require.ErrorIs(t, marked, cause)
	assert.Equal(t, cause.Error(), marked.Error())
	assert.NoError(t, cord.Permanent(nil))
}
