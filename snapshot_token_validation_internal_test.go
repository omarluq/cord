package cord

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNodePageTokenRejectsModificationAndVersion(t *testing.T) {
	t.Parallel()

	token, err := encodeNodePageToken(nodePageToken{
		RunID: snapshotRunID, State: NodeStateReady, LastNodeID: snapshotNodeID,
	})
	require.NoError(t, err)

	modified, err := decodeTokenWire(token)
	require.NoError(t, err)

	modified.Payload.LastNodeID = "modified"
	_, err = decodeNodePageToken(encodeTokenWire(t, &modified))
	require.ErrorContains(t, err, "checksum")

	unsupported, err := decodeTokenWire(token)
	require.NoError(t, err)

	unsupported.Version++
	_, err = decodeNodePageToken(encodeTokenWire(t, &unsupported))
	require.ErrorContains(t, err, "unsupported")
}

func TestNodePageTokenEncodingIsDeterministicAndBounded(t *testing.T) {
	t.Parallel()

	for _, token := range []nodePageToken{
		{RunID: snapshotRunID, LastNodeID: snapshotNodeID},
		{
			RunID: snapshotRunID, State: NodeStateFailed,
			Reason: ReasonFailureLeaseExpired, LastNodeID: "node-世界",
		},
	} {
		first, err := encodeNodePageToken(token)
		require.NoError(t, err)
		second, err := encodeNodePageToken(token)
		require.NoError(t, err)

		assert.Equal(t, first, second)
		assert.LessOrEqual(t, len(first), nodePageTokenMaxLen)
		decoded, err := decodeNodePageToken(first)
		require.NoError(t, err)
		assert.Equal(t, token, decoded)
	}

	_, err := decodeNodePageToken(string(make([]byte, nodePageTokenMaxLen+1)))
	assert.ErrorContains(t, err, "length")
}
func decodeTokenWire(token string) (nodePageTokenWire, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nodePageTokenWire{}, fmt.Errorf("decode token wire: %w", err)
	}

	var wire nodePageTokenWire

	if err := json.Unmarshal(decoded, &wire); err != nil {
		return nodePageTokenWire{}, fmt.Errorf("unmarshal token wire: %w", err)
	}

	return wire, nil
}

func encodeTokenWire(t *testing.T, wire *nodePageTokenWire) string {
	t.Helper()

	encoded, err := json.Marshal(wire)
	require.NoError(t, err)

	return base64.RawURLEncoding.EncodeToString(encoded)
}
