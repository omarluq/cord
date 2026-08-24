package cord

import (
	"testing"
	"unicode/utf8"
)

func FuzzDecodeNodePageToken(f *testing.F) {
	valid, err := encodeNodePageToken(nodePageToken{
		RunID: snapshotRunID, State: NodeStateReady, Reason: "", LastNodeID: snapshotNodeID,
	})
	if err != nil {
		f.Fatalf("seed token: %v", err)
	}

	f.Add("")
	f.Add("not-a-token")
	f.Add(valid)
	f.Add(string(make([]byte, nodePageTokenMaxLen+1)))

	f.Fuzz(func(t *testing.T, token string) {
		decoded, decodeErr := decodeNodePageToken(token)
		if decodeErr != nil {
			return
		}

		encoded, encodeErr := encodeNodePageToken(decoded)
		if encodeErr != nil {
			t.Fatalf("re-encode decoded token: %v", encodeErr)
		}

		roundTrip, roundTripErr := decodeNodePageToken(encoded)
		if roundTripErr != nil {
			t.Fatalf("decode round-trip token: %v", roundTripErr)
		}

		if roundTrip != decoded {
			t.Fatalf("round-trip token = %#v, want %#v", roundTrip, decoded)
		}
	})
}

func FuzzNodePageTokenRoundTrip(f *testing.F) {
	f.Add(snapshotRunID, "ready", "", snapshotNodeID)
	f.Add("run/with spaces", "failed", "failure_lease_expired", "node/with spaces")
	f.Add("run-世界", "canceled", "future_reason", "node-世界")

	f.Fuzz(func(t *testing.T, runID, state, reason, nodeID string) {
		if !allValidUTF8(runID, state, reason, nodeID) {
			return
		}

		token := nodePageToken{
			RunID: RunID(runID), State: NodeState(state),
			Reason: TerminalReason(reason), LastNodeID: NodeID(nodeID),
		}

		encoded, encodeErr := encodeNodePageToken(token)
		if encodeErr != nil {
			t.Fatalf("encode token: %v", encodeErr)
		}

		decoded, decodeErr := decodeNodePageToken(encoded)
		if encodedTokenShouldFail(runID, nodeID, encoded) {
			if decodeErr == nil {
				t.Fatalf("decode invalid token = %#v, want error", decoded)
			}

			return
		}

		if decodeErr != nil {
			t.Fatalf("decode encoded token: %v", decodeErr)
		}

		if decoded != token {
			t.Fatalf("decoded token = %#v, want %#v", decoded, token)
		}
	})
}

func allValidUTF8(values ...string) bool {
	for _, value := range values {
		if !utf8.ValidString(value) {
			return false
		}
	}

	return true
}

func encodedTokenShouldFail(runID, nodeID, encoded string) bool {
	return runID == "" || nodeID == "" || len(encoded) > nodePageTokenMaxLen
}
