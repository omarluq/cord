// Package hashframe implements Cord's persistence-sensitive hash framing.
package hashframe

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
)

// SHA256 hashes parts framed as their decimal byte length, a colon, and content.
// This framing is a persisted compatibility contract and must not change.
func SHA256(parts ...string) string {
	var framed strings.Builder
	for _, part := range parts {
		framed.WriteString(strconv.Itoa(len(part)))
		framed.WriteByte(':')
		framed.WriteString(part)
	}

	digest := sha256.Sum256([]byte(framed.String()))

	return hex.EncodeToString(digest[:])
}
