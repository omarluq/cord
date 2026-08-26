package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNegotiateEncoding(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		header     string
		encoding   string
		acceptable bool
	}{
		{name: "absent", header: "", encoding: identityEncoding, acceptable: true},
		{name: gzipEncoding, header: gzipEncoding, encoding: gzipEncoding, acceptable: true},
		{name: "case insensitive", header: "GZip", encoding: gzipEncoding, acceptable: true},
		{
			name: "gzip tied with identity", header: "gzip, identity",
			encoding: gzipEncoding, acceptable: true,
		},
		{name: "identity preferred", header: "gzip;q=0.5", encoding: identityEncoding, acceptable: true},
		{
			name: "gzip preferred", header: "gzip;q=0.8, identity;q=0.2",
			encoding: gzipEncoding, acceptable: true,
		},
		{name: "wildcard", header: "*", encoding: gzipEncoding, acceptable: true},
		{
			name: "explicit gzip exclusion beats wildcard", header: "gzip;q=0, *;q=1",
			encoding: identityEncoding, acceptable: true,
		},
		{name: "unknown encoding", header: "br", encoding: identityEncoding, acceptable: true},
		{name: "identity only", header: "gzip;q=0", encoding: identityEncoding, acceptable: true},
		{name: "none acceptable", header: "gzip;q=0, identity;q=0", encoding: "", acceptable: false},
		{name: "wildcard excludes identity", header: "*;q=0", encoding: "", acceptable: false},
		{name: "NaN quality", header: "gzip;q=NaN", encoding: identityEncoding, acceptable: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			encoding, acceptable := negotiateEncoding(test.header)
			require.Equal(t, test.encoding, encoding)
			require.Equal(t, test.acceptable, acceptable)
		})
	}
}
