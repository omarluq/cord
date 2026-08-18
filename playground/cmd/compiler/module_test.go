package main

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestModuleSourceQuotesCordDirectory(t *testing.T) {
	t.Parallel()

	const cordDirectory = "/tmp/cord workspace"

	require.Contains(
		t,
		moduleSource(cordDirectory),
		"replace "+cordModule+" => "+strconv.Quote(cordDirectory),
	)
}
