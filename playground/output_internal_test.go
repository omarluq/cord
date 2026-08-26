package playground

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAppendOutput(t *testing.T) {
	t.Parallel()

	const firstLine = "first"

	tests := []struct {
		name    string
		current string
		next    string
		want    string
	}{
		{name: "first line", current: "", next: firstLine, want: firstLine},
		{name: "replace running placeholder", current: "Running workflow…", next: firstLine, want: firstLine},
		{name: "append line", current: firstLine, next: "second", want: "first\nsecond"},
		{name: "avoid blank line", current: "first\n", next: "second", want: "first\nsecond"},
		{name: "ignore empty line", current: firstLine, next: "", want: firstLine},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, test.want, appendOutput(test.current, test.next))
		})
	}
}
