package playground

import "strings"

const (
	compilingMessage = "Compiling workflow…"
	runningMessage   = "Running workflow…"
)

func appendOutput(current, next string) string {
	if next == "" {
		return current
	}
	if current == "" ||
		current == runningMessage ||
		current == compilingMessage {
		return next
	}
	return strings.TrimRight(current, "\n") + "\n" + next
}
