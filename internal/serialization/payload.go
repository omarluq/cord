package serialization

const (
	// PayloadWarningThreshold is the encoded size above which Cord reports a large payload.
	PayloadWarningThreshold = 256 * 1024
)

// PayloadDiagnostic describes an unusually large encoded payload without exposing its contents.
type PayloadDiagnostic struct {
	Size      int
	Threshold int
}

// DiagnosePayload reports whether payload exceeds the recommended warning threshold.
func DiagnosePayload(payload []byte) (PayloadDiagnostic, bool) {
	diagnostic := PayloadDiagnostic{
		Size:      len(payload),
		Threshold: PayloadWarningThreshold,
	}

	return diagnostic, diagnostic.Size > diagnostic.Threshold
}
