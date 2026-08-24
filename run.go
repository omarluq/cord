package cord

// RunID identifies a durable workflow run. Cord generates RunIDs as UUIDv7
// strings; callers may persist and transfer them but cannot construct runs from
// caller-selected IDs.
type RunID string
