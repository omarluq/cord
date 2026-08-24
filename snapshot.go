package cord

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

const (
	defaultNodePageSize = 50
	maxNodePageSize     = 200
	nodePageTokenMaxLen = 8192
	nodePageTokenV1     = byte(1)
)

// NodeStateCounts contains an explicit count for every node state.
type NodeStateCounts struct {
	// Pending is the number of nodes with unsatisfied dependencies.
	Pending int
	// Ready is the number of nodes eligible to be claimed.
	Ready int
	// Running is the number of currently leased nodes.
	Running int
	// RetryWait is the number of nodes waiting for a retry deadline.
	RetryWait int
	// Completed is the number of successfully completed nodes.
	Completed int
	// Failed is the number of terminally failed nodes.
	Failed int
	// Canceled is the number of terminally canceled nodes.
	Canceled int
}

// RunReport is an authoritative current snapshot of one durable run. It omits
// payloads and user error messages. The value may be stale immediately after it
// is returned and does not represent event or attempt history. Timestamps are
// database observations normalized to UTC; precision can vary by provider.
type RunReport struct {
	// SubmittedAt is when the run was durably created.
	SubmittedAt time.Time
	// FirstStartedAt is when any node was first claimed, when known.
	FirstStartedAt *time.Time
	// StateChangedAt is when the current run state was durably entered.
	StateChangedAt time.Time
	// FinishedAt is when the run entered a terminal state, when applicable.
	FinishedAt *time.Time
	// TerminalRunnerID identifies the runner that committed a claimed terminal
	// transition, when applicable. It is diagnostic metadata, not authority.
	TerminalRunnerID *RunnerID
	// ID identifies the durable run.
	ID RunID
	// WorkflowName is the durable workflow identity.
	WorkflowName string
	// State is the run's current durable state.
	State RunState
	// Reason is the stable terminal reason, or empty while nonterminal.
	Reason TerminalReason
	// NodeCounts contains counts for every node state in this observation.
	NodeCounts NodeStateCounts
}

// CurrentLease describes the durable fence currently held for a running node.
// RunnerID alone never authorizes a transition; generation and expiry remain
// part of the storage fencing contract.
type CurrentLease struct {
	// ExpiresAt is the database-time lease deadline, normalized to UTC.
	ExpiresAt time.Time
	// RunnerID identifies the runner holding the current lease.
	RunnerID RunnerID
	// Generation is the current lease fencing generation.
	Generation int64
}

// NodeReport is an authoritative current snapshot of one durable run node. It
// contains no input, output, or user error-message data and is not attempt
// history. A returned value may be stale immediately.
type NodeReport struct {
	// EligibleAt is the durable scheduling time. For ready nodes it is the
	// earliest claim time; for retrying nodes it is the retry deadline.
	EligibleAt time.Time
	// FirstStartedAt is the first successful claim time, when known.
	FirstStartedAt *time.Time
	// LastStartedAt is the latest successful claim time, when known.
	LastStartedAt *time.Time
	// StateChangedAt is when the current node state was entered, when known.
	StateChangedAt *time.Time
	// FinishedAt is when the node entered a terminal state, when applicable.
	FinishedAt *time.Time
	// RunnerID identifies the current or most recent successful claimant, when
	// known. It is diagnostic metadata, not an authorization credential.
	RunnerID *RunnerID
	// CurrentLease is present only while the node is running.
	CurrentLease *CurrentLease
	// RunID identifies the node's durable run.
	RunID RunID
	// NodeID is the stable logical node identifier within the run.
	NodeID NodeID
	// FunctionKey identifies the registered function used by the node.
	FunctionKey string
	// State is the node's current durable state.
	State NodeState
	// Reason is the stable terminal reason, or empty while nonterminal.
	Reason TerminalReason
	// Attempt is the number of successful claims made for this node.
	Attempt int
	// MaxAttempts is the persisted attempt limit for this node.
	MaxAttempts int
}

// NodeQuery selects a bounded page of nodes ordered by stable NodeID. State and
// Reason are optional exact filters. ContinuationToken is opaque and must be
// reused only with the same run and filters. PageSize zero uses a conservative
// default; values above the supported maximum are rejected.
type NodeQuery struct {
	// State optionally filters by an exact known node state.
	State *NodeState
	// Reason optionally filters by an exact known terminal reason.
	Reason *TerminalReason
	// ContinuationToken resumes a previous ListRunNodes call.
	ContinuationToken string
	// PageSize is the maximum number of nodes returned in this page.
	PageSize int
}

// NodePage is one bounded page of node snapshots. Pages are individually
// coherent durable observations, but multiple pages are not one historical
// snapshot and may observe intervening transitions.
type NodePage struct {
	// ContinuationToken is nonempty when another page may be requested.
	ContinuationToken string
	// Nodes contains immutable report values ordered by stable NodeID.
	Nodes []NodeReport
}

// RunnerID returns the opaque identity of this runtime incarnation. The value
// is stable for a successfully created Cord instance and differs for a newly
// created instance. It is diagnostic metadata, not a secret, principal,
// authorization credential, metric label, or lease fencing token. A nil Cord
// has an empty RunnerID.
func (c *Cord) RunnerID() RunnerID {
	if c == nil {
		return ""
	}

	return RunnerID(c.owner)
}

// InspectRun returns one read-only, payload-free snapshot of id without waiting
// for the run to finish or reconstructing its workflow. It does not promote
// retries, recover leases, or authorize access: applications must enforce their
// own tenancy and authorization policy before calling it. Missing and malformed
// or unsupported runs return errors matching ErrRunNotFound and
// ErrRunIncompatible, respectively.
func (c *Cord) InspectRun(ctx context.Context, runID RunID) (RunReport, error) {
	if ctx == nil {
		return RunReport{}, errors.New("cord: snapshot context is nil")
	}

	if runID == "" {
		return RunReport{}, errors.New("cord: run ID is empty")
	}

	if err := c.validateSnapshotRuntime(); err != nil {
		return RunReport{}, err
	}

	report, err := c.store.InspectRun(ctx, storage.RunID(runID))
	if err != nil {
		return RunReport{}, snapshotStorageError("inspect run", runID, err)
	}

	converted, err := publicRunReport(&report)
	if err != nil {
		return RunReport{}, err
	}

	if converted.ID != runID {
		return RunReport{}, incompatibleSnapshot("storage returned run %q while inspecting %q", converted.ID, runID)
	}

	return converted, nil
}

// ListRunNodes returns one bounded, payload-free page ordered by stable NodeID.
// Continuation tokens are portable across Cord instances and replicas, but are
// not credentials and do not replace application authorization. A token is
// bound to the run and normalized filters. Pages may observe state changes made
// between calls and therefore do not provide cross-page snapshot isolation.
func (c *Cord) ListRunNodes(ctx context.Context, runID RunID, query NodeQuery) (NodePage, error) {
	if ctx == nil {
		return NodePage{}, errors.New("cord: snapshot context is nil")
	}

	if runID == "" {
		return NodePage{}, errors.New("cord: run ID is empty")
	}

	normalized, cursor, err := normalizeNodeQuery(runID, query)
	if err != nil {
		return NodePage{}, err
	}

	if runtimeErr := c.validateSnapshotRuntime(); runtimeErr != nil {
		return NodePage{}, runtimeErr
	}

	page, err := c.store.ListRunNodes(ctx, storage.RunID(runID), storageNodeQuery(normalized, cursor))
	if err != nil {
		return NodePage{}, snapshotStorageError("list run nodes", runID, err)
	}

	return publicNodePage(runID, normalized, page)
}

func (c *Cord) validateSnapshotRuntime() error {
	if c == nil || c.store == nil {
		return errors.New("cord: invalid runtime")
	}

	c.admissionMu.Lock()
	open := c.acceptingRuns
	c.admissionMu.Unlock()

	if !open {
		return errors.New("cord: runtime closed")
	}

	return nil
}

func snapshotStorageError(operation string, runID RunID, err error) error {
	switch {
	case errors.Is(err, storage.ErrRunNotFound):
		return fmt.Errorf("cord: %s %q: %w", operation, runID, ErrRunNotFound)
	case errors.Is(err, storage.ErrRunIncompatible):
		return fmt.Errorf("cord: %s %q: %w", operation, runID, ErrRunIncompatible)
	default:
		return fmt.Errorf("cord: %s %q: %w", operation, runID, err)
	}
}

func incompatibleSnapshot(format string, values ...any) error {
	return fmt.Errorf("cord: incompatible run snapshot: %s: %w", fmt.Sprintf(format, values...), ErrRunIncompatible)
}

func publicRunReport(report *storage.RunReport) (RunReport, error) {
	converted := RunReport{
		SubmittedAt:      report.SubmittedAt.UTC(),
		FirstStartedAt:   publicTime(report.FirstStartedAt),
		StateChangedAt:   report.StateChangedAt.UTC(),
		FinishedAt:       publicTime(report.FinishedAt),
		TerminalRunnerID: publicRunnerID(report.TerminalRunnerID),
		ID:               RunID(report.ID),
		WorkflowName:     report.WorkflowName,
		State:            RunState(report.State),
		Reason:           TerminalReason(report.Reason),
		NodeCounts: NodeStateCounts{
			Pending: report.NodeCounts.Pending, Ready: report.NodeCounts.Ready,
			Running: report.NodeCounts.Running, RetryWait: report.NodeCounts.RetryWait,
			Completed: report.NodeCounts.Completed, Failed: report.NodeCounts.Failed,
			Canceled: report.NodeCounts.Canceled,
		},
	}

	if err := validateRunReport(&converted); err != nil {
		return RunReport{}, err
	}

	return converted, nil
}

func validateRunReport(report *RunReport) error {
	if err := validateRunIdentity(report); err != nil {
		return err
	}

	if !report.State.AllowsReason(report.Reason) {
		return incompatibleSnapshot(
			"run %q has state %q with reason %q", report.ID, report.State, report.Reason,
		)
	}

	terminal, _ := report.State.Terminal()
	if terminal != (report.FinishedAt != nil) {
		return incompatibleSnapshot("run %q has inconsistent terminal timestamps", report.ID)
	}

	return validateRunMetadata(report)
}

func validateRunIdentity(report *RunReport) error {
	if report.ID == "" || report.WorkflowName == "" {
		return incompatibleSnapshot("run identity is empty")
	}

	if report.SubmittedAt.IsZero() || report.StateChangedAt.IsZero() {
		return incompatibleSnapshot("run %q has a missing required timestamp", report.ID)
	}

	return nil
}

func validateRunMetadata(report *RunReport) error {
	if err := validateTerminalRunner(report); err != nil {
		return err
	}

	counts := report.NodeCounts
	if counts.Pending < 0 || counts.Ready < 0 || counts.Running < 0 || counts.RetryWait < 0 ||
		counts.Completed < 0 || counts.Failed < 0 || counts.Canceled < 0 {
		return incompatibleSnapshot("run %q has a negative node count", report.ID)
	}

	return nil
}

func validateTerminalRunner(report *RunReport) error {
	if report.TerminalRunnerID != nil && *report.TerminalRunnerID == "" {
		return incompatibleSnapshot("run %q has an empty terminal runner ID", report.ID)
	}

	if report.State == RunStateCanceled && report.TerminalRunnerID != nil {
		return incompatibleSnapshot("canceled run %q has a terminal runner", report.ID)
	}

	if report.StateChangedAt.Before(report.SubmittedAt) ||
		(report.FirstStartedAt != nil && report.FirstStartedAt.Before(report.SubmittedAt)) {
		return incompatibleSnapshot("run %q has inconsistent lifecycle timestamps", report.ID)
	}

	if report.FinishedAt != nil && !report.FinishedAt.Equal(report.StateChangedAt) {
		return incompatibleSnapshot("run %q has inconsistent terminal timestamps", report.ID)
	}

	return nil
}

func publicNodePage(runID RunID, query NodeQuery, page storage.NodePage) (NodePage, error) {
	nodes := make([]NodeReport, len(page.Nodes))
	for index := range page.Nodes {
		node, err := publicNodeReport(&page.Nodes[index])
		if err != nil {
			return NodePage{}, err
		}

		if node.RunID != runID {
			return NodePage{}, incompatibleSnapshot(
				"storage returned node for run %q while listing %q", node.RunID, runID,
			)
		}

		nodes[index] = node
	}

	var token string

	if page.ContinuationToken != "" {
		var err error

		token, err = encodeNodePageToken(nodePageToken{
			RunID: runID, State: nodeStateFilter(query), Reason: nodeReasonFilter(query),
			LastNodeID: NodeID(page.ContinuationToken),
		})
		if err != nil {
			return NodePage{}, incompatibleSnapshot("storage returned an invalid continuation cursor: %v", err)
		}
	}

	return NodePage{Nodes: nodes, ContinuationToken: token}, nil
}

func publicNodeReport(report *storage.NodeReport) (NodeReport, error) {
	converted := NodeReport{
		EligibleAt: report.EligibleAt.UTC(), FirstStartedAt: publicTime(report.FirstStartedAt),
		LastStartedAt: publicTime(report.LastStartedAt), StateChangedAt: publicTime(report.StateChangedAt),
		FinishedAt: publicTime(report.FinishedAt), RunnerID: publicRunnerID(report.RunnerID),
		CurrentLease: nil, RunID: RunID(report.RunID), NodeID: NodeID(report.NodeID),
		FunctionKey: report.FunctionKey, State: NodeState(report.State), Reason: TerminalReason(report.Reason),
		Attempt: report.Attempt, MaxAttempts: report.MaxAttempts,
	}
	if report.CurrentLease != nil {
		converted.CurrentLease = &CurrentLease{
			ExpiresAt: report.CurrentLease.ExpiresAt.UTC(), RunnerID: RunnerID(report.CurrentLease.RunnerID),
			Generation: report.CurrentLease.Generation,
		}
	}

	if err := validateNodeReport(&converted); err != nil {
		return NodeReport{}, err
	}

	return converted, nil
}

func validateNodeReport(report *NodeReport) error {
	if err := validateNodeIdentity(report); err != nil {
		return err
	}

	if !report.State.AllowsReason(report.Reason) {
		return incompatibleSnapshot("node %q has state %q with reason %q", report.NodeID, report.State, report.Reason)
	}

	terminal, _ := report.State.Terminal()
	if terminal != (report.FinishedAt != nil) {
		return incompatibleSnapshot("node %q has inconsistent terminal timestamps", report.NodeID)
	}

	return validateNodeLease(report)
}

func validateNodeIdentity(report *NodeReport) error {
	if report.RunID == "" || report.NodeID == "" || report.FunctionKey == "" {
		return incompatibleSnapshot("node identity is empty")
	}

	if report.EligibleAt.IsZero() || report.MaxAttempts <= 0 ||
		report.Attempt < 0 || report.Attempt > report.MaxAttempts {
		return incompatibleSnapshot("node %q has invalid scheduling metadata", report.NodeID)
	}

	return nil
}

func validateNodeLease(report *NodeReport) error {
	if err := validateNodeStartMetadata(report); err != nil {
		return err
	}

	if report.StateChangedAt != nil && report.FinishedAt != nil &&
		!report.FinishedAt.Equal(*report.StateChangedAt) {
		return incompatibleSnapshot("node %q has inconsistent terminal timestamps", report.NodeID)
	}

	if report.State != NodeStateRunning {
		if report.CurrentLease != nil {
			return incompatibleSnapshot("non-running node %q has current lease metadata", report.NodeID)
		}

		return nil
	}

	return validateRunningLease(report)
}

func validateNodeStartMetadata(report *NodeReport) error {
	if report.RunnerID != nil && (*report.RunnerID == "" || report.LastStartedAt == nil) {
		return incompatibleSnapshot("node %q has invalid runner metadata", report.NodeID)
	}

	if report.FirstStartedAt != nil && report.Attempt == 0 {
		return incompatibleSnapshot("unclaimed node %q has start metadata", report.NodeID)
	}

	if report.LastStartedAt != nil && (report.FirstStartedAt == nil ||
		report.LastStartedAt.Before(*report.FirstStartedAt)) {
		return incompatibleSnapshot("node %q has inconsistent start metadata", report.NodeID)
	}

	return nil
}

func validateRunningLease(report *NodeReport) error {
	if report.CurrentLease == nil || report.RunnerID == nil {
		return incompatibleSnapshot("running node %q has missing lease metadata", report.NodeID)
	}

	lease := report.CurrentLease
	if lease.RunnerID != *report.RunnerID || lease.RunnerID == "" ||
		lease.ExpiresAt.IsZero() || lease.Generation <= 0 {
		return incompatibleSnapshot("running node %q has invalid lease metadata", report.NodeID)
	}

	return nil
}

func publicTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}

	converted := value.UTC()

	return &converted
}

func publicRunnerID(value *storage.RunnerID) *RunnerID {
	if value == nil {
		return nil
	}

	converted := RunnerID(*value)

	return &converted
}

func normalizeNodeQuery(runID RunID, query NodeQuery) (NodeQuery, NodeID, error) {
	query, err := normalizeNodeFilters(query)
	if err != nil {
		return NodeQuery{}, "", err
	}

	if query.ContinuationToken == "" {
		return query, "", nil
	}

	token, err := decodeNodePageToken(query.ContinuationToken)
	if err != nil {
		return NodeQuery{}, "", fmt.Errorf("cord: invalid node continuation token: %w", err)
	}

	if token.RunID != runID || token.State != nodeStateFilter(query) || token.Reason != nodeReasonFilter(query) {
		return NodeQuery{}, "", errors.New("cord: node continuation token does not match run or filters")
	}

	query.ContinuationToken = ""

	return query, token.LastNodeID, nil
}

func normalizeNodeFilters(query NodeQuery) (NodeQuery, error) {
	if query.PageSize < 0 || query.PageSize > maxNodePageSize {
		return NodeQuery{}, fmt.Errorf("cord: node page size must be between 0 and %d", maxNodePageSize)
	}

	if query.PageSize == 0 {
		query.PageSize = defaultNodePageSize
	}

	if query.State != nil {
		state := *query.State
		if !state.IsKnown() {
			return NodeQuery{}, fmt.Errorf("cord: unknown node state %q", state)
		}

		query.State = &state
	}

	if query.Reason != nil {
		reason := *query.Reason
		if !reason.IsKnown() {
			return NodeQuery{}, fmt.Errorf("cord: unknown terminal reason %q", reason)
		}

		query.Reason = &reason
	}

	return query, nil
}

func storageNodeQuery(query NodeQuery, cursor NodeID) storage.NodeQuery {
	converted := storage.NodeQuery{
		State: nil, Reason: nil, ContinuationToken: string(cursor), PageSize: query.PageSize,
	}
	if query.State != nil {
		state := storage.NodeStatus(*query.State)
		converted.State = &state
	}

	if query.Reason != nil {
		reason := storage.TerminalReason(*query.Reason)
		converted.Reason = &reason
	}

	return converted
}

func nodeStateFilter(query NodeQuery) NodeState {
	if query.State == nil {
		return ""
	}

	return *query.State
}

func nodeReasonFilter(query NodeQuery) TerminalReason {
	if query.Reason == nil {
		return ""
	}

	return *query.Reason
}

type nodePageToken struct {
	RunID      RunID          `json:"run_id"`
	State      NodeState      `json:"state,omitempty"`
	Reason     TerminalReason `json:"reason,omitempty"`
	LastNodeID NodeID         `json:"last_node_id"`
}

type nodePageTokenWire struct {
	Payload  nodePageToken `json:"payload"`
	Checksum string        `json:"checksum"`
	Version  int           `json:"version"`
}

func encodeNodePageToken(token nodePageToken) (string, error) {
	payload, err := json.Marshal(token)
	if err != nil {
		return "", fmt.Errorf("encode token payload: %w", err)
	}

	checksum := sha256.Sum256(payload)
	wire := nodePageTokenWire{
		Payload: token, Checksum: base64.RawURLEncoding.EncodeToString(checksum[:]),
		Version: int(nodePageTokenV1),
	}

	encoded, err := json.Marshal(wire)
	if err != nil {
		return "", fmt.Errorf("encode token envelope: %w", err)
	}

	return base64.RawURLEncoding.EncodeToString(encoded), nil
}

func decodeNodePageToken(encoded string) (nodePageToken, error) {
	wire, err := decodeNodePageTokenWire(encoded)
	if err != nil {
		return nodePageToken{}, err
	}

	if wire.Version != int(nodePageTokenV1) {
		return nodePageToken{}, fmt.Errorf("token version %d is unsupported", wire.Version)
	}

	if wire.Payload.RunID == "" || wire.Payload.LastNodeID == "" {
		return nodePageToken{}, errors.New("token cursor identity is empty")
	}

	if err := validateNodePageTokenChecksum(&wire); err != nil {
		return nodePageToken{}, err
	}

	return wire.Payload, nil
}

func decodeNodePageTokenWire(encoded string) (nodePageTokenWire, error) {
	if encoded == "" || len(encoded) > nodePageTokenMaxLen {
		return nodePageTokenWire{}, errors.New("token length is invalid")
	}

	decoded, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return nodePageTokenWire{}, errors.New("token encoding is invalid")
	}

	decoder := json.NewDecoder(bytes.NewReader(decoded))
	decoder.DisallowUnknownFields()

	var wire nodePageTokenWire
	if err = decoder.Decode(&wire); err != nil {
		return nodePageTokenWire{}, errors.New("token payload is invalid")
	}

	if err = decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nodePageTokenWire{}, errors.New("token payload has trailing data")
	}

	return wire, nil
}

func validateNodePageTokenChecksum(wire *nodePageTokenWire) error {
	payload, err := json.Marshal(wire.Payload)
	if err != nil {
		return errors.New("token payload is invalid")
	}

	expectedChecksum := sha256.Sum256(payload)

	providedChecksum, err := base64.RawURLEncoding.DecodeString(wire.Checksum)
	if err != nil || len(providedChecksum) != sha256.Size ||
		subtle.ConstantTimeCompare(providedChecksum, expectedChecksum[:]) != 1 {
		return errors.New("token checksum is invalid")
	}

	return nil
}
