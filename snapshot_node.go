package cord

import (
	"time"

	"github.com/omarluq/cord/internal/storage"
)

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
