package executor

import "time"

const (
	executionOverheadFloor    = 250 * time.Millisecond
	executionOverheadCeiling  = 2 * time.Second
	executionOverheadRatio    = 0.10
	executionOverheadMaxRatio = 0.50
)

// computeExecutionBudget grows the caller's timeout with a buffer tuned by observed
// docker exec latency so startup overhead does not prematurely cancel user code.
func computeExecutionBudget(base, observed time.Duration) (effective time.Duration, overhead time.Duration) {
	if base <= 0 {
		base = 3 * time.Second
	}

	baseline := time.Duration(float64(base) * executionOverheadRatio)
	if baseline < executionOverheadFloor {
		baseline = executionOverheadFloor
	}

	overhead = baseline
	if observed > overhead {
		overhead = observed
	}

	maxOverhead := time.Duration(float64(base) * executionOverheadMaxRatio)
	if maxOverhead <= 0 {
		maxOverhead = executionOverheadCeiling
	}

	if overhead > executionOverheadCeiling {
		overhead = executionOverheadCeiling
	}
	if maxOverhead > 0 && overhead > maxOverhead {
		overhead = maxOverhead
	}

	effective = base + overhead
	return effective, overhead
}
