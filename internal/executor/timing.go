package executor

import "time"

const (
	executionOverheadFloor   = 250 * time.Millisecond
	executionOverheadCeiling = 2 * time.Second
	executionOverheadRatio   = 0.10
)

// computeExecutionBudget grows the caller's timeout with a small buffer so docker
// exec startup latency does not prematurely cancel user code.
func computeExecutionBudget(base time.Duration) (effective time.Duration, overhead time.Duration) {
	if base <= 0 {
		base = 3 * time.Second
	}

	overhead = time.Duration(float64(base) * executionOverheadRatio)
	if overhead < executionOverheadFloor {
		overhead = executionOverheadFloor
	}
	if overhead > executionOverheadCeiling {
		overhead = executionOverheadCeiling
	}

	effective = base + overhead
	return effective, overhead
}
