package executor

import (
	"strings"
	"sync"
	"time"
)

type overheadTracker struct {
	mu    sync.Mutex
	ewma  time.Duration
	count int64
}

func newOverheadTracker() *overheadTracker {
	return &overheadTracker{}
}

func (t *overheadTracker) observe(value time.Duration) {
	if value <= 0 {
		return
	}

	const alpha = 0.25

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.count == 0 {
		t.ewma = value
	} else {
		current := float64(t.ewma)
		sample := float64(value)
		t.ewma = time.Duration((1-alpha)*current + alpha*sample)
	}
	t.count++
}

func (t *overheadTracker) estimate() time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.count == 0 {
		return 0
	}
	return t.ewma
}

func (r *DockerRunner) getOverheadTracker(language string) *overheadTracker {
	key := strings.TrimSpace(language)
	if key == "" {
		key = "unknown"
	}

	r.overheadMu.RLock()
	tracker := r.overheadStats[key]
	r.overheadMu.RUnlock()
	if tracker != nil {
		return tracker
	}

	r.overheadMu.Lock()
	defer r.overheadMu.Unlock()

	if tracker = r.overheadStats[key]; tracker == nil {
		tracker = newOverheadTracker()
		r.overheadStats[key] = tracker
	}
	return tracker
}

func (r *DockerRunner) estimateOverhead(language string) time.Duration {
	tracker := r.getOverheadTracker(language)
	if tracker == nil {
		return 0
	}
	return tracker.estimate()
}

func (r *DockerRunner) recordOverhead(language string, observed time.Duration) {
	tracker := r.getOverheadTracker(language)
	if tracker == nil {
		return
	}
	tracker.observe(observed)
}
