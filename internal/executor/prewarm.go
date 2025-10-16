package executor

import (
	"context"
	"log"
	"strings"
	"sync"
	"time"
)

const (
	prewarmInterval          = 30 * time.Second
	prewarmTimeout           = 5 * time.Second
	prewarmSuccessCooldown   = 2 * time.Minute
	prewarmFailureBackoff    = 5 * time.Second
	prewarmFailureBackoffCap = 5 * time.Minute
	prewarmMaxConcurrency    = 4
)

type prewarmStatus struct {
	nextAttempt time.Time
	failures    int
}

func (r *DockerRunner) startPrewarmLoop() {
	go func() {
		r.triggerPrewarm()
		ticker := time.NewTicker(prewarmInterval)
		defer ticker.Stop()
		for range ticker.C {
			r.triggerPrewarm()
		}
	}()
}

func (r *DockerRunner) prewarmConcurrency(total int) int {
	if total <= 0 {
		return 0
	}
	limit := prewarmMaxConcurrency
	if limit <= 0 {
		limit = 1
	}
	if total < limit {
		limit = total
	}
	if limit <= 0 {
		return 1
	}
	return limit
}

func (r *DockerRunner) shouldPrewarm(name string, now time.Time) bool {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return false
	}

	r.prewarmStateMu.Lock()
	defer r.prewarmStateMu.Unlock()

	status, ok := r.prewarmState[trimmed]
	if !ok {
		return true
	}
	if status.nextAttempt.IsZero() {
		return true
	}
	return !now.Before(status.nextAttempt)
}

func (r *DockerRunner) recordPrewarmSuccess(name string, now time.Time) time.Duration {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return 0
	}

	r.prewarmStateMu.Lock()
	defer r.prewarmStateMu.Unlock()

	status := r.prewarmState[trimmed]
	if status == nil {
		status = &prewarmStatus{}
		r.prewarmState[trimmed] = status
	}
	status.failures = 0
	if prewarmSuccessCooldown > 0 {
		status.nextAttempt = now.Add(prewarmSuccessCooldown)
		return prewarmSuccessCooldown
	}
	status.nextAttempt = now
	return 0
}

func (r *DockerRunner) recordPrewarmFailure(name string, now time.Time) time.Duration {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return 0
	}

	r.prewarmStateMu.Lock()
	defer r.prewarmStateMu.Unlock()

	status := r.prewarmState[trimmed]
	if status == nil {
		status = &prewarmStatus{}
		r.prewarmState[trimmed] = status
	}
	status.failures++
	backoff := prewarmFailureBackoff
	if status.failures > 1 {
		shift := status.failures - 1
		if shift > 6 {
			shift = 6
		}
		backoff = prewarmFailureBackoff << shift
	}
	if backoff > prewarmFailureBackoffCap {
		backoff = prewarmFailureBackoffCap
	}
	status.nextAttempt = now.Add(backoff)
	return backoff
}

func (r *DockerRunner) clearPrewarmState(name string) {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return
	}
	r.prewarmStateMu.Lock()
	delete(r.prewarmState, trimmed)
	r.prewarmStateMu.Unlock()
}

func (r *DockerRunner) triggerPrewarm() {
	if !r.prewarmGate.CompareAndSwap(false, true) {
		return
	}

	go func() {
		defer r.prewarmGate.Store(false)
		// log.Printf("%s[PREWARM]%s initiating prewarm scan", colorCyan, colorReset)
		r.prewarmAll()
	}()
}

func (r *DockerRunner) prewarmAll() {
	names := r.allContainerNames()
	if len(names) == 0 {
		log.Printf("%s[PREWARM]%s no containers available for prewarm", colorMagenta, colorReset)
		return
	}

	now := time.Now()
	limit := r.prewarmConcurrency(len(names))
	if limit <= 0 {
		return
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, limit)
	scheduled := 0
	skippedCooldown := 0

	for _, name := range names {
		if r.isContainerDead(name) {
			continue
		}
		if !r.shouldPrewarm(name, now) {
			skippedCooldown++
			continue
		}
		if !r.beginPrewarm(name) {
			continue
		}
		scheduled++
		sem <- struct{}{}
		wg.Add(1)
		go func(containerName string) {
			defer wg.Done()
			defer func() { <-sem }()
			r.prewarmContainer(containerName)
		}(name)
	}

	wg.Wait()

	if scheduled == 0 {
		log.Printf("%s[PREWARM]%s no eligible containers this cycle (skipped=%d)", colorMagenta, colorReset, skippedCooldown)
		return
	}

	log.Printf("%s[PREWARM]%s warming %d container(s) with concurrency=%d (skipped=%d)", colorMagenta, colorReset, scheduled, limit, skippedCooldown)
}

func (r *DockerRunner) beginPrewarm(name string) bool {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return false
	}

	r.prewarmMu.Lock()
	defer r.prewarmMu.Unlock()
	if _, exists := r.prewarming[trimmed]; exists {
		return false
	}
	r.prewarming[trimmed] = struct{}{}
	return true
}

func (r *DockerRunner) finishPrewarm(name string) {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return
	}
	r.prewarmMu.Lock()
	delete(r.prewarming, trimmed)
	r.prewarmMu.Unlock()
}

func (r *DockerRunner) isContainerPrewarming(name string) bool {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return false
	}
	r.prewarmMu.RLock()
	_, ok := r.prewarming[trimmed]
	r.prewarmMu.RUnlock()
	return ok
}

func (r *DockerRunner) prewarmContainer(name string) {
	defer r.finishPrewarm(name)

	ctx, cancel := context.WithTimeout(context.Background(), prewarmTimeout)
	defer cancel()

	if err := r.ensureContainer(ctx, name); err != nil {
		backoff := r.recordPrewarmFailure(name, time.Now())
		log.Printf("%s[WARN]%s prewarm ensure failed for container %s: %v (retry in %s)", colorYellow, colorReset, name, err, backoff)
		return
	}

	r.markContainerAlive(name)
	next := r.recordPrewarmSuccess(name, time.Now())
	if next > 0 {
		log.Printf("%s[PREWARM]%s container %s is warmed and ready (next check in %s)", colorGreen, colorReset, name, next)
		return
	}
	log.Printf("%s[PREWARM]%s container %s is warmed and ready", colorGreen, colorReset, name)
}
