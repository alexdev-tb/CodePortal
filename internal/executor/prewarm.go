package executor

import (
	"context"
	"log"
	"strings"
	"sync"
	"time"
)

const (
	prewarmInterval = 30 * time.Second
	prewarmTimeout  = 5 * time.Second
)

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

func (r *DockerRunner) triggerPrewarm() {
	if !r.prewarmGate.CompareAndSwap(false, true) {
		return
	}

	go func() {
		defer r.prewarmGate.Store(false)
		log.Printf("%s[PREWARM]%s initiating prewarm scan", colorCyan, colorReset)
		r.prewarmAll()
	}()
}

func (r *DockerRunner) prewarmAll() {
	names := r.allContainerNames()
	if len(names) == 0 {
		log.Printf("%s[PREWARM]%s no containers available for prewarm", colorMagenta, colorReset)
		return
	}

	var wg sync.WaitGroup
	log.Printf("%s[PREWARM]%s evaluating %d container(s) for readiness", colorMagenta, colorReset, len(names))
	for _, name := range names {
		if r.isContainerDead(name) {
			continue
		}
		if !r.beginPrewarm(name) {
			continue
		}
		wg.Add(1)
		go func(containerName string) {
			defer wg.Done()
			r.prewarmContainer(containerName)
		}(name)
	}

	wg.Wait()
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
		log.Printf("%s[WARN]%s prewarm ensure failed for container %s: %v", colorYellow, colorReset, name, err)
		return
	}

	r.markContainerAlive(name)
	log.Printf("%s[PREWARM]%s container %s is warmed and ready", colorGreen, colorReset, name)
}
