package executor

import (
	"context"
	"errors"
	"log"
	"strings"
)

type containerPool struct {
	available chan string
	names     []string
}

var errNoContainersConfigured = errors.New("no containers configured")

func newContainerPool(names []string) *containerPool {
	cleaned := dedupeNames(names)
	if len(cleaned) == 0 {
		return &containerPool{}
	}

	ch := make(chan string, len(cleaned))
	for _, name := range cleaned {
		if name == "" {
			continue
		}
		ch <- name
	}

	if len(ch) == 0 {
		return &containerPool{}
	}

	return &containerPool{available: ch, names: cleaned}
}

func (p *containerPool) acquire(ctx context.Context) (string, error) {
	if p == nil || p.available == nil {
		return "", errNoContainersConfigured
	}

	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case name := <-p.available:
		return name, nil
	}
}

func (p *containerPool) release(name string) bool {
	if p == nil || p.available == nil || name == "" {
		return false
	}

	select {
	case p.available <- name:
		return true
	default:
		log.Printf("executor: dropping container %s due to full pool", name)
		return false
	}
}

func (p *containerPool) capacity() int {
	if p == nil || p.available == nil {
		return 0
	}
	return cap(p.available)
}

func (p *containerPool) availableCount() int {
	if p == nil || p.available == nil {
		return 0
	}
	return len(p.available)
}

func (p *containerPool) snapshotNames() []string {
	if p == nil {
		return nil
	}
	return append([]string(nil), p.names...)
}

func dedupeNames(names []string) []string {
	seen := make(map[string]struct{})
	var result []string
	for _, name := range names {
		trimmed := strings.TrimSpace(name)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		result = append(result, trimmed)
	}
	return result
}
