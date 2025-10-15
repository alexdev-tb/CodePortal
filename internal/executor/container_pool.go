package executor

import (
	"context"
	"errors"
	"log"
	"strings"
	"sync"
	"sync/atomic"
)

const poolChannelSlack = 16

type containerPool struct {
	mu        sync.RWMutex
	available chan string
	names     []string
	set       map[string]struct{}
	disabled  map[string]struct{}
	slots     atomic.Int32
}

var errNoContainersConfigured = errors.New("no containers configured")

func newContainerPool(names []string) *containerPool {
	cleaned := dedupeNames(names)
	if len(cleaned) == 0 {
		return &containerPool{}
	}

	capacity := len(cleaned) + poolChannelSlack
	if capacity < len(cleaned) {
		capacity = len(cleaned)
	}
	if capacity <= 0 {
		capacity = poolChannelSlack
	}

	ch := make(chan string, capacity)
	set := make(map[string]struct{}, len(cleaned))
	for _, name := range cleaned {
		if name == "" {
			continue
		}
		set[name] = struct{}{}
		ch <- name
	}

	if len(ch) == 0 {
		return &containerPool{}
	}

	pool := &containerPool{
		available: ch,
		names:     append([]string(nil), cleaned...),
		set:       set,
		disabled:  make(map[string]struct{}),
	}
	pool.slots.Store(int32(len(ch)))
	return pool
}

func (p *containerPool) acquire(ctx context.Context) (string, error) {
	if p == nil || p.available == nil {
		return "", errNoContainersConfigured
	}

	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case name := <-p.available:
			trimmed := strings.TrimSpace(name)
			if trimmed == "" {
				continue
			}
			if p.isDisabled(trimmed) {
				continue
			}
			p.slots.Add(-1)
			return trimmed, nil
		}
	}
}

func (p *containerPool) release(name string) bool {
	if p == nil || p.available == nil || name == "" {
		return false
	}

	trimmed := strings.TrimSpace(name)
	if trimmed == "" || p.isDisabled(trimmed) {
		return false
	}

	select {
	case p.available <- trimmed:
		p.slots.Add(1)
		return true
	default:
		log.Printf("executor: dropping container %s due to full pool", trimmed)
		return false
	}
}

func (p *containerPool) capacity() int {
	if p == nil || p.available == nil {
		return 0
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	active := 0
	for _, name := range p.names {
		if _, disabled := p.disabled[name]; disabled {
			continue
		}
		active++
	}
	return active
}

func (p *containerPool) availableCount() int {
	if p == nil || p.available == nil {
		return 0
	}
	return int(p.slots.Load())
}

func (p *containerPool) snapshotNames() []string {
	if p == nil {
		return nil
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	var result []string
	for _, name := range p.names {
		if _, disabled := p.disabled[name]; disabled {
			continue
		}
		result = append(result, name)
	}
	return result
}

func (p *containerPool) add(names ...string) int {
	if p == nil || p.available == nil {
		return 0
	}
	added := 0
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, raw := range names {
		name := strings.TrimSpace(raw)
		if name == "" {
			continue
		}
		if _, exists := p.set[name]; exists {
			if _, disabled := p.disabled[name]; disabled {
				delete(p.disabled, name)
				select {
				case p.available <- name:
					p.slots.Add(1)
				default:
				}
			}
			continue
		}
		p.set[name] = struct{}{}
		p.names = append(p.names, name)
		select {
		case p.available <- name:
			p.slots.Add(1)
			added++
		default:
			// channel unexpectedly full; drop entry but log once
			log.Printf("executor: pool channel full while adding %s; skipping", name)
		}
	}
	return added
}

func (p *containerPool) disable(name string) bool {
	if p == nil {
		return false
	}
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, exists := p.set[trimmed]; !exists {
		return false
	}
	if _, already := p.disabled[trimmed]; already {
		return false
	}
	p.disabled[trimmed] = struct{}{}
	return true
}

func (p *containerPool) isDisabled(name string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	_, disabled := p.disabled[name]
	return disabled
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
