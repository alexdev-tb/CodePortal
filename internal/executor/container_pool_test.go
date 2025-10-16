package executor

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestContainerPoolAcquireRelease(t *testing.T) {
	pool := newContainerPool([]string{"runner-1"})
	if pool.capacity() != 1 {
		t.Fatalf("expected capacity 1, got %d", pool.capacity())
	}

	first, err := pool.acquire(context.Background())
	if err != nil {
		t.Fatalf("unexpected error acquiring first container: %v", err)
	}
	if first != "runner-1" {
		t.Fatalf("expected runner-1, got %s", first)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err = pool.acquire(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}

	if ok := pool.release(first); !ok {
		t.Fatalf("expected successful release")
	}

	second, err := pool.acquire(context.Background())
	if err != nil {
		t.Fatalf("unexpected error acquiring after release: %v", err)
	}
	if second != "runner-1" {
		t.Fatalf("expected runner-1 after release, got %s", second)
	}
}

func TestContainerPoolEmpty(t *testing.T) {
	pool := newContainerPool(nil)
	if _, err := pool.acquire(context.Background()); !errors.Is(err, errNoContainersConfigured) {
		t.Fatalf("expected errNoContainersConfigured, got %v", err)
	}
}

func TestContainerPoolDeduplicatesNames(t *testing.T) {
	pool := newContainerPool([]string{"runner-1", "runner-1", " runner-2 "})
	if pool.capacity() != 2 {
		t.Fatalf("expected capacity 2 after dedupe, got %d", pool.capacity())
	}
	names := pool.snapshotNames()
	if len(names) != 2 {
		t.Fatalf("expected 2 names in snapshot, got %d", len(names))
	}
}
