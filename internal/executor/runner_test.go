package executor

import "testing"

func TestDockerRunnerPoolSnapshots(t *testing.T) {
	runner := NewDockerRunner(RunnerConfig{
		Container: "fallback-runner",
		LanguageContainers: map[string][]string{
			"go":     {"go-1", "go-2"},
			"python": {"py-1"},
		},
		JobDir: t.TempDir(),
	})

	snapshots := runner.PoolSnapshots()
	if len(snapshots) != 3 {
		t.Fatalf("expected 3 pools including fallback, got %d", len(snapshots))
	}

	var goFound, pythonFound, fallbackFound bool
	for _, snap := range snapshots {
		switch snap.Language {
		case "go":
			goFound = true
			if snap.Total != 2 {
				t.Fatalf("expected go pool total 2, got %d", snap.Total)
			}
		case "python":
			pythonFound = true
			if snap.Total != 1 {
				t.Fatalf("expected python pool total 1, got %d", snap.Total)
			}
		case "fallback":
			fallbackFound = snap.Fallback
		default:
			t.Fatalf("unexpected pool language %s", snap.Language)
		}
	}

	if !goFound || !pythonFound || !fallbackFound {
		t.Fatalf("expected go, python, and fallback pools, got go=%t python=%t fallback=%t", goFound, pythonFound, fallbackFound)
	}
}

func TestSelectFallbackContainer(t *testing.T) {
	name := selectFallbackContainer(map[string][]string{
		"python": {"", "py-1"},
		"go":     {"go-2", "go-1"},
	})

	if name != "go-2" {
		t.Fatalf("expected go-2 as fallback, got %s", name)
	}

	if selectFallbackContainer(nil) != "" {
		t.Fatalf("expected empty fallback for nil map")
	}
}

func TestNormalizeLanguageMapAliases(t *testing.T) {
	mapped := normalizeLanguageMap(map[string][]string{
		"JavaScript": {"node-1"},
		"nodejs":     {"node-2"},
		"GoLang":     {"go-1"},
	})

	nodeNames, ok := mapped["node"]
	if !ok {
		t.Fatalf("expected node language to be present")
	}
	if len(nodeNames) != 2 {
		t.Fatalf("expected 2 node containers, got %d", len(nodeNames))
	}

	if _, ok := mapped["golang"]; ok {
		t.Fatalf("expected canonical key only")
	}
}
