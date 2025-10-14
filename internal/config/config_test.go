package config

import "testing"

func TestParseLanguageContainers(t *testing.T) {
	cases := []struct {
		name      string
		input     string
		expectLen int
		expectErr bool
	}{
		{name: "empty", input: "", expectLen: 0},
		{name: "single language", input: "go:alpha|beta|gamma", expectLen: 1},
		{name: "multiple languages", input: "go:alpha|beta,python:py-1|py-2", expectLen: 2},
		{name: "invalid entry", input: "golang", expectErr: true},
		{name: "missing containers", input: "python:", expectErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := parseLanguageContainers(tc.input)
			if tc.expectErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(result) != tc.expectLen {
				t.Fatalf("expected %d languages, got %d", tc.expectLen, len(result))
			}
		})
	}
}
