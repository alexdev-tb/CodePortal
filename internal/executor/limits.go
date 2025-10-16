package executor

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os/exec"
	"sort"
	"strings"
)

type ContainerLimits struct {
	CPUs            float64
	MemoryBytes     int64
	PidsLimit       int64
	ReadOnlyRoot    bool
	NoNewPrivileges bool
	SeccompProfile  string
	AppArmorProfile string
	CapDrop         []string
	NetworkMode     string
	Tmpfs           []string
	Ulimits         map[string]UlimitRange
}

type UlimitRange struct {
	Soft int64
	Hard int64
}

func FormatLimits(limits ContainerLimits) string {
	parts := make([]string, 0, 8)

	if limits.CPUs > 0 {
		parts = append(parts, fmt.Sprintf("cpu=%.2f", limits.CPUs))
	}
	if limits.MemoryBytes > 0 {
		parts = append(parts, fmt.Sprintf("mem=%s", formatBytes(limits.MemoryBytes)))
	}
	if limits.PidsLimit > 0 {
		parts = append(parts, fmt.Sprintf("pids=%d", limits.PidsLimit))
	}
	if limits.ReadOnlyRoot {
		parts = append(parts, "ro-root")
	}
	if limits.NoNewPrivileges {
		parts = append(parts, "no-new-privs")
	}
	if limits.SeccompProfile != "" {
		parts = append(parts, fmt.Sprintf("seccomp=%s", limits.SeccompProfile))
	}
	if limits.AppArmorProfile != "" {
		parts = append(parts, fmt.Sprintf("apparmor=%s", limits.AppArmorProfile))
	}
	if len(limits.CapDrop) > 0 {
		parts = append(parts, fmt.Sprintf("cap-drop=%s", strings.Join(limits.CapDrop, ",")))
	}
	if limits.NetworkMode != "" {
		parts = append(parts, fmt.Sprintf("net=%s", limits.NetworkMode))
	}
	if len(limits.Tmpfs) > 0 {
		parts = append(parts, fmt.Sprintf("tmpfs=%d", len(limits.Tmpfs)))
	}
	if len(limits.Ulimits) > 0 {
		ulimits := make([]string, 0, len(limits.Ulimits))
		for name, rng := range limits.Ulimits {
			ulimits = append(ulimits, fmt.Sprintf("%s=%d:%d", name, rng.Soft, rng.Hard))
		}
		sort.Strings(ulimits)
		parts = append(parts, fmt.Sprintf("ulimits=[%s]", strings.Join(ulimits, ",")))
	}

	if len(parts) == 0 {
		return "-"
	}
	return strings.Join(parts, " ")
}

func formatBytes(n int64) string {
	if n <= 0 {
		return "0B"
	}
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	value := float64(n)
	suffix := []string{"KB", "MB", "GB", "TB", "PB", "EB"}
	exp := 0
	for value >= unit && exp < len(suffix)-1 {
		value /= unit
		exp++
	}
	return fmt.Sprintf("%.1f%s", value, suffix[exp])
}

func (r *DockerRunner) captureContainerLimits(names []string) {
	var pending []string

	r.limitsMu.RLock()
	for _, raw := range names {
		name := strings.TrimSpace(raw)
		if name == "" {
			continue
		}
		if r.isContainerDead(name) {
			continue
		}
		if _, exists := r.limits[name]; !exists {
			pending = append(pending, name)
		}
	}
	r.limitsMu.RUnlock()

	for _, name := range pending {
		info, err := fetchContainerLimits(r.dockerBin, name)
		if err != nil {
			log.Printf("%s[WARN]%s failed to inspect container %s: %v", colorYellow, colorReset, name, err)
			continue
		}
		r.limitsMu.Lock()
		if _, exists := r.limits[name]; !exists {
			r.limits[name] = info
		}
		r.limitsMu.Unlock()
		r.markContainerAlive(name)
	}
}

func (r *DockerRunner) aggregateLimits(names []string) ContainerLimits {
	r.limitsMu.RLock()
	defer r.limitsMu.RUnlock()

	var agg ContainerLimits
	first := true
	for _, raw := range names {
		name := strings.TrimSpace(raw)
		if name == "" {
			continue
		}
		info, ok := r.limits[name]
		if !ok {
			continue
		}
		if first {
			agg = info
			agg.CapDrop = append([]string(nil), info.CapDrop...)
			agg.Tmpfs = append([]string(nil), info.Tmpfs...)
			agg.Ulimits = cloneUlimitMap(info.Ulimits)
			first = false
			continue
		}
		agg.CPUs = minFloat(agg.CPUs, info.CPUs)
		agg.MemoryBytes = minInt64Positive(agg.MemoryBytes, info.MemoryBytes)
		agg.PidsLimit = minInt64Positive(agg.PidsLimit, info.PidsLimit)
		agg.ReadOnlyRoot = agg.ReadOnlyRoot && info.ReadOnlyRoot
		agg.NoNewPrivileges = agg.NoNewPrivileges && info.NoNewPrivileges
		agg.SeccompProfile = selectCommonString(agg.SeccompProfile, info.SeccompProfile)
		agg.AppArmorProfile = selectCommonString(agg.AppArmorProfile, info.AppArmorProfile)
		agg.CapDrop = intersectStrings(agg.CapDrop, info.CapDrop)
		agg.NetworkMode = selectCommonString(agg.NetworkMode, info.NetworkMode)
		agg.Tmpfs = intersectStrings(agg.Tmpfs, info.Tmpfs)
		agg.Ulimits = mergeUlimits(agg.Ulimits, info.Ulimits)
	}
	return agg
}

type rawContainerInspect struct {
	HostConfig struct {
		NanoCPUs        int64             `json:"NanoCpus"`
		CPUQuota        int64             `json:"CpuQuota"`
		CPUPeriod       int64             `json:"CpuPeriod"`
		Memory          int64             `json:"Memory"`
		PidsLimit       int64             `json:"PidsLimit"`
		SecurityOpt     []string          `json:"SecurityOpt"`
		CapDrop         []string          `json:"CapDrop"`
		ReadonlyRootfs  bool              `json:"ReadonlyRootfs"`
		NoNewPrivileges bool              `json:"NoNewPrivileges"`
		NetworkMode     string            `json:"NetworkMode"`
		Tmpfs           map[string]string `json:"Tmpfs"`
		Ulimits         []struct {
			Name string `json:"Name"`
			Soft int64  `json:"Soft"`
			Hard int64  `json:"Hard"`
		} `json:"Ulimits"`
	} `json:"HostConfig"`
	AppArmorProfile string `json:"AppArmorProfile"`
}

func fetchContainerLimits(dockerBin, name string) (ContainerLimits, error) {
	cmd := exec.Command(dockerBin, "inspect", name)
	output, err := cmd.Output()
	if err != nil {
		return ContainerLimits{}, fmt.Errorf("docker inspect %s: %w", name, err)
	}

	var decoded []rawContainerInspect
	if err := json.Unmarshal(output, &decoded); err != nil {
		return ContainerLimits{}, fmt.Errorf("parse inspect for %s: %w", name, err)
	}
	if len(decoded) == 0 {
		return ContainerLimits{}, fmt.Errorf("no inspect data for %s", name)
	}
	info := decoded[0]

	return ContainerLimits{
		CPUs:            calculateCPUs(info.HostConfig.NanoCPUs, info.HostConfig.CPUQuota, info.HostConfig.CPUPeriod),
		MemoryBytes:     info.HostConfig.Memory,
		PidsLimit:       info.HostConfig.PidsLimit,
		ReadOnlyRoot:    info.HostConfig.ReadonlyRootfs,
		NoNewPrivileges: info.HostConfig.NoNewPrivileges || hasSecurityOpt(info.HostConfig.SecurityOpt, "no-new-privileges:true"),
		SeccompProfile:  extractSecurityOpt(info.HostConfig.SecurityOpt, "seccomp"),
		AppArmorProfile: info.AppArmorProfile,
		CapDrop:         append([]string(nil), info.HostConfig.CapDrop...),
		NetworkMode:     info.HostConfig.NetworkMode,
		Tmpfs:           mapTmpfs(info.HostConfig.Tmpfs),
		Ulimits:         mapUlimits(info.HostConfig.Ulimits),
	}, nil
}

func calculateCPUs(nanoCPUs, quota, period int64) float64 {
	if nanoCPUs > 0 {
		return float64(nanoCPUs) / 1_000_000_000
	}
	if quota > 0 && period > 0 {
		return float64(quota) / float64(period)
	}
	return 0
}

func hasSecurityOpt(opts []string, target string) bool {
	for _, opt := range opts {
		if opt == target {
			return true
		}
	}
	return false
}

func extractSecurityOpt(opts []string, prefix string) string {
	needle := prefix + "="
	for _, opt := range opts {
		if strings.HasPrefix(opt, needle) {
			return strings.TrimPrefix(opt, needle)
		}
	}
	return ""
}

func mapTmpfs(tmpfs map[string]string) []string {
	if len(tmpfs) == 0 {
		return nil
	}
	var paths []string
	for path, opts := range tmpfs {
		if opts != "" {
			paths = append(paths, fmt.Sprintf("%s (%s)", path, opts))
			continue
		}
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return paths
}

func mapUlimits(src []struct {
	Name string `json:"Name"`
	Soft int64  `json:"Soft"`
	Hard int64  `json:"Hard"`
}) map[string]UlimitRange {
	if len(src) == 0 {
		return nil
	}
	result := make(map[string]UlimitRange, len(src))
	for _, item := range src {
		result[strings.ToLower(item.Name)] = UlimitRange{Soft: item.Soft, Hard: item.Hard}
	}
	return result
}

func minFloat(a, b float64) float64 {
	if a == 0 {
		return b
	}
	if b == 0 {
		return a
	}
	return math.Min(a, b)
}

func minInt64Positive(a, b int64) int64 {
	switch {
	case a <= 0:
		return b
	case b <= 0:
		return a
	case a < b:
		return a
	default:
		return b
	}
}

func selectCommonString(current, next string) string {
	if current == "" {
		return next
	}
	if next == "" {
		return current
	}
	if current == next {
		return current
	}
	return "mixed"
}

func intersectStrings(a, b []string) []string {
	if len(a) == 0 {
		return append([]string(nil), b...)
	}
	if len(b) == 0 {
		return append([]string(nil), a...)
	}
	set := make(map[string]struct{}, len(b))
	for _, item := range b {
		set[item] = struct{}{}
	}
	var result []string
	for _, item := range a {
		if _, ok := set[item]; ok {
			result = append(result, item)
		}
	}
	return result
}

func mergeUlimits(a, b map[string]UlimitRange) map[string]UlimitRange {
	if len(a) == 0 {
		if len(b) == 0 {
			return nil
		}
		result := make(map[string]UlimitRange, len(b))
		for k, v := range b {
			result[k] = v
		}
		return result
	}
	if len(b) == 0 {
		return a
	}
	for name, limits := range b {
		if existing, ok := a[name]; ok {
			a[name] = UlimitRange{
				Soft: minInt64Positive(existing.Soft, limits.Soft),
				Hard: minInt64Positive(existing.Hard, limits.Hard),
			}
			continue
		}
		a[name] = limits
	}
	return a
}

func cloneUlimitMap(in map[string]UlimitRange) map[string]UlimitRange {
	if len(in) == 0 {
		return nil
	}
	cloned := make(map[string]UlimitRange, len(in))
	for k, v := range in {
		cloned[k] = v
	}
	return cloned
}
