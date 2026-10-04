// Package worker stages and removes one immutable training window. The cache
// volume must only be writable by the operator's workers; training mounts are
// read-only. Ownership markers guard accidents, not a hostile writer on the PVC.
package worker

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path"
	"regexp"
	"strings"
	"unicode/utf8"
)

// Config is the JSON payload passed in WINDOWFLOW_WORKER_CONFIG.
type Config struct {
	Action        string          `json:"action"`
	Backend       string          `json:"backend"`
	PlanUID       string          `json:"planUID"`
	Generation    string          `json:"generation"`
	RelativePath  string          `json:"relativePath"`
	Source        string          `json:"source"`
	ExpectedBytes int64           `json:"expectedBytes"`
	DataFlow      *DataFlowConfig `json:"dataFlow,omitempty"`
}

// DataFlowConfig describes the existing OSS-to-CPFS data flow. PVCPath is the
// actual CPFS path mounted at cacheRoot, not the path inside the worker pod.
type DataFlowConfig struct {
	Region         string `json:"region"`
	FileSystemID   string `json:"fileSystemId"`
	DataFlowID     string `json:"dataFlowId"`
	FileSystemPath string `json:"fileSystemPath"`
	PVCPath        string `json:"pvcPath"`
}

var componentPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$`)
var regionPattern = regexp.MustCompile(`^[a-z0-9]+(?:-[a-z0-9]+)+$`)

// Validate rejects path aliases as well as escapes: every generation has one
// canonical directory, so another path cannot accidentally name the same data.
func Validate(c Config) error {
	if c.Action != "stage" && c.Action != "clean" {
		return fmt.Errorf("action must be stage or clean")
	}
	if c.Backend != "local" && c.Backend != "cpfs-dataflow" {
		return fmt.Errorf("unsupported backend %q", c.Backend)
	}
	if !componentPattern.MatchString(c.PlanUID) || !componentPattern.MatchString(c.Generation) {
		return fmt.Errorf("planUID and generation must be safe nonempty path components")
	}
	if c.RelativePath != path.Join("windowflow", c.PlanUID, c.Generation) {
		return fmt.Errorf("relativePath must be windowflow/<planUID>/<generation>")
	}
	if err := relativeDirectory(c.Source); err != nil {
		return fmt.Errorf("source: %w", err)
	}
	if c.ExpectedBytes < 0 {
		return fmt.Errorf("expectedBytes must be nonnegative")
	}
	if c.Backend == "cpfs-dataflow" {
		if c.DataFlow == nil || !regionPattern.MatchString(c.DataFlow.Region) ||
			!strings.HasPrefix(c.DataFlow.FileSystemID, "bmcpfs-") ||
			!componentPattern.MatchString(c.DataFlow.FileSystemID) ||
			!strings.HasPrefix(c.DataFlow.DataFlowID, "df-") || !componentPattern.MatchString(c.DataFlow.DataFlowID) {
			return fmt.Errorf("cpfs-dataflow requires a region, bmcpfs fileSystemId and df dataFlowId")
		}
		if _, err := dataFlowDestination(c); err != nil {
			return err
		}
	}
	return nil
}

func relativeDirectory(p string) error {
	if p == "" || p == "." || path.IsAbs(p) || path.Clean(p) != p ||
		strings.ContainsAny(p, "\\\x00\r\n") || !utf8.ValidString(p) {
		return fmt.Errorf("must be a canonical relative directory")
	}
	for _, component := range strings.Split(p, "/") {
		if component == ".." || component == "." || strings.HasPrefix(component, ".windowflow-") {
			return fmt.Errorf("reserved or unsafe directory component")
		}
	}
	return nil
}

func absoluteDirectory(p string) (string, error) {
	if !path.IsAbs(p) || strings.ContainsAny(p, "\\\x00\r\n") || !utf8.ValidString(p) {
		return "", fmt.Errorf("must be an absolute POSIX directory")
	}
	trimmed := strings.TrimSuffix(p, "/")
	if trimmed == "" {
		return "/", nil
	}
	if path.Clean(trimmed) != trimmed {
		return "", fmt.Errorf("directory must not contain dot components or repeated separators")
	}
	return trimmed, nil
}

func dataFlowDestination(c Config) (string, error) {
	linked, err := absoluteDirectory(c.DataFlow.FileSystemPath)
	if err != nil {
		return "", fmt.Errorf("fileSystemPath: %w", err)
	}
	pvc, err := absoluteDirectory(c.DataFlow.PVCPath)
	if err != nil {
		return "", fmt.Errorf("pvcPath: %w", err)
	}
	if linked != "/" && pvc != linked && !strings.HasPrefix(pvc, linked+"/") {
		return "", fmt.Errorf("pvcPath must lie within dataFlow.fileSystemPath")
	}
	destination := path.Join(pvc, c.RelativePath)
	rel := strings.TrimPrefix(destination, linked)
	rel = "/" + strings.Trim(rel, "/") + "/"
	if len(rel) > 1023 || len(c.Source)+2 > 1023 {
		return "", fmt.Errorf("DataFlow directory exceeds API length limit")
	}
	return rel, nil
}

func configDigest(c Config) string {
	c.Action = ""
	b, _ := json.Marshal(c)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
