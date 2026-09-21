package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// TemplateContext holds values available to pathTemplate.
type TemplateContext struct {
	Namespace    string
	PVC          string
	ClaimName    string
	Owner        string
	Index        string
	StorageClass string
	Labels       map[string]string
	Annotations  map[string]string
}

// Ownership specifies Linux UID and GID.
type Ownership struct {
	UID int64
	GID int64
}

// EncryptionConfig represents encryption options for dataset creation.
type EncryptionConfig struct {
	Enabled     bool
	KeyFormat   string // e.g. "hex", "passphrase", "raw"
	KeyLocation string // e.g. "http://...", "file://...", "prompt"
	KeyData     []byte // If loaded from secret
}

// StorageClassConfig holds validated and parsed parameters from a StorageClass.
type StorageClassConfig struct {
	Driver       string
	Node         string
	Parent       string
	MountPrefix  string
	PathTemplate string
	DefaultOwner *Ownership
	DefaultMode  *os.FileMode
	Properties   map[string]string
	Encryption   *EncryptionConfig
}

// ClaimConfig holds validated parameters resolved for a specific PVC.
type ClaimConfig struct {
	RelativePath         string
	DatasetName          string
	MountPath            string
	QuotaBytes           int64
	Owner                *Ownership
	Mode                 *os.FileMode
	Properties           map[string]string
	Encryption           *EncryptionConfig
	SnapshotBeforeDelete bool
}

// ParseOwnership parses a "UID:GID" string into an Ownership struct.
func ParseOwnership(s string) (*Ownership, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}
	parts := strings.Split(s, ":")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid ownership format %q: expected UID:GID", s)
	}
	uid, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid UID %q: %w", parts[0], err)
	}
	gid, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid GID %q: %w", parts[1], err)
	}
	return &Ownership{UID: uid, GID: gid}, nil
}

// ParseFileMode parses an octal string like "0750" or "750" into an os.FileMode.
func ParseFileMode(s string) (*os.FileMode, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}
	modeUint, err := strconv.ParseUint(s, 8, 32)
	if err != nil {
		return nil, fmt.Errorf("invalid file mode %q: %w", s, err)
	}
	mode := os.FileMode(modeUint)
	return &mode, nil
}

// ParseKeyValueLines parses lines of "key=value" or "key: value" into a map.
func ParseKeyValueLines(content string) map[string]string {
	result := make(map[string]string)
	lines := strings.Split(content, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		var k, v string
		if strings.Contains(line, "=") {
			parts := strings.SplitN(line, "=", 2)
			k, v = strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
		} else if strings.Contains(line, ":") {
			parts := strings.SplitN(line, ":", 2)
			k, v = strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
		}
		if k != "" {
			result[k] = v
		}
	}
	return result
}
