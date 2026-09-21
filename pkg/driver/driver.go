package driver

import (
	"context"
	"os"

	"github.com/blesswinsamuel/k8s-subvol-provisioner/pkg/apis/config"
)

// VolumeInfo contains details about a successfully provisioned volume.
type VolumeInfo struct {
	VolumeID   string            `json:"volumeID"`
	Name       string            `json:"name"`
	MountPath  string            `json:"mountPath"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

// CreateOptions specifies the input parameters for volume creation.
type CreateOptions struct {
	Name        string
	MountPath   string
	QuotaBytes  int64
	Owner       *config.Ownership
	Mode        *os.FileMode
	Properties  map[string]string
	Encryption  *config.EncryptionConfig
}

// DeleteOptions specifies the input parameters for volume destruction.
type DeleteOptions struct {
	Name                 string
	MountPath            string
	SnapshotBeforeDelete bool
}

// Driver is the pluggable filesystem interface implemented by ZFS, Btrfs, etc.
type Driver interface {
	Name() string
	Create(ctx context.Context, opts CreateOptions) (*VolumeInfo, error)
	Delete(ctx context.Context, opts DeleteOptions) error
	Exists(ctx context.Context, name string) (bool, error)
}
