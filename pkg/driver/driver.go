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
	AdoptExisting bool
	Name          string
	MountPath     string
	HostPrefix    string
	QuotaBytes    int64
	Owner         *config.Ownership
	Mode          *os.FileMode
	Properties    map[string]string
	Encryption    *config.EncryptionConfig
}

// DeleteOptions specifies the input parameters for volume destruction.
type DeleteOptions struct {
	Name                 string
	MountPath            string
	HostPrefix           string
	SnapshotBeforeDelete bool
}

// OwnerModeOptions specifies ownership/mode reconciliation for an existing volume.
type OwnerModeOptions struct {
	Name       string
	MountPath  string
	HostPrefix string
	Owner      *config.Ownership
	Mode       *os.FileMode
	// Recursive applies owner/mode changes to all files and directories under
	// the mountpoint (symlinks are skipped, never followed).
	Recursive bool
	DryRun    bool
}

// Reconciler is an optional interface for drivers that support reconciling
// live state (quota, ownership, properties) onto existing volumes. Drivers
// that do not implement it are skipped by the controller's reconcile pass.
// Each method returns a human-readable list of changes applied (or, in
// dry-run mode, planned but not applied).
type Reconciler interface {
	// ReconcileQuota sets the volume quota to quotaBytes, or removes it
	// (quota=none) when quotaBytes is nil. No-op when already matching.
	ReconcileQuota(ctx context.Context, name string, quotaBytes *int64, dryRun bool) ([]string, error)
	// ReconcileOwnerMode applies ownership and/or mode to the volume mountpoint.
	ReconcileOwnerMode(ctx context.Context, opts OwnerModeOptions) ([]string, error)
	// ReconcileProperties sets properties to the desired values and removes
	// (via inherit) locally-set properties no longer present. No-op when
	// everything already matches.
	ReconcileProperties(ctx context.Context, name string, props map[string]string, dryRun bool) ([]string, error)
}

// Driver is the pluggable filesystem interface implemented by ZFS, Btrfs, etc.
type Driver interface {
	Name() string
	Create(ctx context.Context, opts CreateOptions) (*VolumeInfo, error)
	Delete(ctx context.Context, opts DeleteOptions) error
	Exists(ctx context.Context, name string) (bool, error)
}
