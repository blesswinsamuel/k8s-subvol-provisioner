package mock

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/blesswinsamuel/k8s-subvol-provisioner/pkg/driver"
)

// Driver is a mock implementation of driver.Driver for testing.
type Driver struct {
	mu         sync.RWMutex
	volumes    map[string]*driver.VolumeInfo
	options    map[string]driver.CreateOptions
	deleted    map[string]driver.DeleteOptions
	reconciles []ReconcileCall
	upToDate   bool
}

// ReconcileCall records a single reconcile invocation.
type ReconcileCall struct {
	Name   string
	Quota  *int64
	Props  map[string]string
	Owner  *driver.OwnerModeOptions
	DryRun bool
}

// New creates a new in-memory mock driver.
func New() *Driver {
	return &Driver{
		volumes: make(map[string]*driver.VolumeInfo),
		options: make(map[string]driver.CreateOptions),
		deleted: make(map[string]driver.DeleteOptions),
	}
}

func (m *Driver) Name() string {
	return "mock"
}

func (m *Driver) Create(ctx context.Context, opts driver.CreateOptions) (*driver.VolumeInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if opts.Name == "" {
		return nil, fmt.Errorf("volume name cannot be empty")
	}
	if info, exists := m.volumes[opts.Name]; exists {
		if !opts.AdoptExisting {
			return nil, fmt.Errorf("volume %q already exists", opts.Name)
		}
		return info, nil
	}

	info := &driver.VolumeInfo{
		VolumeID:   fmt.Sprintf("mock:%s", opts.Name),
		Name:       opts.Name,
		MountPath:  opts.MountPath,
		Attributes: make(map[string]string),
	}
	for k, v := range opts.Properties {
		info.Attributes[k] = v
	}

	m.volumes[opts.Name] = info
	m.options[opts.Name] = opts

	// If a mount path is specified and starts with a temp/accessible dir, we can optionally mkdir
	if opts.MountPath != "" {
		if err := os.MkdirAll(opts.MountPath, 0755); err == nil && opts.Mode != nil {
			_ = os.Chmod(opts.MountPath, *opts.Mode)
		}
	}

	return info, nil
}

func (m *Driver) Delete(ctx context.Context, opts driver.DeleteOptions) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.volumes[opts.Name]; !exists {
		return fmt.Errorf("volume %q does not exist", opts.Name)
	}

	delete(m.volumes, opts.Name)
	m.deleted[opts.Name] = opts

	if opts.MountPath != "" {
		_ = os.RemoveAll(opts.MountPath)
	}

	return nil
}

func (m *Driver) Exists(ctx context.Context, name string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, exists := m.volumes[name]
	return exists, nil
}

func (m *Driver) GetVolume(name string) *driver.VolumeInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.volumes[name]
}

func (m *Driver) GetCreateOptions(name string) (driver.CreateOptions, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	opt, ok := m.options[name]
	return opt, ok
}

// SetUpToDate makes the reconcile methods report no pending changes, modeling
// a dataset that already matches the desired configuration.
func (m *Driver) SetUpToDate(upToDate bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.upToDate = upToDate
}

func (m *Driver) ReconcileQuota(ctx context.Context, name string, quotaBytes *int64, dryRun bool) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reconciles = append(m.reconciles, ReconcileCall{Name: name, Quota: quotaBytes, DryRun: dryRun})
	if m.upToDate {
		return nil, nil
	}
	if quotaBytes == nil {
		return []string{"quota=none"}, nil
	}
	return []string{fmt.Sprintf("quota=%d", *quotaBytes)}, nil
}

func (m *Driver) ReconcileOwnerMode(ctx context.Context, opts driver.OwnerModeOptions) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reconciles = append(m.reconciles, ReconcileCall{Name: opts.Name, Owner: &opts, DryRun: opts.DryRun})
	if m.upToDate {
		return nil, nil
	}
	if opts.Owner == nil && opts.Mode == nil {
		return nil, nil
	}
	return []string{"owner/mode"}, nil
}

func (m *Driver) ReconcileProperties(ctx context.Context, name string, props map[string]string, dryRun bool) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reconciles = append(m.reconciles, ReconcileCall{Name: name, Props: props, DryRun: dryRun})
	if m.upToDate {
		return nil, nil
	}
	if len(props) == 0 {
		return nil, nil
	}
	return []string{fmt.Sprintf("%d properties", len(props))}, nil
}

// GetReconcileCalls returns all recorded reconcile invocations.
func (m *Driver) GetReconcileCalls() []ReconcileCall {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]ReconcileCall, len(m.reconciles))
	copy(out, m.reconciles)
	return out
}
