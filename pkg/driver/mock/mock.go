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
	mu      sync.RWMutex
	volumes map[string]*driver.VolumeInfo
	options map[string]driver.CreateOptions
	deleted map[string]driver.DeleteOptions
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
