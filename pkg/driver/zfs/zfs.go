package zfs

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/blesswinsamuel/k8s-subvol-provisioner/pkg/driver"
	"github.com/rs/zerolog/log"
)

// CommandExecutor abstracts command execution for testability.
type CommandExecutor interface {
	Run(ctx context.Context, stdin []byte, cmd string, args ...string) ([]byte, error)
}

type osExecutor struct{}

func (e *osExecutor) Run(ctx context.Context, stdin []byte, cmd string, args ...string) ([]byte, error) {
	c := exec.CommandContext(ctx, cmd, args...)
	if len(stdin) > 0 {
		c.Stdin = bytes.NewReader(stdin)
	}
	out, err := c.CombinedOutput()
	if err != nil {
		return out, fmt.Errorf("command %s %s failed (%w): %s", cmd, strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return out, nil
}

// Driver implements driver.Driver for ZFS filesystems.
type Driver struct {
	zfsPath  string
	executor CommandExecutor
}

// Option allows customizing Driver options.
type Option func(*Driver)

// WithExecutor sets a custom CommandExecutor.
func WithExecutor(e CommandExecutor) Option {
	return func(d *Driver) {
		d.executor = e
	}
}

// WithZFSPath sets a custom path to the zfs binary.
func WithZFSPath(p string) Option {
	return func(d *Driver) {
		d.zfsPath = p
	}
}

// New creates a new ZFS Driver.
func New(opts ...Option) *Driver {
	d := &Driver{
		zfsPath:  "zfs",
		executor: &osExecutor{},
	}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

func (d *Driver) Name() string {
	return "zfs"
}

func (d *Driver) Exists(ctx context.Context, name string) (bool, error) {
	_, err := d.executor.Run(ctx, nil, d.zfsPath, "list", "-H", "-o", "name", name)
	if err != nil {
		// If zfs list fails because dataset doesn't exist, it's not an error condition
		return false, nil
	}
	return true, nil
}

func (d *Driver) Create(ctx context.Context, opts driver.CreateOptions) (*driver.VolumeInfo, error) {
	if opts.Name == "" {
		return nil, fmt.Errorf("dataset name cannot be empty")
	}

	exists, err := d.Exists(ctx, opts.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to check if dataset %q exists: %w", opts.Name, err)
	}
	if exists {
		return nil, fmt.Errorf("dataset %q already exists", opts.Name)
	}

	args := []string{"create", "-p"}

	// Set quota if specified
	if opts.QuotaBytes > 0 {
		args = append(args, "-o", fmt.Sprintf("quota=%s", strconv.FormatInt(opts.QuotaBytes, 10)))
	}

	// Set user properties
	for k, v := range opts.Properties {
		args = append(args, "-o", fmt.Sprintf("%s=%s", k, v))
	}

	// Handle encryption
	var stdin []byte
	if opts.Encryption != nil && opts.Encryption.Enabled {
		args = append(args, "-o", "encryption=on")
		if opts.Encryption.KeyFormat != "" {
			args = append(args, "-o", fmt.Sprintf("keyformat=%s", opts.Encryption.KeyFormat))
		}
		if opts.Encryption.KeyLocation != "" {
			args = append(args, "-o", fmt.Sprintf("keylocation=%s", opts.Encryption.KeyLocation))
		} else if len(opts.Encryption.KeyData) > 0 {
			args = append(args, "-o", "keylocation=prompt")
			stdin = opts.Encryption.KeyData
		}
	}

	args = append(args, opts.Name)

	log.Info().Str("dataset", opts.Name).Strs("args", args).Msg("Creating ZFS dataset")
	if _, err := d.executor.Run(ctx, stdin, d.zfsPath, args...); err != nil {
		return nil, fmt.Errorf("failed to create zfs dataset %q: %w", opts.Name, err)
	}

	// Query mountpoint from zfs list
	mountPath := opts.MountPath
	if mountPath == "" {
		out, err := d.executor.Run(ctx, nil, d.zfsPath, "get", "-H", "-o", "value", "mountpoint", opts.Name)
		if err == nil {
			mountPath = strings.TrimSpace(string(out))
		}
	}

	// Apply ownership & mode if mount directory exists on host
	effectivePath := mountPath
	if opts.HostPrefix != "" && mountPath != "" {
		effectivePath = filepath.Join(opts.HostPrefix, mountPath)
	}
	if effectivePath != "" && mountPath != "none" && mountPath != "legacy" {
		if fi, err := os.Stat(effectivePath); err == nil && fi.IsDir() {
			if opts.Owner != nil {
				if err := os.Chown(effectivePath, int(opts.Owner.UID), int(opts.Owner.GID)); err != nil {
					log.Warn().Err(err).Str("path", effectivePath).Msg("Failed to chown dataset mountpoint")
				}
			}
			if opts.Mode != nil {
				if err := os.Chmod(effectivePath, *opts.Mode); err != nil {
					log.Warn().Err(err).Str("path", effectivePath).Msg("Failed to chmod dataset mountpoint")
				}
			}
		}
	}

	return &driver.VolumeInfo{
		VolumeID:  fmt.Sprintf("zfs:%s", opts.Name),
		Name:      opts.Name,
		MountPath: mountPath,
	}, nil
}

func (d *Driver) Delete(ctx context.Context, opts driver.DeleteOptions) error {
	if opts.Name == "" {
		return fmt.Errorf("dataset name cannot be empty")
	}

	exists, err := d.Exists(ctx, opts.Name)
	if err != nil {
		return fmt.Errorf("failed to check if dataset %q exists: %w", opts.Name, err)
	}
	if !exists {
		log.Warn().Str("dataset", opts.Name).Msg("ZFS dataset does not exist, skipping deletion")
		return nil
	}

	if opts.SnapshotBeforeDelete {
		snapName := fmt.Sprintf("%s@deleted-%d", opts.Name, time.Now().Unix())
		log.Info().Str("snapshot", snapName).Msg("Creating pre-destroy snapshot")
		if _, err := d.executor.Run(ctx, nil, d.zfsPath, "snapshot", snapName); err != nil {
			log.Warn().Err(err).Str("snapshot", snapName).Msg("Failed to create pre-destroy snapshot, proceeding with destroy")
		}
	}

	log.Info().Str("dataset", opts.Name).Msg("Destroying ZFS dataset")
	if _, err := d.executor.Run(ctx, nil, d.zfsPath, "destroy", "-r", opts.Name); err != nil {
		return fmt.Errorf("failed to destroy zfs dataset %q: %w", opts.Name, err)
	}

	return nil
}
