package btrfs

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
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

// Driver implements driver.Driver for Btrfs filesystems.
type Driver struct {
	btrfsPath string
	executor  CommandExecutor
}

// Option allows customizing Driver options.
type Option func(*Driver)

// WithExecutor sets a custom CommandExecutor.
func WithExecutor(e CommandExecutor) Option {
	return func(d *Driver) {
		d.executor = e
	}
}

// WithBtrfsPath sets a custom path to the btrfs binary.
func WithBtrfsPath(p string) Option {
	return func(d *Driver) {
		d.btrfsPath = p
	}
}

// New creates a new Btrfs Driver.
func New(opts ...Option) *Driver {
	d := &Driver{
		btrfsPath: "btrfs",
		executor:  &osExecutor{},
	}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

func (d *Driver) Name() string {
	return "btrfs"
}

func (d *Driver) Exists(ctx context.Context, path string) (bool, error) {
	_, err := d.executor.Run(ctx, nil, d.btrfsPath, "subvolume", "show", path)
	if err != nil {
		return false, nil
	}
	return true, nil
}

func (d *Driver) Create(ctx context.Context, opts driver.CreateOptions) (*driver.VolumeInfo, error) {
	targetPath := opts.MountPath
	if targetPath == "" {
		targetPath = opts.Name
	}
	if targetPath == "" {
		return nil, fmt.Errorf("btrfs subvolume target path cannot be empty")
	}

	exists, err := d.Exists(ctx, targetPath)
	if err != nil {
		return nil, fmt.Errorf("failed to check if btrfs subvolume %q exists: %w", targetPath, err)
	}
	if exists {
		return nil, fmt.Errorf("btrfs subvolume %q already exists", targetPath)
	}

	log.Info().Str("path", targetPath).Msg("Creating Btrfs subvolume")
	if _, err := d.executor.Run(ctx, nil, d.btrfsPath, "subvolume", "create", targetPath); err != nil {
		return nil, fmt.Errorf("failed to create btrfs subvolume %q: %w", targetPath, err)
	}

	// Apply quota if specified
	if opts.QuotaBytes > 0 {
		_ = d.enableQuota(ctx, targetPath)
		limitStr := strconv.FormatInt(opts.QuotaBytes, 10)
		log.Info().Str("path", targetPath).Str("limit", limitStr).Msg("Setting Btrfs qgroup limit")
		if _, err := d.executor.Run(ctx, nil, d.btrfsPath, "qgroup", "limit", limitStr, targetPath); err != nil {
			log.Warn().Err(err).Str("path", targetPath).Msg("Failed to set Btrfs qgroup limit")
		}
	}

	// Apply ownership & mode
	if fi, err := os.Stat(targetPath); err == nil && fi.IsDir() {
		if opts.Owner != nil {
			if err := os.Chown(targetPath, int(opts.Owner.UID), int(opts.Owner.GID)); err != nil {
				log.Warn().Err(err).Str("path", targetPath).Msg("Failed to chown btrfs subvolume")
			}
		}
		if opts.Mode != nil {
			if err := os.Chmod(targetPath, *opts.Mode); err != nil {
				log.Warn().Err(err).Str("path", targetPath).Msg("Failed to chmod btrfs subvolume")
			}
		}
	}

	return &driver.VolumeInfo{
		VolumeID:  fmt.Sprintf("btrfs:%s", opts.Name),
		Name:      opts.Name,
		MountPath: targetPath,
	}, nil
}

func (d *Driver) enableQuota(ctx context.Context, path string) error {
	_, err := d.executor.Run(ctx, nil, d.btrfsPath, "quota", "enable", path)
	return err
}

func (d *Driver) Delete(ctx context.Context, opts driver.DeleteOptions) error {
	targetPath := opts.MountPath
	if targetPath == "" {
		targetPath = opts.Name
	}
	if targetPath == "" {
		return fmt.Errorf("btrfs subvolume target path cannot be empty")
	}

	exists, err := d.Exists(ctx, targetPath)
	if err != nil {
		return fmt.Errorf("failed to check if btrfs subvolume %q exists: %w", targetPath, err)
	}
	if !exists {
		log.Warn().Str("path", targetPath).Msg("Btrfs subvolume does not exist, skipping deletion")
		return nil
	}

	if opts.SnapshotBeforeDelete {
		snapPath := fmt.Sprintf("%s-deleted-%d", strings.TrimSuffix(targetPath, "/"), time.Now().Unix())
		log.Info().Str("snapshot", snapPath).Msg("Creating pre-destroy Btrfs snapshot")
		if _, err := d.executor.Run(ctx, nil, d.btrfsPath, "subvolume", "snapshot", "-r", targetPath, snapPath); err != nil {
			log.Warn().Err(err).Str("snapshot", snapPath).Msg("Failed to create pre-destroy snapshot, proceeding with delete")
		}
	}

	log.Info().Str("path", targetPath).Msg("Deleting Btrfs subvolume")
	if _, err := d.executor.Run(ctx, nil, d.btrfsPath, "subvolume", "delete", targetPath); err != nil {
		return fmt.Errorf("failed to delete btrfs subvolume %q: %w", targetPath, err)
	}

	return nil
}
