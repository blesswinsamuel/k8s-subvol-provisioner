package dir

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/blesswinsamuel/k8s-subvol-provisioner/pkg/driver"
	"github.com/rs/zerolog/log"
)

// Driver implements driver.Driver for standard host directories.
type Driver struct{}

// New creates a new Driver instance for directory provisioning.
func New() *Driver {
	return &Driver{}
}

func (d *Driver) Name() string {
	return "dir"
}

func (d *Driver) Exists(ctx context.Context, path string) (bool, error) {
	if path == "" {
		return false, fmt.Errorf("path cannot be empty")
	}
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func (d *Driver) Create(ctx context.Context, opts driver.CreateOptions) (*driver.VolumeInfo, error) {
	targetPath := opts.MountPath
	if targetPath == "" {
		targetPath = opts.Name
	}
	if targetPath == "" {
		return nil, fmt.Errorf("directory target path cannot be empty")
	}

	effectivePath := targetPath
	if opts.HostPrefix != "" && targetPath != "" {
		effectivePath = filepath.Join(opts.HostPrefix, targetPath)
	}

	exists, err := d.Exists(ctx, effectivePath)
	if err != nil {
		return nil, fmt.Errorf("failed to check if directory %q exists: %w", effectivePath, err)
	}
	if exists {
		if !opts.AdoptExisting {
			return nil, fmt.Errorf("directory %q already exists", targetPath)
		}
		log.Info().Str("path", targetPath).Msg("Adopting existing directory")
		return &driver.VolumeInfo{
			VolumeID:  fmt.Sprintf("dir:%s", opts.Name),
			Name:      opts.Name,
			MountPath: targetPath,
		}, nil
	}

	perm := os.FileMode(0750)
	if opts.Mode != nil {
		perm = *opts.Mode
	}

	log.Info().Str("path", targetPath).Msg("Creating directory")
	if err := os.MkdirAll(effectivePath, perm); err != nil {
		return nil, fmt.Errorf("failed to create directory %q: %w", effectivePath, err)
	}

	// Apply ownership & mode
	if opts.Owner != nil {
		if err := os.Chown(effectivePath, int(opts.Owner.UID), int(opts.Owner.GID)); err != nil {
			log.Warn().Err(err).Str("path", effectivePath).Msg("Failed to chown directory")
		}
	}
	if opts.Mode != nil {
		if err := os.Chmod(effectivePath, *opts.Mode); err != nil {
			log.Warn().Err(err).Str("path", effectivePath).Msg("Failed to chmod directory")
		}
	}

	return &driver.VolumeInfo{
		VolumeID:  fmt.Sprintf("dir:%s", opts.Name),
		Name:      opts.Name,
		MountPath: targetPath,
	}, nil
}

func (d *Driver) Delete(ctx context.Context, opts driver.DeleteOptions) error {
	targetPath := opts.MountPath
	if targetPath == "" {
		targetPath = opts.Name
	}
	if targetPath == "" {
		return fmt.Errorf("directory target path cannot be empty")
	}

	effectivePath := targetPath
	if opts.HostPrefix != "" && targetPath != "" {
		effectivePath = filepath.Join(opts.HostPrefix, targetPath)
	}

	exists, err := d.Exists(ctx, effectivePath)
	if err != nil {
		return fmt.Errorf("failed to check if directory %q exists: %w", effectivePath, err)
	}
	if !exists {
		log.Warn().Str("path", targetPath).Msg("Directory does not exist, skipping deletion")
		return nil
	}

	if opts.SnapshotBeforeDelete {
		backupPath := fmt.Sprintf("%s-deleted-%d", strings.TrimSuffix(effectivePath, "/"), time.Now().Unix())
		log.Info().Str("path", effectivePath).Str("backup", backupPath).Msg("Preserving directory before deletion (snapshot-before-delete)")
		if err := os.Rename(effectivePath, backupPath); err != nil {
			log.Warn().Err(err).Str("backup", backupPath).Msg("Failed to rename directory for snapshot-before-delete, proceeding with delete")
		} else {
			return nil
		}
	}

	log.Info().Str("path", targetPath).Msg("Deleting directory")
	if err := os.RemoveAll(effectivePath); err != nil {
		return fmt.Errorf("failed to delete directory %q: %w", effectivePath, err)
	}

	return nil
}
