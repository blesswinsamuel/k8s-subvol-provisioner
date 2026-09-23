package driver

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"

	"github.com/rs/zerolog/log"
)

// ReconcileOwnerModeAtPath applies the declared owner and/or mode (from opts)
// to the directory at path, and recursively to everything beneath it when
// opts.Recursive. Symlinks are skipped and never followed. It returns
// human-readable change descriptions; empty when the path already matches or
// neither owner nor mode is declared. A missing path is not an error: no
// changes are returned (the volume may not be present on this host).
func ReconcileOwnerModeAtPath(path string, opts OwnerModeOptions) ([]string, error) {
	if opts.Owner == nil && opts.Mode == nil {
		return nil, nil
	}

	fi, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			log.Warn().Str("path", path).Msg("Mount directory does not exist, skipping owner/mode reconciliation")
			return nil, nil
		}
		return nil, fmt.Errorf("failed to stat %q: %w", path, err)
	}

	var changes []string
	if opts.Owner != nil {
		if stat, ok := fi.Sys().(*syscall.Stat_t); ok && (stat.Uid != uint32(opts.Owner.UID) || stat.Gid != uint32(opts.Owner.GID)) {
			if !opts.DryRun {
				if err := os.Chown(path, int(opts.Owner.UID), int(opts.Owner.GID)); err != nil {
					return nil, fmt.Errorf("failed to chown %q: %w", path, err)
				}
			}
			changes = append(changes, fmt.Sprintf("owner=%d:%d→%d:%d", stat.Uid, stat.Gid, opts.Owner.UID, opts.Owner.GID))
		}
	}
	if opts.Mode != nil {
		if want := opts.Mode.Perm(); fi.Mode().Perm() != want {
			if !opts.DryRun {
				if err := os.Chmod(path, want); err != nil {
					return nil, fmt.Errorf("failed to chmod %q: %w", path, err)
				}
			}
			changes = append(changes, fmt.Sprintf("mode=%04o→%04o", fi.Mode().Perm(), want))
		}
	}

	if opts.Recursive {
		reowned, rechmod, err := reconcileOwnerModeRecursive(path, opts)
		if err != nil {
			return nil, err
		}
		if reowned > 0 {
			changes = append(changes, fmt.Sprintf("owner recursive: %d paths", reowned))
		}
		if rechmod > 0 {
			changes = append(changes, fmt.Sprintf("mode recursive: %d paths", rechmod))
		}
	}
	return changes, nil
}

// reconcileOwnerModeRecursive walks the directory at root and applies the
// desired owner/mode to every file and directory whose ownership or
// permissions differ. Symlinks are skipped and never followed. Returns the
// number of paths that were (or in dry-run, would be) re-owned and re-chmodded.
func reconcileOwnerModeRecursive(root string, opts OwnerModeOptions) (reowned int, rechmod int, err error) {
	walkErr := filepath.WalkDir(root, func(p string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if p == root || entry.Type()&fs.ModeSymlink != 0 {
			return nil // root is handled by the caller; never follow symlinks
		}
		info, err := entry.Info()
		if err != nil {
			return fmt.Errorf("failed to stat %q: %w", p, err)
		}
		if opts.Owner != nil {
			if stat, ok := info.Sys().(*syscall.Stat_t); ok && (stat.Uid != uint32(opts.Owner.UID) || stat.Gid != uint32(opts.Owner.GID)) {
				if !opts.DryRun {
					if err := os.Chown(p, int(opts.Owner.UID), int(opts.Owner.GID)); err != nil {
						return fmt.Errorf("failed to chown %q: %w", p, err)
					}
				}
				reowned++
			}
		}
		if opts.Mode != nil {
			if want := opts.Mode.Perm(); info.Mode().Perm() != want {
				if !opts.DryRun {
					if err := os.Chmod(p, want); err != nil {
						return fmt.Errorf("failed to chmod %q: %w", p, err)
					}
				}
				rechmod++
			}
		}
		return nil
	})
	if walkErr != nil {
		return 0, 0, fmt.Errorf("failed recursive owner/mode reconciliation under %q: %w", root, walkErr)
	}
	return reowned, rechmod, nil
}
