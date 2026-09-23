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
		if !opts.AdoptExisting {
			return nil, fmt.Errorf("dataset %q already exists", opts.Name)
		}
		// Adopt the existing dataset as-is: no create, no property/quota/ownership mutations.
		mountPath := opts.MountPath
		if mountPath == "" {
			out, err := d.executor.Run(ctx, nil, d.zfsPath, "get", "-H", "-o", "value", "mountpoint", opts.Name)
			if err == nil {
				mountPath = strings.TrimSpace(string(out))
			}
		}
		log.Info().Str("dataset", opts.Name).Str("mountPath", mountPath).Msg("Adopting existing ZFS dataset")
		return &driver.VolumeInfo{
			VolumeID:  fmt.Sprintf("zfs:%s", opts.Name),
			Name:      opts.Name,
			MountPath: mountPath,
		}, nil
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

// reconcilableProps lists standard ZFS properties the controller may
// reconcile live. Anything outside this set (and user properties) is ignored
// with a warning; casesensitivity, quota-family siblings (refquota,
// reservation, refreservation) and other create-time-only properties are
// intentionally excluded.
var reconcilableProps = map[string]bool{
	"compression":    true,
	"atime":          true,
	"relatime":       true,
	"recordsize":     true,
	"sync":           true,
	"logbias":        true,
	"primarycache":   true,
	"secondarycache": true,
	"exec":           true,
	"setuid":         true,
	"devices":        true,
	"readonly":       true,
	"nbmand":         true,
	"snapdir":        true,
	"acltype":        true,
	"aclinherit":     true,
	"dedup":          true,
	"checksum":       true,
	"copies":         true,
}

// parseZFSQuota parses a `zfs get quota` value; "0", "none" and "-" mean unset.
func parseZFSQuota(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" || s == "-" || s == "0" || s == "none" {
		return 0, nil
	}
	return strconv.ParseInt(s, 10, 64)
}

func (d *Driver) ReconcileQuota(ctx context.Context, name string, quotaBytes *int64, dryRun bool) ([]string, error) {
	// -p renders the quota in parsable bytes; without it zfs prints
	// human-readable values (e.g. "1G", "2.10G") that cannot be compared.
	out, err := d.executor.Run(ctx, nil, d.zfsPath, "get", "-H", "-p", "-o", "value", "quota", name)
	if err != nil {
		return nil, fmt.Errorf("failed to read quota of zfs dataset %q: %w", name, err)
	}
	current, err := parseZFSQuota(string(out))
	if err != nil {
		return nil, fmt.Errorf("failed to parse quota of zfs dataset %q: %w", name, err)
	}

	if quotaBytes == nil {
		if current == 0 {
			return nil, nil
		}
		if err := d.runZfsSet(ctx, dryRun, name, "quota=none"); err != nil {
			return nil, fmt.Errorf("failed to unset quota of zfs dataset %q: %w", name, err)
		}
		return []string{fmt.Sprintf("quota=%d→none", current)}, nil
	}
	if current == *quotaBytes {
		return nil, nil
	}
	if err := d.runZfsSet(ctx, dryRun, name, fmt.Sprintf("quota=%d", *quotaBytes)); err != nil {
		return nil, fmt.Errorf("failed to set quota of zfs dataset %q to %d: %w", name, *quotaBytes, err)
	}
	return []string{fmt.Sprintf("quota=%d→%d", current, *quotaBytes)}, nil
}

func (d *Driver) ReconcileOwnerMode(ctx context.Context, opts driver.OwnerModeOptions) ([]string, error) {
	if opts.Owner == nil && opts.Mode == nil {
		return nil, nil
	}
	if opts.MountPath == "" || opts.MountPath == "none" || opts.MountPath == "legacy" {
		return nil, nil
	}
	path := opts.MountPath
	if opts.HostPrefix != "" {
		path = filepath.Join(opts.HostPrefix, path)
	}
	return driver.ReconcileOwnerModeAtPath(path, opts)
}

// getProps queries the given properties (plus user properties) and returns
// prop -> {value, source}.
func (d *Driver) getProps(ctx context.Context, name string, props []string) (map[string]zfsPropValue, error) {
	args := []string{"get", "-H", "-o", "property,value,source"}
	if len(props) > 0 {
		args = append(args, strings.Join(append([]string{}, props...), ","))
	} else {
		args = append(args, "all")
	}
	args = append(args, name)
	out, err := d.executor.Run(ctx, nil, d.zfsPath, args...)
	if err != nil {
		return nil, err
	}
	result := make(map[string]zfsPropValue)
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		v := zfsPropValue{Value: fields[1]}
		if len(fields) >= 3 {
			v.Source = fields[len(fields)-1]
		}
		result[fields[0]] = v
	}
	return result, nil
}

type zfsPropValue struct {
	Value  string
	Source string
}

// isLocallySet reports whether a property is set directly on the dataset
// (source "local" or a user property like "subvol.io:foo").
func isLocallySet(source string) bool {
	return strings.HasPrefix(source, "local") || strings.Contains(source, ":")
}

func (d *Driver) ReconcileProperties(ctx context.Context, name string, props map[string]string, dryRun bool) ([]string, error) {
	var changes []string

	// Filter out properties that are not safe/possible to reconcile live.
	desired := make(map[string]string, len(props))
	for k, v := range props {
		if strings.Contains(k, ":") {
			desired[k] = v
			continue
		}
		if !reconcilableProps[k] {
			log.Warn().Str("dataset", name).Str("property", k).Msg("Skipping non-reconcilable ZFS property")
			continue
		}
		desired[k] = v
	}

	current, err := d.getProps(ctx, name, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to read properties of zfs dataset %q: %w", name, err)
	}

	// Set/override desired properties that differ.
	for k, v := range desired {
		cur, ok := current[k]
		if ok && cur.Value == v {
			continue
		}
		if err := d.runZfsSet(ctx, dryRun, name, fmt.Sprintf("%s=%s", k, v)); err != nil {
			return nil, fmt.Errorf("failed to set zfs property %q on %q: %w", k, name, err)
		}
		if !ok {
			changes = append(changes, fmt.Sprintf("%s→%s", k, v))
		} else {
			changes = append(changes, fmt.Sprintf("%s=%s→%s", k, cur.Value, v))
		}
	}

	// Remove locally-set properties that are no longer desired.
	for k, cur := range current {
		if _, keep := desired[k]; keep {
			continue
		}
		if !isLocallySet(cur.Source) {
			continue
		}
		if !strings.Contains(k, ":") && !reconcilableProps[k] {
			continue
		}
		if err := d.runZfsInherit(ctx, dryRun, name, k); err != nil {
			return nil, fmt.Errorf("failed to inherit zfs property %q on %q: %w", k, name, err)
		}
		changes = append(changes, fmt.Sprintf("%s=%s→inherit", k, cur.Value))
	}

	return changes, nil
}

// runZfsSet applies a single property via `zfs set`, or only logs the planned
// command when dryRun is true.
func (d *Driver) runZfsSet(ctx context.Context, dryRun bool, name, prop string) error {
	if dryRun {
		log.Info().Str("dataset", name).Str("property", prop).Msg("DRY RUN: would run zfs set")
		return nil
	}
	if _, err := d.executor.Run(ctx, nil, d.zfsPath, "set", prop, name); err != nil {
		return err
	}
	return nil
}

// runZfsInherit resets a property to its inherited/default value, or only logs
// the planned command when dryRun is true.
func (d *Driver) runZfsInherit(ctx context.Context, dryRun bool, name, prop string) error {
	if dryRun {
		log.Info().Str("dataset", name).Str("property", prop).Msg("DRY RUN: would run zfs inherit")
		return nil
	}
	if _, err := d.executor.Run(ctx, nil, d.zfsPath, "inherit", name, prop); err != nil {
		return err
	}
	return nil
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
		quarantineName := fmt.Sprintf("%s-deleted-%d", opts.Name, time.Now().Unix())
		log.Info().Str("dataset", opts.Name).Str("quarantine", quarantineName).Msg("Renaming dataset to quarantine")
		if _, err := d.executor.Run(ctx, nil, d.zfsPath, "rename", opts.Name, quarantineName); err != nil {
			log.Warn().Err(err).Str("quarantine", quarantineName).Msg("Failed to rename dataset to quarantine, proceeding with destroy")
		} else {
			return nil
		}
	}

	log.Info().Str("dataset", opts.Name).Msg("Destroying ZFS dataset")
	if _, err := d.executor.Run(ctx, nil, d.zfsPath, "destroy", "-r", opts.Name); err != nil {
		return fmt.Errorf("failed to destroy zfs dataset %q: %w", opts.Name, err)
	}

	return nil
}
