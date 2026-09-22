package zfs

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/blesswinsamuel/k8s-subvol-provisioner/pkg/apis/config"
	"github.com/blesswinsamuel/k8s-subvol-provisioner/pkg/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recordExecutor struct {
	commands  [][]string
	stdins    [][]byte
	responses map[string][]byte
	errors    map[string]error
}

func newRecordExecutor() *recordExecutor {
	return &recordExecutor{
		responses: make(map[string][]byte),
		errors:    make(map[string]error),
	}
}

func (r *recordExecutor) Run(ctx context.Context, stdin []byte, cmd string, args ...string) ([]byte, error) {
	full := append([]string{cmd}, args...)
	r.commands = append(r.commands, full)
	r.stdins = append(r.stdins, stdin)

	key := strings.Join(full, " ")
	if err, ok := r.errors[key]; ok {
		return nil, err
	}
	if resp, ok := r.responses[key]; ok {
		return resp, nil
	}
	return []byte(""), nil
}

func TestZFSCreateBasic(t *testing.T) {
	exec := newRecordExecutor()
	// zfs list fails initially (dataset doesn't exist)
	exec.errors["zfs list -H -o name tank/k8s/my-pvc"] = fmt.Errorf("dataset does not exist")
	exec.responses["zfs get -H -o value mountpoint tank/k8s/my-pvc"] = []byte("/tank/k8s/my-pvc\n")

	d := New(WithExecutor(exec))
	info, err := d.Create(context.Background(), driver.CreateOptions{
		Name:       "tank/k8s/my-pvc",
		QuotaBytes: 10 * 1024 * 1024 * 1024, // 10GiB
		Properties: map[string]string{
			"compression": "zstd",
		},
	})

	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, "tank/k8s/my-pvc", info.Name)
	assert.Equal(t, "/tank/k8s/my-pvc", info.MountPath)

	// Check create command args
	expectedCmd := []string{
		"zfs", "create", "-p",
		"-o", "quota=10737418240",
		"-o", "compression=zstd",
		"tank/k8s/my-pvc",
	}
	assert.Contains(t, exec.commands, expectedCmd)
}

func TestZFSCreateWithEncryption(t *testing.T) {
	exec := newRecordExecutor()
	exec.errors["zfs list -H -o name tank/k8s/sec-pvc"] = fmt.Errorf("does not exist")

	d := New(WithExecutor(exec))
	_, err := d.Create(context.Background(), driver.CreateOptions{
		Name:      "tank/k8s/sec-pvc",
		MountPath: "/mnt/tank/k8s/sec-pvc",
		Encryption: &config.EncryptionConfig{
			Enabled:     true,
			KeyFormat:   "hex",
			KeyLocation: "http://vault:8200/v1/secret/key",
		},
	})

	require.NoError(t, err)

	expectedCmd := []string{
		"zfs", "create", "-p",
		"-o", "encryption=on",
		"-o", "keyformat=hex",
		"-o", "keylocation=http://vault:8200/v1/secret/key",
		"tank/k8s/sec-pvc",
	}
	assert.Contains(t, exec.commands, expectedCmd)
}

func TestZFSDeleteWithSnapshot(t *testing.T) {
	exec := newRecordExecutor()
	// zfs list succeeds (dataset exists)
	exec.responses["zfs list -H -o name tank/k8s/old-pvc"] = []byte("tank/k8s/old-pvc\n")

	d := New(WithExecutor(exec))
	err := d.Delete(context.Background(), driver.DeleteOptions{
		Name:                 "tank/k8s/old-pvc",
		SnapshotBeforeDelete: true,
	})

	require.NoError(t, err)

	// Verify rename-to-quarantine was recorded and no destroy ran
	var foundRename, foundDestroy bool
	for _, cmd := range exec.commands {
		if len(cmd) >= 2 && cmd[1] == "rename" && cmd[2] == "tank/k8s/old-pvc" && strings.HasPrefix(cmd[3], "tank/k8s/old-pvc-deleted-") {
			foundRename = true
		}
		if len(cmd) >= 2 && cmd[1] == "destroy" {
			foundDestroy = true
		}
	}
	assert.True(t, foundRename, "expected rename to quarantine dataset")
	assert.False(t, foundDestroy, "quarantined dataset must not be destroyed")
}

// renameFailingExecutor forces the quarantine rename to fail so the
// destroy fallback path can be exercised.
type renameFailingExecutor struct {
	recordExecutor
}

func (r *renameFailingExecutor) Run(ctx context.Context, stdin []byte, cmd string, args ...string) ([]byte, error) {
	out, err := r.recordExecutor.Run(ctx, stdin, cmd, args...)
	if len(args) > 0 && args[0] == "rename" {
		return out, fmt.Errorf("rename failed")
	}
	return out, err
}

func TestZFSDeleteSnapshotRenameFailsDestroys(t *testing.T) {
	exec := &renameFailingExecutor{recordExecutor: *newRecordExecutor()}
	// zfs list succeeds (dataset exists)
	exec.responses["zfs list -H -o name tank/k8s/old-pvc"] = []byte("tank/k8s/old-pvc\n")

	d := New(WithExecutor(exec))
	err := d.Delete(context.Background(), driver.DeleteOptions{
		Name:                 "tank/k8s/old-pvc",
		SnapshotBeforeDelete: true,
	})

	require.NoError(t, err)

	var foundRename, foundDestroy bool
	for _, cmd := range exec.commands {
		if len(cmd) >= 2 && cmd[1] == "rename" {
			foundRename = true
		}
		if len(cmd) >= 4 && cmd[1] == "destroy" && cmd[2] == "-r" && cmd[3] == "tank/k8s/old-pvc" {
			foundDestroy = true
		}
	}
	assert.True(t, foundRename, "expected attempted quarantine rename")
	assert.True(t, foundDestroy, "expected fallback destroy after failed rename")
}

func TestZFSCreateAdoptExisting(t *testing.T) {
	exec := newRecordExecutor()
	// zfs list succeeds: dataset already exists
	exec.responses["zfs list -H -o name tank/k8s/existing-pvc"] = []byte("tank/k8s/existing-pvc\n")
	exec.responses["zfs get -H -o value mountpoint tank/k8s/existing-pvc"] = []byte("/tank/k8s/existing-pvc\n")

	d := New(WithExecutor(exec))
	info, err := d.Create(context.Background(), driver.CreateOptions{
		Name:          "tank/k8s/existing-pvc",
		QuotaBytes:    10 * 1024 * 1024 * 1024,
		Properties:    map[string]string{"compression": "zstd"},
		AdoptExisting: true,
	})

	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, "tank/k8s/existing-pvc", info.Name)
	assert.Equal(t, "/tank/k8s/existing-pvc", info.MountPath)

	// Adoption must not mutate: no create, no set, no promote/inherit commands.
	for _, cmd := range exec.commands {
		if len(cmd) >= 2 && cmd[1] == "create" {
			t.Errorf("unexpected zfs create during adoption: %v", cmd)
		}
		if len(cmd) >= 2 && cmd[1] == "set" {
			t.Errorf("unexpected zfs set during adoption: %v", cmd)
		}
	}
	// Mountpoint is queried the same way the create path does.
	assert.Contains(t, exec.commands, []string{"zfs", "get", "-H", "-o", "value", "mountpoint", "tank/k8s/existing-pvc"})
}

func TestZFSCreateExistingNoAdopt(t *testing.T) {
	exec := newRecordExecutor()
	// zfs list succeeds: dataset already exists, adopt annotation absent.
	exec.responses["zfs list -H -o name tank/k8s/existing-pvc"] = []byte("tank/k8s/existing-pvc\n")

	d := New(WithExecutor(exec))
	info, err := d.Create(context.Background(), driver.CreateOptions{
		Name: "tank/k8s/existing-pvc",
	})

	require.Error(t, err)
	assert.Nil(t, info)
	assert.Contains(t, err.Error(), `dataset "tank/k8s/existing-pvc" already exists`)

	for _, cmd := range exec.commands {
		if len(cmd) >= 2 && cmd[1] == "create" {
			t.Errorf("unexpected zfs create: %v", cmd)
		}
	}
}

func TestZFSReconcileQuota(t *testing.T) {
	quota := int64(10 * 1024 * 1024 * 1024)

	t.Run("sets quota when different", func(t *testing.T) {
		exec := newRecordExecutor()
		exec.responses["zfs get -H -p -o value quota tank/k8s/pvc"] = []byte("5368709120\n") // 5Gi

		d := New(WithExecutor(exec))
		changes, err := d.ReconcileQuota(context.Background(), "tank/k8s/pvc", &quota, false)
		require.NoError(t, err)
		assert.Equal(t, []string{"quota=5.0G→10G"}, changes)
		assert.Contains(t, exec.commands, []string{"zfs", "set", fmt.Sprintf("quota=%d", quota), "tank/k8s/pvc"})
	})

	t.Run("no-op when matching", func(t *testing.T) {
		exec := newRecordExecutor()
		exec.responses["zfs get -H -p -o value quota tank/k8s/pvc"] = []byte("10737418240\n")

		d := New(WithExecutor(exec))
		changes, err := d.ReconcileQuota(context.Background(), "tank/k8s/pvc", &quota, false)
		require.NoError(t, err)
		assert.Empty(t, changes)
		assert.Len(t, exec.commands, 1, "only the quota get should have run")
	})

	t.Run("unsets quota when nil and quota exists", func(t *testing.T) {
		exec := newRecordExecutor()
		exec.responses["zfs get -H -p -o value quota tank/k8s/pvc"] = []byte("10737418240\n")

		d := New(WithExecutor(exec))
		changes, err := d.ReconcileQuota(context.Background(), "tank/k8s/pvc", nil, false)
		require.NoError(t, err)
		assert.Equal(t, []string{"quota=10G→none"}, changes)
		assert.Contains(t, exec.commands, []string{"zfs", "set", "quota=none", "tank/k8s/pvc"})
	})

	t.Run("no-op when nil and unset", func(t *testing.T) {
		exec := newRecordExecutor()
		exec.responses["zfs get -H -p -o value quota tank/k8s/pvc"] = []byte("0\n")

		d := New(WithExecutor(exec))
		changes, err := d.ReconcileQuota(context.Background(), "tank/k8s/pvc", nil, false)
		require.NoError(t, err)
		assert.Empty(t, changes)
		assert.Len(t, exec.commands, 1)
	})

	t.Run("dry run records nothing", func(t *testing.T) {
		exec := newRecordExecutor()
		exec.responses["zfs get -H -p -o value quota tank/k8s/pvc"] = []byte("5368709120\n")

		d := New(WithExecutor(exec))
		changes, err := d.ReconcileQuota(context.Background(), "tank/k8s/pvc", &quota, true)
		require.NoError(t, err)
		assert.Equal(t, []string{"quota=5.0G→10G"}, changes)
		for _, cmd := range exec.commands {
			if len(cmd) >= 2 && cmd[1] == "set" {
				t.Errorf("unexpected zfs set during dry run: %v", cmd)
			}
		}
	})
}

func TestZFSReconcileProperties(t *testing.T) {
	t.Run("sets changed and inherits removed", func(t *testing.T) {
		exec := newRecordExecutor()
		exec.responses["zfs get -H -o property,value,source all tank/k8s/pvc"] = []byte(
			"compression\toff\tlocal\n" +
				"atime\ton\tdefault\n" +
				"mountpoint\t/tank/k8s/pvc\tlocal\n" +
				"subvol.io:custom\tbar\tlocal\n")

		d := New(WithExecutor(exec))
		changes, err := d.ReconcileProperties(context.Background(), "tank/k8s/pvc", map[string]string{
			"compression": "lz4",
			"atime":       "off",
		}, false)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"compression=off→lz4", "atime=on→off", "subvol.io:custom=bar→inherit"}, changes)
	})

	t.Run("skips non-reconcilable properties", func(t *testing.T) {
		exec := newRecordExecutor()
		exec.responses["zfs get -H -o property,value,source all tank/k8s/pvc"] = []byte(
			"mountpoint\t/tank/k8s/pvc\tlocal\n")

		d := New(WithExecutor(exec))
		_, err := d.ReconcileProperties(context.Background(), "tank/k8s/pvc", map[string]string{
			"compression":    "lz4",
			"casesensitivity": "insensitive", // create-time only, must be ignored
		}, false)
		require.NoError(t, err)

		var foundSet bool
		for _, cmd := range exec.commands {
			if len(cmd) >= 2 && cmd[1] == "set" && strings.HasPrefix(cmd[2], "casesensitivity") {
				t.Errorf("unexpected zfs set for create-time-only property: %v", cmd)
			}
			if len(cmd) >= 2 && cmd[1] == "set" {
				foundSet = true
			}
		}
		assert.True(t, foundSet, "expected compression to be set")
		// mountpoint is local but not reconcilable: must not be inherited.
		for _, cmd := range exec.commands {
			if len(cmd) >= 2 && cmd[1] == "inherit" {
				t.Errorf("unexpected zfs inherit: %v", cmd)
			}
		}
	})

	t.Run("ignores inherited and default sources for removal", func(t *testing.T) {
		exec := newRecordExecutor()
		exec.responses["zfs get -H -o property,value,source all tank/k8s/pvc"] = []byte(
			"compression\tzstd\tinherited from tank/k8s\n" +
				"atime\ton\tdefault\n")

		d := New(WithExecutor(exec))
		_, err := d.ReconcileProperties(context.Background(), "tank/k8s/pvc", nil, false)
		require.NoError(t, err)
		for _, cmd := range exec.commands {
			if len(cmd) >= 2 && (cmd[1] == "set" || cmd[1] == "inherit") {
				t.Errorf("unexpected mutation: %v", cmd)
			}
		}
	})
}

func TestZFSReconcileOwnerMode(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0755))

	t.Run("skips when matching", func(t *testing.T) {
		d := New(WithExecutor(newRecordExecutor()))
		mode := os.FileMode(0755)
		changes, err := d.ReconcileOwnerMode(context.Background(), driver.OwnerModeOptions{
			Name:      "tank/k8s/pvc",
			MountPath: dir,
			Owner:     nil,
			Mode:      &mode,
		})
		require.NoError(t, err)
		assert.Empty(t, changes)
	})

	t.Run("changes mode and reports change", func(t *testing.T) {
		d := New(WithExecutor(newRecordExecutor()))
		mode := os.FileMode(0750)
		changes, err := d.ReconcileOwnerMode(context.Background(), driver.OwnerModeOptions{
			Name:      "tank/k8s/pvc",
			MountPath: dir,
			Mode:      &mode,
		})
		require.NoError(t, err)
		assert.Equal(t, []string{"mode=0755→0750"}, changes)

		fi, err := os.Stat(dir)
		require.NoError(t, err)
		assert.Equal(t, fs.FileMode(0750), fi.Mode().Perm())
	})

	t.Run("skips legacy mountpoints", func(t *testing.T) {
		d := New(WithExecutor(newRecordExecutor()))
		mode := os.FileMode(0750)
		changes, err := d.ReconcileOwnerMode(context.Background(), driver.OwnerModeOptions{
			Name:      "tank/k8s/pvc",
			MountPath: "legacy",
			Mode:      &mode,
		})
		require.NoError(t, err)
		assert.Empty(t, changes)
	})
}
