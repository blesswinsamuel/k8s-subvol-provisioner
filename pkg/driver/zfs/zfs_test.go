package zfs

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/blesswinsamuel/k8s-subvol-provisioner/pkg/apis/config"
	"github.com/blesswinsamuel/k8s-subvol-provisioner/pkg/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recordExecutor struct {
	commands [][]string
	stdins   [][]byte
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

	// Verify snapshot command and destroy command were recorded
	var foundSnapshot, foundDestroy bool
	for _, cmd := range exec.commands {
		if len(cmd) >= 2 && cmd[1] == "snapshot" && strings.HasPrefix(cmd[2], "tank/k8s/old-pvc@deleted-") {
			foundSnapshot = true
		}
		if len(cmd) >= 4 && cmd[1] == "destroy" && cmd[2] == "-r" && cmd[3] == "tank/k8s/old-pvc" {
			foundDestroy = true
		}
	}
	assert.True(t, foundSnapshot, "expected snapshot command")
	assert.True(t, foundDestroy, "expected destroy command")
}
