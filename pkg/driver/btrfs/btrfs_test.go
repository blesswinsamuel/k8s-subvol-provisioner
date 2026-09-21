package btrfs

import (
	"context"
	"fmt"
	"strings"
	"testing"

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

func TestBtrfsCreateBasic(t *testing.T) {
	exec := newRecordExecutor()
	exec.errors["btrfs subvolume show /mnt/btrfs/sub1"] = fmt.Errorf("not a subvolume")

	d := New(WithExecutor(exec))
	info, err := d.Create(context.Background(), driver.CreateOptions{
		Name:       "sub1",
		MountPath:  "/mnt/btrfs/sub1",
		QuotaBytes: 5 * 1024 * 1024 * 1024,
	})

	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, "/mnt/btrfs/sub1", info.MountPath)

	expectedCreate := []string{"btrfs", "subvolume", "create", "/mnt/btrfs/sub1"}
	expectedLimit := []string{"btrfs", "qgroup", "limit", "5368709120", "/mnt/btrfs/sub1"}
	assert.Contains(t, exec.commands, expectedCreate)
	assert.Contains(t, exec.commands, expectedLimit)
}

func TestBtrfsDeleteWithSnapshot(t *testing.T) {
	exec := newRecordExecutor()
	exec.responses["btrfs subvolume show /mnt/btrfs/sub1"] = []byte("subvolume details")

	d := New(WithExecutor(exec))
	err := d.Delete(context.Background(), driver.DeleteOptions{
		Name:                 "sub1",
		MountPath:            "/mnt/btrfs/sub1",
		SnapshotBeforeDelete: true,
	})

	require.NoError(t, err)

	var foundSnapshot, foundDelete bool
	for _, cmd := range exec.commands {
		if len(cmd) >= 3 && cmd[1] == "subvolume" && cmd[2] == "snapshot" {
			foundSnapshot = true
		}
		if len(cmd) >= 4 && cmd[1] == "subvolume" && cmd[2] == "delete" && cmd[3] == "/mnt/btrfs/sub1" {
			foundDelete = true
		}
	}
	assert.True(t, foundSnapshot, "expected snapshot command")
	assert.True(t, foundDelete, "expected delete command")
}

func TestBtrfsCreateAdoptExisting(t *testing.T) {
	exec := newRecordExecutor()
	// btrfs subvolume show succeeds: subvolume already exists
	exec.responses["btrfs subvolume show /mnt/btrfs/existing-sub"] = []byte("subvolume details")

	d := New(WithExecutor(exec))
	info, err := d.Create(context.Background(), driver.CreateOptions{
		Name:          "existing-sub",
		MountPath:     "/mnt/btrfs/existing-sub",
		QuotaBytes:    5 * 1024 * 1024 * 1024,
		AdoptExisting: true,
	})

	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, "existing-sub", info.Name)
	assert.Equal(t, "/mnt/btrfs/existing-sub", info.MountPath)

	// Adoption must not mutate: no create, no qgroup limit, no property changes.
	for _, cmd := range exec.commands {
		if len(cmd) >= 2 && cmd[1] == "subvolume" && cmd[2] == "create" {
			t.Errorf("unexpected btrfs subvolume create during adoption: %v", cmd)
		}
		if len(cmd) >= 2 && cmd[1] == "qgroup" {
			t.Errorf("unexpected btrfs qgroup mutation during adoption: %v", cmd)
		}
	}
}

func TestBtrfsCreateExistingNoAdopt(t *testing.T) {
	exec := newRecordExecutor()
	exec.responses["btrfs subvolume show /mnt/btrfs/existing-sub"] = []byte("subvolume details")

	d := New(WithExecutor(exec))
	info, err := d.Create(context.Background(), driver.CreateOptions{
		Name:      "existing-sub",
		MountPath: "/mnt/btrfs/existing-sub",
	})

	require.Error(t, err)
	assert.Nil(t, info)
	assert.Contains(t, err.Error(), `btrfs subvolume "/mnt/btrfs/existing-sub" already exists`)

	for _, cmd := range exec.commands {
		if len(cmd) >= 3 && cmd[1] == "subvolume" && cmd[2] == "create" {
			t.Errorf("unexpected btrfs subvolume create: %v", cmd)
		}
	}
}
