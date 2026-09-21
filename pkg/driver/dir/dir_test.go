package dir

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/blesswinsamuel/k8s-subvol-provisioner/pkg/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDirCreateBasic(t *testing.T) {
	tempDir := t.TempDir()
	targetPath := filepath.Join(tempDir, "ns1", "pvc1")

	d := New()
	mode := os.FileMode(0700)
	info, err := d.Create(context.Background(), driver.CreateOptions{
		Name:      "ns1/pvc1",
		MountPath: targetPath,
		Mode:      &mode,
	})

	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, "ns1/pvc1", info.Name)
	assert.Equal(t, targetPath, info.MountPath)
	assert.Equal(t, "dir:ns1/pvc1", info.VolumeID)

	fi, err := os.Stat(targetPath)
	require.NoError(t, err)
	assert.True(t, fi.IsDir())
	assert.Equal(t, os.FileMode(0700), fi.Mode().Perm())
}

func TestDirCreateHostPrefix(t *testing.T) {
	hostPrefix := t.TempDir()
	relMount := "/data/volumes/sub1"

	d := New()
	info, err := d.Create(context.Background(), driver.CreateOptions{
		Name:       "sub1",
		MountPath:  relMount,
		HostPrefix: hostPrefix,
	})

	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, relMount, info.MountPath)

	actualPath := filepath.Join(hostPrefix, relMount)
	fi, err := os.Stat(actualPath)
	require.NoError(t, err)
	assert.True(t, fi.IsDir())
}

func TestDirCreateExistingNoAdopt(t *testing.T) {
	tempDir := t.TempDir()
	targetPath := filepath.Join(tempDir, "existing")
	require.NoError(t, os.Mkdir(targetPath, 0755))

	d := New()
	info, err := d.Create(context.Background(), driver.CreateOptions{
		Name:      "existing",
		MountPath: targetPath,
	})

	require.Error(t, err)
	assert.Nil(t, info)
	assert.Contains(t, err.Error(), "already exists")
}

func TestDirCreateAdoptExisting(t *testing.T) {
	tempDir := t.TempDir()
	targetPath := filepath.Join(tempDir, "adopt-me")
	require.NoError(t, os.Mkdir(targetPath, 0755))

	testFile := filepath.Join(targetPath, "sample.txt")
	require.NoError(t, os.WriteFile(testFile, []byte("data"), 0644))

	d := New()
	info, err := d.Create(context.Background(), driver.CreateOptions{
		Name:          "adopt-me",
		MountPath:     targetPath,
		AdoptExisting: true,
	})

	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, targetPath, info.MountPath)

	// Verify original file is still intact
	content, err := os.ReadFile(testFile)
	require.NoError(t, err)
	assert.Equal(t, "data", string(content))
}

func TestDirDeleteBasic(t *testing.T) {
	tempDir := t.TempDir()
	targetPath := filepath.Join(tempDir, "to-delete")
	require.NoError(t, os.Mkdir(targetPath, 0755))

	d := New()
	err := d.Delete(context.Background(), driver.DeleteOptions{
		Name:      "to-delete",
		MountPath: targetPath,
	})

	require.NoError(t, err)
	_, err = os.Stat(targetPath)
	assert.True(t, os.IsNotExist(err))
}

func TestDirDeleteSnapshotBeforeDelete(t *testing.T) {
	tempDir := t.TempDir()
	targetPath := filepath.Join(tempDir, "with-snapshot")
	require.NoError(t, os.Mkdir(targetPath, 0755))

	testFile := filepath.Join(targetPath, "data.txt")
	require.NoError(t, os.WriteFile(testFile, []byte("important data"), 0644))

	d := New()
	err := d.Delete(context.Background(), driver.DeleteOptions{
		Name:                 "with-snapshot",
		MountPath:            targetPath,
		SnapshotBeforeDelete: true,
	})

	require.NoError(t, err)

	// Original path should no longer exist
	_, err = os.Stat(targetPath)
	assert.True(t, os.IsNotExist(err))

	// Backup path should exist and contain our test file
	entries, err := os.ReadDir(tempDir)
	require.NoError(t, err)

	var foundBackup bool
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "with-snapshot-deleted-") {
			foundBackup = true
			data, err := os.ReadFile(filepath.Join(tempDir, entry.Name(), "data.txt"))
			require.NoError(t, err)
			assert.Equal(t, "important data", string(data))
		}
	}
	assert.True(t, foundBackup, "expected backup directory with -deleted- prefix")
}

func TestDirDeleteNonExistent(t *testing.T) {
	tempDir := t.TempDir()
	targetPath := filepath.Join(tempDir, "does-not-exist")

	d := New()
	err := d.Delete(context.Background(), driver.DeleteOptions{
		Name:      "does-not-exist",
		MountPath: targetPath,
	})

	require.NoError(t, err)
}
