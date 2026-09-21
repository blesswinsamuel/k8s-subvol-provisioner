package controller

import (
	"testing"

	"github.com/blesswinsamuel/k8s-subvol-provisioner/pkg/apis/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestBuildPersistentVolume(t *testing.T) {
	scPolicy := corev1.PersistentVolumeReclaimDelete
	sc := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "zfs-fast",
		},
		ReclaimPolicy: &scPolicy,
	}

	scName := "zfs-fast"
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-claim",
			Namespace: "default",
			UID:       "abc-123",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &scName,
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("20Gi"),
				},
			},
		},
	}

	pv, err := BuildPersistentVolume(BuildPVOptions{
		PVC:             pvc,
		StorageClass:    sc,
		ProvisionerName: "subvol.io/provisioner",
		DriverName:      "zfs",
		DatasetName:     "tank/k8s/default/test-claim",
		MountPath:       "/mnt/tank/k8s/default/test-claim",
		NodeName:        "nas-pc",
	})

	require.NoError(t, err)
	require.NotNil(t, pv)

	assert.Equal(t, "pvc-abc-123", pv.Name)
	assert.Equal(t, corev1.PersistentVolumeReclaimDelete, pv.Spec.PersistentVolumeReclaimPolicy)
	assert.Equal(t, "/mnt/tank/k8s/default/test-claim", pv.Spec.Local.Path)
	assert.Equal(t, "subvol.io/provisioner", pv.Annotations[config.AnnProvisionedBy])
	assert.Equal(t, "tank/k8s/default/test-claim", pv.Annotations[config.AnnDatasetName])
	assert.Equal(t, "nas-pc", pv.Spec.NodeAffinity.Required.NodeSelectorTerms[0].MatchExpressions[0].Values[0])
}
