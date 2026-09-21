package controller

import (
	"context"
	"strings"
	"testing"

	"github.com/blesswinsamuel/k8s-subvol-provisioner/pkg/apis/config"
	"github.com/blesswinsamuel/k8s-subvol-provisioner/pkg/driver"
	"github.com/blesswinsamuel/k8s-subvol-provisioner/pkg/driver/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
)

func TestControllerProvisioning(t *testing.T) {
	ctx := context.Background()
	mockDriver := mock.New()

	sc := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "zfs-mock",
		},
		Provisioner: "subvol.io/provisioner",
		Parameters: map[string]string{
			config.ParamDriver:       "mock",
			config.ParamParent:       "pool/k8s",
			config.ParamMountPrefix:  "/mnt/pool/k8s",
			config.ParamPathTemplate: "{{ .Namespace }}/{{ .PVC }}",
			config.ParamNode:         "nas-pc",
		},
	}

	scName := "zfs-mock"
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "media-claim",
			Namespace: "media",
			UID:       "111-222-333",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &scName,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("100Gi"),
				},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Phase: corev1.ClaimPending,
		},
	}

	client := fake.NewSimpleClientset(sc, pvc)

	ctrl, err := NewController(ControllerOptions{
		Client:          client,
		NodeName:        "nas-pc",
		ProvisionerName: "subvol.io/provisioner",
		Drivers: map[string]driver.Driver{
			"mock": mockDriver,
		},
	})
	require.NoError(t, err)

	// Run single reconciliation cycle
	ctrl.reconcileClaims(ctx)

	// Check if mock driver created volume
	expectedName := "pool/k8s/media/media-claim"
	vol := mockDriver.GetVolume(expectedName)
	require.NotNil(t, vol, "expected mock driver to create volume %s", expectedName)
	assert.Equal(t, "/mnt/pool/k8s/media/media-claim", vol.MountPath)

	// Check if PV was created in API
	pvs, err := client.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	require.Len(t, pvs.Items, 1)

	pv := pvs.Items[0]
	assert.Equal(t, "pvc-111-222-333", pv.Name)
	assert.Equal(t, "/mnt/pool/k8s/media/media-claim", pv.Spec.Local.Path)
	assert.Equal(t, "subvol.io/provisioner", pv.Annotations[config.AnnProvisionedBy])
}

func TestControllerSkipOtherNode(t *testing.T) {
	ctx := context.Background()
	mockDriver := mock.New()

	sc := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "zfs-remote",
		},
		Provisioner: "subvol.io/provisioner",
		Parameters: map[string]string{
			config.ParamDriver: "mock",
			config.ParamNode:   "other-node",
		},
	}

	scName := "zfs-remote"
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-claim",
			Namespace: "default",
			UID:       "999",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &scName,
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Phase: corev1.ClaimPending,
		},
	}

	client := fake.NewSimpleClientset(sc, pvc)

	// Controller runs on "my-node", but SC targets "other-node"
	ctrl, err := NewController(ControllerOptions{
		Client:   client,
		NodeName: "my-node",
		Drivers: map[string]driver.Driver{
			"mock": mockDriver,
		},
	})
	require.NoError(t, err)

	ctrl.reconcileClaims(ctx)

	// No PV should be created
	pvs, err := client.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	assert.Empty(t, pvs.Items)
}

func TestControllerDeleteReleasedPV(t *testing.T) {
	ctx := context.Background()
	mockDriver := mock.New()

	// Pre-create volume in mock driver
	_, err := mockDriver.Create(ctx, driver.CreateOptions{
		Name: "tank/k8s/default/old-pvc",
	})
	require.NoError(t, err)

	deletePolicy := corev1.PersistentVolumeReclaimDelete
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pvc-old-123",
			Annotations: map[string]string{
				config.AnnProvisionedBy: config.DefaultProvisionerName,
				config.AnnSubvolDriver:  "mock",
				config.AnnDatasetName:   "tank/k8s/default/old-pvc",
			},
		},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: deletePolicy,
			NodeAffinity: &corev1.VolumeNodeAffinity{
				Required: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{
						{
							MatchExpressions: []corev1.NodeSelectorRequirement{
								{
									Key:      "kubernetes.io/hostname",
									Operator: corev1.NodeSelectorOpIn,
									Values:   []string{"nas-pc"},
								},
							},
						},
					},
				},
			},
		},
		Status: corev1.PersistentVolumeStatus{
			Phase: corev1.VolumeReleased,
		},
	}

	client := fake.NewSimpleClientset(pv)

	ctrl, err := NewController(ControllerOptions{
		Client:   client,
		NodeName: "nas-pc",
		Drivers: map[string]driver.Driver{
			"mock": mockDriver,
		},
	})
	require.NoError(t, err)

	ctrl.reconcileVolumes(ctx)

	// Mock driver volume should be deleted
	exists, err := mockDriver.Exists(ctx, "tank/k8s/default/old-pvc")
	require.NoError(t, err)
	assert.False(t, exists)

	// PV should be deleted from K8s
	pvs, err := client.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	assert.Empty(t, pvs.Items)
}

func TestControllerAdoptExisting(t *testing.T) {
	ctx := context.Background()
	mockDriver := mock.New()

	// Pre-create the dataset the PVC will adopt (simulating a migrated static local PV).
	_, err := mockDriver.Create(ctx, driver.CreateOptions{
		Name:      "pool/k8s/media/media-claim",
		MountPath: "/mnt/pool/k8s/media/media-claim",
	})
	require.NoError(t, err)

	sc := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "zfs-mock",
		},
		Provisioner: "subvol.io/provisioner",
		Parameters: map[string]string{
			config.ParamDriver:       "mock",
			config.ParamParent:       "pool/k8s",
			config.ParamMountPrefix:  "/mnt/pool/k8s",
			config.ParamPathTemplate: "{{ .Namespace }}/{{ .PVC }}",
			config.ParamNode:         "nas-pc",
		},
	}

	scName := "zfs-mock"
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "media-claim",
			Namespace: "media",
			UID:       "444-555-666",
			Annotations: map[string]string{
				config.AnnAdoptExisting: "true",
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &scName,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("100Gi"),
				},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Phase: corev1.ClaimPending,
		},
	}

	client := fake.NewSimpleClientset(sc, pvc)
	recorder := record.NewFakeRecorder(10)

	ctrl, err := NewController(ControllerOptions{
		Client:          client,
		NodeName:        "nas-pc",
		ProvisionerName: "subvol.io/provisioner",
		Drivers: map[string]driver.Driver{
			"mock": mockDriver,
		},
		Recorder: recorder,
	})
	require.NoError(t, err)

	ctrl.reconcileClaims(ctx)

	// The PV should be built against the existing dataset.
	pvs, err := client.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	require.Len(t, pvs.Items, 1)
	assert.Equal(t, "pvc-444-555-666", pvs.Items[0].Name)
	assert.Equal(t, "/mnt/pool/k8s/media/media-claim", pvs.Items[0].Spec.Local.Path)

	// Adoption must not have re-created or mutated the mock volume: the recorded
	// CreateOptions are still the original ones from pre-seeding.
	origOpts, ok := mockDriver.GetCreateOptions("pool/k8s/media/media-claim")
	require.True(t, ok)
	assert.False(t, origOpts.AdoptExisting, "original create options must be untouched")

	// A Normal VolumeAdopted event must have been emitted.
	select {
	case event := <-recorder.Events:
		assert.Contains(t, event, "VolumeAdopted")
	default:
		t.Error("expected a VolumeAdopted event to be emitted")
	}
}

func TestControllerExistingNoAdopt(t *testing.T) {
	ctx := context.Background()
	mockDriver := mock.New()

	// Pre-create the dataset; PVC lacks the adopt annotation.
	_, err := mockDriver.Create(ctx, driver.CreateOptions{
		Name:      "pool/k8s/media/media-claim",
		MountPath: "/mnt/pool/k8s/media/media-claim",
	})
	require.NoError(t, err)

	sc := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "zfs-mock",
		},
		Provisioner: "subvol.io/provisioner",
		Parameters: map[string]string{
			config.ParamDriver:       "mock",
			config.ParamParent:       "pool/k8s",
			config.ParamMountPrefix:  "/mnt/pool/k8s",
			config.ParamPathTemplate: "{{ .Namespace }}/{{ .PVC }}",
			config.ParamNode:         "nas-pc",
		},
	}

	scName := "zfs-mock"
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "media-claim",
			Namespace: "media",
			UID:       "777-888-999",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &scName,
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Phase: corev1.ClaimPending,
		},
	}

	client := fake.NewSimpleClientset(sc, pvc)
	recorder := record.NewFakeRecorder(10)

	ctrl, err := NewController(ControllerOptions{
		Client:          client,
		NodeName:        "nas-pc",
		ProvisionerName: "subvol.io/provisioner",
		Drivers: map[string]driver.Driver{
			"mock": mockDriver,
		},
		Recorder: recorder,
	})
	require.NoError(t, err)

	ctrl.reconcileClaims(ctx)

	// Without the annotation, an existing dataset must fail exactly as before:
	// no PV created, ProvisioningFailed warning emitted.
	pvs, err := client.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	assert.Empty(t, pvs.Items)

	foundFailure := false
	for {
		select {
		case event := <-recorder.Events:
			if strings.Contains(event, "ProvisioningFailed") {
				foundFailure = true
			}
		default:
			assert.True(t, foundFailure, "expected a ProvisioningFailed event")
			return
		}
	}
}
