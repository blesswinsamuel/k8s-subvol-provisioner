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
	ctrl.processPVC(ctx, pvc)

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

	ctrl.processPVC(ctx, pvc)

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

	ctrl.processPV(ctx, pv)

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

	ctrl.processPVC(ctx, pvc)

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

	ctrl.processPVC(ctx, pvc)

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

func TestControllerReconcileVolumeStatePlanByDefault(t *testing.T) {
	ctx := context.Background()
	mockDriver := mock.New()

	sc := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: "zfs-mock"},
		Provisioner: "subvol.io/provisioner",
		Parameters: map[string]string{
			config.ParamDriver:      "mock",
			config.ParamParent:      "pool/k8s",
			config.ParamMountPrefix: "/mnt/pool/k8s",
			config.ParamNode:        "nas-pc",
			config.ParamProperties:  "compression=zstd",
		},
	}

	scName := "zfs-mock"
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "media-claim",
			Namespace: "media",
			UID:       "aaa-bbb-ccc",
			Annotations: map[string]string{
				config.AnnQuota:      "20Gi",
				config.AnnProperties: "compression=lz4\nrecordsize=1M",
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &scName,
			VolumeName:       "pvc-aaa-bbb-ccc",
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Phase: corev1.ClaimBound,
		},
	}

	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pvc-aaa-bbb-ccc",
			Annotations: map[string]string{
				config.AnnProvisionedBy: "subvol.io/provisioner",
				config.AnnSubvolDriver:  "mock",
				config.AnnDatasetName:   "pool/k8s/media/media-claim",
				config.AnnHostMountPath: "/mnt/pool/k8s/media/media-claim",
			},
		},
		Spec: corev1.PersistentVolumeSpec{
			ClaimRef: &corev1.ObjectReference{
				Kind:      "PersistentVolumeClaim",
				Namespace: "media",
				Name:      "media-claim",
			},
		},
		Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
	}

	client := fake.NewSimpleClientset(sc, pvc, pv)
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

	ctrl.processPVC(ctx, pvc)

	calls := mockDriver.GetReconcileCalls()
	require.Len(t, calls, 3, "expected quota, owner/mode and properties reconcile calls")

	var quotaCall *mock.ReconcileCall
	var propsCall *mock.ReconcileCall
	for i := range calls {
		if calls[i].Quota != nil {
			quotaCall = &calls[i]
		}
		if calls[i].Props != nil {
			propsCall = &calls[i]
		}
	}
	require.NotNil(t, quotaCall, "expected a quota reconcile call")
	assert.Equal(t, "pool/k8s/media/media-claim", quotaCall.Name)
	assert.Equal(t, int64(20*1024*1024*1024), *quotaCall.Quota)
	assert.True(t, quotaCall.DryRun, "changes must be planned, not applied, without authorization")

	require.NotNil(t, propsCall, "expected a properties reconcile call")
	// StorageClass base + PVC overlay.
	assert.Equal(t, map[string]string{
		"compression": "lz4",
		"recordsize":  "1M",
	}, propsCall.Props)

	// A VolumeDryRun event must have been emitted; the dataset stays untouched.
	found := false
	for {
		select {
		case event := <-recorder.Events:
			if strings.Contains(event, "VolumeDryRun") {
				found = true
			}
		default:
			assert.True(t, found, "expected a VolumeDryRun event")
			return
		}
	}
}

func TestControllerReconcileVolumeStateAutoApply(t *testing.T) {
	ctx := context.Background()
	mockDriver := mock.New()

	sc := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: "zfs-mock"},
		Provisioner: "subvol.io/provisioner",
		Parameters: map[string]string{
			config.ParamDriver:    "mock",
			config.ParamNode:      "nas-pc",
			config.ParamAutoApply: "true",
		},
	}

	scName := "zfs-mock"
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "media-claim",
			Namespace: "media",
			UID:       "aaa-bbb-ccc",
			Annotations: map[string]string{
				config.AnnQuota: "20Gi",
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &scName,
			VolumeName:       "pvc-aaa-bbb-ccc",
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Phase: corev1.ClaimBound,
		},
	}

	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pvc-aaa-bbb-ccc",
			Annotations: map[string]string{
				config.AnnProvisionedBy: "subvol.io/provisioner",
				config.AnnSubvolDriver:  "mock",
				config.AnnDatasetName:   "pool/k8s/media/media-claim",
			},
		},
		Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
	}

	client := fake.NewSimpleClientset(sc, pvc, pv)
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

	ctrl.processPVC(ctx, pvc)

	calls := mockDriver.GetReconcileCalls()
	require.Len(t, calls, 3, "expected quota, owner/mode and properties reconcile calls")
	for _, call := range calls {
		assert.False(t, call.DryRun, "autoApply must apply changes for real")
	}

	found := false
	for {
		select {
		case event := <-recorder.Events:
			if strings.Contains(event, "VolumeUpdated") {
				found = true
			}
		default:
			assert.True(t, found, "expected a VolumeUpdated event")
			return
		}
	}
}

func TestControllerReconcileVolumeStateApplyAnnotation(t *testing.T) {
	ctx := context.Background()
	mockDriver := mock.New()

	scName := "zfs-mock"
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "media-claim",
			Namespace: "media",
			UID:       "aaa-bbb-ccc",
			Annotations: map[string]string{
				config.AnnQuota: "20Gi",
				config.AnnApply: "true",
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &scName,
			VolumeName:       "pvc-aaa-bbb-ccc",
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}

	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pvc-aaa-bbb-ccc",
			Annotations: map[string]string{
				config.AnnProvisionedBy: "subvol.io/provisioner",
				config.AnnSubvolDriver:  "mock",
				config.AnnDatasetName:   "pool/k8s/media/media-claim",
			},
		},
		Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
	}

	sc := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: "zfs-mock"},
		Provisioner: "subvol.io/provisioner",
		Parameters: map[string]string{
			config.ParamDriver: "mock",
		},
	}

	client := fake.NewSimpleClientset(sc, pvc, pv)
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

	ctrl.processPVC(ctx, pvc)

	calls := mockDriver.GetReconcileCalls()
	require.Len(t, calls, 3, "expected quota, owner/mode and properties reconcile calls")
	for _, call := range calls {
		assert.False(t, call.DryRun, "apply annotation must authorize real changes")
	}

	// The one-shot apply annotation must be removed after applying.
	updated, err := client.CoreV1().PersistentVolumeClaims("media").Get(ctx, "media-claim", metav1.GetOptions{})
	require.NoError(t, err)
	assert.NotContains(t, updated.Annotations, config.AnnApply, "apply annotation must be removed after applying")

	found := false
	for {
		select {
		case event := <-recorder.Events:
			if strings.Contains(event, "VolumeUpdated") {
				found = true
			}
		default:
			assert.True(t, found, "expected a VolumeUpdated event")
			return
		}
	}
}

func TestControllerReconcileVolumeStateDryRun(t *testing.T) {
	ctx := context.Background()
	mockDriver := mock.New()

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "media-claim",
			Namespace: "media",
			UID:       "ddd-eee-fff",
			Annotations: map[string]string{
				config.AnnQuota:  "20Gi",
				config.AnnDryRun: "true",
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "pvc-ddd-eee-fff",
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}

	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pvc-ddd-eee-fff",
			Annotations: map[string]string{
				config.AnnProvisionedBy: "subvol.io/provisioner",
				config.AnnSubvolDriver:  "mock",
				config.AnnDatasetName:   "pool/k8s/media/media-claim",
			},
		},
		Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
	}

	client := fake.NewSimpleClientset(pvc, pv)
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

	ctrl.processPVC(ctx, pvc)

	calls := mockDriver.GetReconcileCalls()
	require.Len(t, calls, 3, "expected quota, owner/mode and properties reconcile calls")
	for _, call := range calls {
		assert.True(t, call.DryRun, "all reconcile calls must be dry-run")
	}
	quotaCall := &calls[0]
	require.NotNil(t, quotaCall.Quota)
	assert.Equal(t, int64(20*1024*1024*1024), *quotaCall.Quota)

	found := false
	for {
		select {
		case event := <-recorder.Events:
			if strings.Contains(event, "VolumeDryRun") {
				found = true
			}
		default:
			assert.True(t, found, "expected a VolumeDryRun event")
			return
		}
	}
}

func TestControllerReconcileVolumeStateUpToDate(t *testing.T) {
	ctx := context.Background()
	mockDriver := mock.New()
	// The dataset already matches the desired configuration (e.g. the config
	// was reverted after a dry run), so any previously pending plan is moot.
	mockDriver.SetUpToDate(true)

	scName := "zfs-mock"
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "media-claim",
			Namespace: "media",
			UID:       "ggg-hhh-iii",
			Annotations: map[string]string{
				config.AnnQuota: "20Gi",
				// A leftover one-shot apply annotation must still be consumed.
				config.AnnApply: "true",
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &scName,
			VolumeName:       "pvc-ggg-hhh-iii",
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}

	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pvc-ggg-hhh-iii",
			Annotations: map[string]string{
				config.AnnProvisionedBy: "subvol.io/provisioner",
				config.AnnSubvolDriver:  "mock",
				config.AnnDatasetName:   "pool/k8s/media/media-claim",
			},
		},
		Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
	}

	sc := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: "zfs-mock"},
		Provisioner: "subvol.io/provisioner",
		Parameters: map[string]string{
			config.ParamDriver: "mock",
		},
	}

	client := fake.NewSimpleClientset(sc, pvc, pv)
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

	ctrl.processPVC(ctx, pvc)

	// The one-shot apply annotation must be consumed even when there is
	// nothing to apply, so it never lingers as drift.
	updated, err := client.CoreV1().PersistentVolumeClaims("media").Get(ctx, "media-claim", metav1.GetOptions{})
	require.NoError(t, err)
	assert.NotContains(t, updated.Annotations, config.AnnApply, "apply annotation must be removed when no changes are pending")

	upToDate := false
	for {
		select {
		case event := <-recorder.Events:
			assert.NotContains(t, event, "VolumeDryRun", "no plan may be emitted when the dataset matches the desired config")
			if strings.Contains(event, "VolumeUpToDate") {
				upToDate = true
			}
		default:
			assert.True(t, upToDate, "expected a VolumeUpToDate event")
			return
		}
	}
}

func TestControllerKeyLocationSecret(t *testing.T) {
	ctx := context.Background()
	mockDriver := mock.New()

	sc := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "zfs-mock",
		},
		Provisioner: "subvol.io/provisioner",
		Parameters: map[string]string{
			config.ParamDriver:            "mock",
			config.ParamParent:            "pool/k8s",
			config.ParamMountPrefix:       "/mnt/pool/k8s",
			config.ParamPathTemplate:      "{{ .Namespace }}/{{ .PVC }}",
			config.ParamNode:              "nas-pc",
			config.ParamEncryption:        "keyformat=hex",
			config.ParamKeyLocationSecret: "default/zfs-keys#key-location",
		},
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "zfs-keys",
			Namespace: "default",
		},
		Data: map[string][]byte{
			"key-location": []byte("http://keys.home.lan:8080/token/pool\n"),
		},
	}

	scName := "zfs-mock"
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "enc-claim",
			Namespace: "media",
			UID:       "444-555-666",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &scName,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("10Gi"),
				},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Phase: corev1.ClaimPending,
		},
	}

	client := fake.NewSimpleClientset(sc, pvc, secret)

	ctrl, err := NewController(ControllerOptions{
		Client:          client,
		NodeName:        "nas-pc",
		ProvisionerName: "subvol.io/provisioner",
		Drivers: map[string]driver.Driver{
			"mock": mockDriver,
		},
	})
	require.NoError(t, err)

	ctrl.processPVC(ctx, pvc)

	opts, ok := mockDriver.GetCreateOptions("pool/k8s/media/enc-claim")
	require.True(t, ok, "expected mock driver create options")
	require.NotNil(t, opts.Encryption, "expected encryption config to be set")
	assert.True(t, opts.Encryption.Enabled)
	assert.Equal(t, "hex", opts.Encryption.KeyFormat)
	assert.Equal(t, "http://keys.home.lan:8080/token/pool", opts.Encryption.KeyLocation)
	assert.Empty(t, opts.Encryption.KeyData, "keylocation secret must not be passed as raw key data")
}
