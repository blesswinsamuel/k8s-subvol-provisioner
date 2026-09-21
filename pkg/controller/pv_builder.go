package controller

import (
	"fmt"

	"github.com/blesswinsamuel/k8s-subvol-provisioner/pkg/apis/config"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// BuildPVOptions contains the arguments needed to construct a PersistentVolume.
type BuildPVOptions struct {
	PVC             *corev1.PersistentVolumeClaim
	StorageClass    *storagev1.StorageClass
	ProvisionerName string
	DriverName      string
	DatasetName     string
	MountPath       string
	NodeName        string
}

// BuildPersistentVolume constructs a Kubernetes local PersistentVolume object.
func BuildPersistentVolume(opts BuildPVOptions) (*corev1.PersistentVolume, error) {
	if opts.PVC == nil {
		return nil, fmt.Errorf("pvc cannot be nil")
	}
	if opts.NodeName == "" {
		return nil, fmt.Errorf("nodeName cannot be empty")
	}
	if opts.MountPath == "" {
		return nil, fmt.Errorf("mountPath cannot be empty")
	}

	pvName := fmt.Sprintf("pvc-%s", opts.PVC.UID)

	reclaimPolicy := corev1.PersistentVolumeReclaimRetain
	if opts.StorageClass != nil && opts.StorageClass.ReclaimPolicy != nil {
		reclaimPolicy = *opts.StorageClass.ReclaimPolicy
	}

	storageClass := ""
	if opts.PVC.Spec.StorageClassName != nil {
		storageClass = *opts.PVC.Spec.StorageClassName
	}

	capacity := opts.PVC.Spec.Resources.Requests

	accessModes := opts.PVC.Spec.AccessModes
	if len(accessModes) == 0 {
		accessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
	}

	volumeMode := corev1.PersistentVolumeFilesystem
	if opts.PVC.Spec.VolumeMode != nil {
		volumeMode = *opts.PVC.Spec.VolumeMode
	}

	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: pvName,
			Annotations: map[string]string{
				config.AnnProvisionedBy: opts.ProvisionerName,
				config.AnnSubvolDriver:  opts.DriverName,
				config.AnnDatasetName:   opts.DatasetName,
				config.AnnHostMountPath: opts.MountPath,
			},
		},
		Spec: corev1.PersistentVolumeSpec{
			Capacity:                      capacity,
			AccessModes:                   accessModes,
			PersistentVolumeReclaimPolicy: reclaimPolicy,
			StorageClassName:              storageClass,
			VolumeMode:                    &volumeMode,
			ClaimRef: &corev1.ObjectReference{
				Kind:            "PersistentVolumeClaim",
				APIVersion:      "v1",
				Namespace:       opts.PVC.Namespace,
				Name:            opts.PVC.Name,
				UID:             opts.PVC.UID,
				ResourceVersion: opts.PVC.ResourceVersion,
			},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				Local: &corev1.LocalVolumeSource{
					Path: opts.MountPath,
				},
			},
			NodeAffinity: &corev1.VolumeNodeAffinity{
				Required: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{
						{
							MatchExpressions: []corev1.NodeSelectorRequirement{
								{
									Key:      "kubernetes.io/hostname",
									Operator: corev1.NodeSelectorOpIn,
									Values:   []string{opts.NodeName},
								},
							},
						},
					},
				},
			},
		},
	}

	return pv, nil
}
