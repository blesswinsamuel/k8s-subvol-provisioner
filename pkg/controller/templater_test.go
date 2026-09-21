package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestParseStatefulSetPVC(t *testing.T) {
	tests := []struct {
		pvcName       string
		wantClaimName string
		wantOwner     string
		wantIndex     string
		wantOk        bool
	}{
		{
			pvcName:       "data-redis-0",
			wantClaimName: "data",
			wantOwner:     "redis",
			wantIndex:     "0",
			wantOk:        true,
		},
		{
			pvcName:       "storage-my-long-app-cluster-2",
			wantClaimName: "storage",
			wantOwner:     "my-long-app-cluster",
			wantIndex:     "2",
			wantOk:        true,
		},
		{
			pvcName: "single-pvc",
			wantOk:  false,
		},
		{
			pvcName: "standalone-pod-abc",
			wantOk:  false,
		},
	}

	for _, tt := range tests {
		claimName, owner, idx, ok := ParseStatefulSetPVC(tt.pvcName)
		assert.Equal(t, tt.wantOk, ok, "pvc: %s", tt.pvcName)
		if tt.wantOk {
			assert.Equal(t, tt.wantClaimName, claimName)
			assert.Equal(t, tt.wantOwner, owner)
			assert.Equal(t, tt.wantIndex, idx)
		}
	}
}

func TestEvaluatePathTemplate(t *testing.T) {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "data-redis-0",
			Namespace: "databases",
		},
	}
	ctx := ExtractTemplateContext(pvc, "zfs-fast")

	// Default template
	res, err := EvaluatePathTemplate("", ctx)
	require.NoError(t, err)
	assert.Equal(t, "databases/data-redis-0", res)

	// Custom template with index and owner
	customTmpl := "{{ .Namespace }}/{{ if .Index }}{{ .Owner }}/{{ .Index }}{{ else }}{{ .PVC }}{{ end }}"
	res, err = EvaluatePathTemplate(customTmpl, ctx)
	require.NoError(t, err)
	assert.Equal(t, "databases/redis/0", res)

	// Non-statefulset PVC
	pvcNonSts := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vault-file-storage",
			Namespace: "security",
		},
	}
	ctxNonSts := ExtractTemplateContext(pvcNonSts, "zfs-fast")
	res, err = EvaluatePathTemplate(customTmpl, ctxNonSts)
	require.NoError(t, err)
	assert.Equal(t, "security/vault-file-storage", res)

	// Malformed / dangerous path rejection
	_, err = EvaluatePathTemplate("{{ .Namespace }}/../../root", ctx)
	require.Error(t, err)
}
