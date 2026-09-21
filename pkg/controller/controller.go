package controller

import (
	"context"
	"fmt"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/blesswinsamuel/k8s-subvol-provisioner/pkg/apis/config"
	"github.com/blesswinsamuel/k8s-subvol-provisioner/pkg/driver"
	"github.com/rs/zerolog/log"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
)

// ControllerOptions contains initialization parameters for the provisioner controller.
type ControllerOptions struct {
	Client          kubernetes.Interface
	NodeName        string
	ProvisionerName string
	HostPrefix      string
	Drivers         map[string]driver.Driver
	Recorder        record.EventRecorder
	ResyncPeriod    time.Duration
}

// Controller runs the reconciliation loops for PVC provisioning and PV deletion.
type Controller struct {
	client          kubernetes.Interface
	nodeName        string
	provisionerName string
	hostPrefix      string
	drivers         map[string]driver.Driver
	recorder        record.EventRecorder
	resyncPeriod    time.Duration

	mu         sync.Mutex
	inProgress map[string]bool
}

// NewController creates a new instance of Controller.
func NewController(opts ControllerOptions) (*Controller, error) {
	if opts.Client == nil {
		return nil, fmt.Errorf("kubernetes client cannot be nil")
	}
	if opts.ProvisionerName == "" {
		opts.ProvisionerName = config.DefaultProvisionerName
	}
	if opts.ResyncPeriod == 0 {
		opts.ResyncPeriod = 15 * time.Second
	}
	if opts.Drivers == nil {
		opts.Drivers = make(map[string]driver.Driver)
	}

	return &Controller{
		client:          opts.Client,
		nodeName:        opts.NodeName,
		provisionerName: opts.ProvisionerName,
		hostPrefix:      opts.HostPrefix,
		drivers:         opts.Drivers,
		recorder:        opts.Recorder,
		resyncPeriod:    opts.ResyncPeriod,
		inProgress:      make(map[string]bool),
	}, nil
}

// RegisterDriver registers a filesystem driver with the controller.
func (c *Controller) RegisterDriver(d driver.Driver) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.drivers[d.Name()] = d
}

// Run starts the controller reconciliation loop until ctx is canceled.
func (c *Controller) Run(ctx context.Context) error {
	log.Info().
		Str("nodeName", c.nodeName).
		Str("provisioner", c.provisionerName).
		Msg("Starting k8s-subvol-provisioner controller")

	ticker := time.NewTicker(c.resyncPeriod)
	defer ticker.Stop()

	// Initial reconcile
	c.reconcileAll(ctx)

	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("Shutting down controller")
			return nil
		case <-ticker.C:
			c.reconcileAll(ctx)
		}
	}
}

func (c *Controller) reconcileAll(ctx context.Context) {
	c.reconcileClaims(ctx)
	c.reconcileVolumes(ctx)
}

// ReconcileClaims finds Pending PVCs targeted for this provisioner and node, and provisions them.
func (c *Controller) reconcileClaims(ctx context.Context) {
	pvcs, err := c.client.CoreV1().PersistentVolumeClaims("").List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Error().Err(err).Msg("Failed to list PVCs")
		return
	}

	for i := range pvcs.Items {
		pvc := &pvcs.Items[i]
		if pvc.Status.Phase != corev1.ClaimPending || pvc.Spec.VolumeName != "" {
			continue
		}
		if pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName == "" {
			continue
		}

		sc, err := c.client.StorageV1().StorageClasses().Get(ctx, *pvc.Spec.StorageClassName, metav1.GetOptions{})
		if err != nil {
			continue
		}

		if sc.Provisioner != c.provisionerName {
			continue
		}

		if !c.isNodeTargeted(sc, pvc) {
			continue
		}

		c.reconcileClaim(ctx, pvc, sc)
	}
}

// isNodeTargeted checks if the PVC is meant to be provisioned on this controller's node.
func (c *Controller) isNodeTargeted(sc *storagev1.StorageClass, pvc *corev1.PersistentVolumeClaim) bool {
	// If controller has no node restriction set (e.g. running centralized or in test), proceed
	if c.nodeName == "" {
		return true
	}

	// 1. Check volume.kubernetes.io/selected-node annotation (set by scheduler in WaitForFirstConsumer mode)
	if selectedNode, ok := pvc.Annotations[config.AnnSelectedNode]; ok && selectedNode != "" {
		return selectedNode == c.nodeName
	}

	// 2. Check StorageClass "node" parameter (for Immediate binding mode)
	if scNode, ok := sc.Parameters[config.ParamNode]; ok && scNode != "" {
		return scNode == c.nodeName
	}

	// If no node specified anywhere and binding mode is WaitForFirstConsumer, wait for scheduler
	if sc.VolumeBindingMode != nil && *sc.VolumeBindingMode == storagev1.VolumeBindingWaitForFirstConsumer {
		return false
	}

	// Immediate binding with no explicit node defaults to true if nodeName is empty or match
	return true
}

func (c *Controller) reconcileClaim(ctx context.Context, pvc *corev1.PersistentVolumeClaim, sc *storagev1.StorageClass) {
	key := fmt.Sprintf("pvc:%s/%s", pvc.Namespace, pvc.Name)
	c.mu.Lock()
	if c.inProgress[key] {
		c.mu.Unlock()
		return
	}
	c.inProgress[key] = true
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.inProgress, key)
		c.mu.Unlock()
	}()

	driverName := sc.Parameters[config.ParamDriver]
	if driverName == "" {
		driverName = "zfs" // default
	}

	d, ok := c.drivers[driverName]
	if !ok {
		log.Error().Str("driver", driverName).Str("pvc", pvc.Name).Msg("Driver not found")
		c.emitEvent(pvc, corev1.EventTypeWarning, "DriverNotFound", fmt.Sprintf("Storage driver %q is not registered", driverName))
		return
	}

	// Calculate subvolume/dataset path
	relPath := pvc.Annotations[config.AnnSubvolPath]
	if relPath == "" {
		tmplCtx := ExtractTemplateContext(pvc, sc.Name)
		var err error
		relPath, err = EvaluatePathTemplate(sc.Parameters[config.ParamPathTemplate], tmplCtx)
		if err != nil {
			log.Error().Err(err).Str("pvc", pvc.Name).Msg("Failed to evaluate path template")
			c.emitEvent(pvc, corev1.EventTypeWarning, "TemplateError", err.Error())
			return
		}
	}

	parent := strings.Trim(sc.Parameters[config.ParamParent], "/")
	datasetName := relPath
	if parent != "" {
		datasetName = fmt.Sprintf("%s/%s", parent, relPath)
	}

	// Calculate host mount path
	mountPrefix := strings.TrimSuffix(sc.Parameters[config.ParamMountPrefix], "/")
	var mountPath string
	if mountPrefix != "" {
		mountPath = path.Join(mountPrefix, relPath)
	}

	// Quota
	var quotaBytes int64
	if req, ok := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; ok {
		quotaBytes = req.Value()
	}

	// Ownership and Mode
	ownerStr := pvc.Annotations[config.AnnOwner]
	if ownerStr == "" {
		ownerStr = sc.Parameters[config.ParamDefaultOwner]
	}
	owner, _ := config.ParseOwnership(ownerStr)

	modeStr := pvc.Annotations[config.AnnMode]
	if modeStr == "" {
		modeStr = sc.Parameters[config.ParamDefaultMode]
	}
	mode, _ := config.ParseFileMode(modeStr)

	// Properties
	properties := config.ParseKeyValueLines(sc.Parameters[config.ParamProperties])

	// Encryption
	encConfig := &config.EncryptionConfig{}
	if rawEnc, ok := sc.Parameters[config.ParamEncryption]; ok && rawEnc != "" {
		encMap := config.ParseKeyValueLines(rawEnc)
		encConfig.Enabled = true
		encConfig.KeyFormat = encMap["keyformat"]
		encConfig.KeyLocation = encMap["keylocation"]
	}
	if loc, ok := pvc.Annotations[config.AnnKeyLocation]; ok && loc != "" {
		encConfig.Enabled = true
		encConfig.KeyLocation = loc
	}
	if secretRef, ok := pvc.Annotations[config.AnnSecretKeyRef]; ok && secretRef != "" {
		keyData, err := c.loadSecretKey(ctx, pvc.Namespace, secretRef)
		if err != nil {
			log.Error().Err(err).Str("pvc", pvc.Name).Msg("Failed to load encryption secret")
			c.emitEvent(pvc, corev1.EventTypeWarning, "SecretLoadFailed", err.Error())
			return
		}
		encConfig.Enabled = true
		encConfig.KeyData = keyData
	}

	adoptExisting := pvc.Annotations[config.AnnAdoptExisting] == "true"

	createOpts := driver.CreateOptions{
		AdoptExisting: adoptExisting,
		Name:          datasetName,
		MountPath:     mountPath,
		HostPrefix:    c.hostPrefix,
		QuotaBytes:    quotaBytes,
		Owner:         owner,
		Mode:          mode,
		Properties:    properties,
		Encryption:    encConfig,
	}

	log.Info().Str("pvc", pvc.Name).Str("dataset", datasetName).Str("mountPath", mountPath).Msg("Provisioning volume")
	volInfo, err := d.Create(ctx, createOpts)
	if err != nil {
		log.Error().Err(err).Str("pvc", pvc.Name).Msg("Failed to provision volume")
		c.emitEvent(pvc, corev1.EventTypeWarning, "ProvisioningFailed", err.Error())
		return
	}

	if adoptExisting {
		log.Info().Str("pvc", pvc.Name).Str("dataset", volInfo.Name).Msg("Adopted existing dataset")
		c.emitEvent(pvc, corev1.EventTypeNormal, "VolumeAdopted", fmt.Sprintf("Adopted existing dataset %q without modification", volInfo.Name))
	}

	if volInfo.MountPath != "" {
		mountPath = volInfo.MountPath
	}

	nodeName := c.nodeName
	if nodeName == "" {
		if sel, ok := pvc.Annotations[config.AnnSelectedNode]; ok && sel != "" {
			nodeName = sel
		} else if scNode, ok := sc.Parameters[config.ParamNode]; ok && scNode != "" {
			nodeName = scNode
		}
	}

	// Build and create PV
	pv, err := BuildPersistentVolume(BuildPVOptions{
		PVC:             pvc,
		StorageClass:    sc,
		ProvisionerName: c.provisionerName,
		DriverName:      driverName,
		DatasetName:     volInfo.Name,
		MountPath:       mountPath,
		NodeName:        nodeName,
	})
	if err != nil {
		log.Error().Err(err).Msg("Failed to build PV object")
		c.emitEvent(pvc, corev1.EventTypeWarning, "PVBuildFailed", err.Error())
		return
	}

	createdPV, err := c.client.CoreV1().PersistentVolumes().Create(ctx, pv, metav1.CreateOptions{})
	if err != nil {
		log.Error().Err(err).Str("pv", pv.Name).Msg("Failed to create PV in API")
		c.emitEvent(pvc, corev1.EventTypeWarning, "PVCreateFailed", err.Error())
		return
	}

	log.Info().Str("pv", createdPV.Name).Str("pvc", pvc.Name).Msg("Successfully provisioned and created PV")
	c.emitEvent(pvc, corev1.EventTypeNormal, "ProvisioningSucceeded", fmt.Sprintf("Successfully provisioned volume %s", createdPV.Name))
}

// ReconcileVolumes finds Released PVs with Delete policy provisioned by this provisioner, and destroys them.
func (c *Controller) reconcileVolumes(ctx context.Context) {
	pvs, err := c.client.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Error().Err(err).Msg("Failed to list PVs")
		return
	}

	for i := range pvs.Items {
		pv := &pvs.Items[i]
		if pv.Annotations[config.AnnProvisionedBy] != c.provisionerName {
			continue
		}
		if pv.Status.Phase != corev1.VolumeReleased {
			continue
		}
		if pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimDelete {
			continue
		}

		if !c.isPVOnThisNode(pv) {
			continue
		}

		c.reconcileVolumeDelete(ctx, pv)
	}
}

func (c *Controller) isPVOnThisNode(pv *corev1.PersistentVolume) bool {
	if c.nodeName == "" {
		return true
	}
	if pv.Spec.NodeAffinity == nil || pv.Spec.NodeAffinity.Required == nil {
		return true
	}
	for _, term := range pv.Spec.NodeAffinity.Required.NodeSelectorTerms {
		for _, expr := range term.MatchExpressions {
			if expr.Key == "kubernetes.io/hostname" {
				for _, val := range expr.Values {
					if val == c.nodeName {
						return true
					}
				}
			}
		}
	}
	return false
}

func (c *Controller) reconcileVolumeDelete(ctx context.Context, pv *corev1.PersistentVolume) {
	key := fmt.Sprintf("pv:%s", pv.Name)
	c.mu.Lock()
	if c.inProgress[key] {
		c.mu.Unlock()
		return
	}
	c.inProgress[key] = true
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.inProgress, key)
		c.mu.Unlock()
	}()

	driverName := pv.Annotations[config.AnnSubvolDriver]
	d, ok := c.drivers[driverName]
	if !ok {
		log.Error().Str("driver", driverName).Str("pv", pv.Name).Msg("Driver not found for PV deletion")
		return
	}

	datasetName := pv.Annotations[config.AnnDatasetName]
	mountPath := pv.Annotations[config.AnnHostMountPath]
	snapshotBeforeDelete := pv.Annotations[config.AnnSnapshotBeforeDelete] == "true"

	log.Info().Str("pv", pv.Name).Str("dataset", datasetName).Msg("Deleting volume backend")
	err := d.Delete(ctx, driver.DeleteOptions{
		Name:                 datasetName,
		MountPath:            mountPath,
		HostPrefix:           c.hostPrefix,
		SnapshotBeforeDelete: snapshotBeforeDelete,
	})
	if err != nil {
		log.Error().Err(err).Str("pv", pv.Name).Msg("Failed to delete volume in backend")
		c.emitEvent(pv, corev1.EventTypeWarning, "DeletingFailed", err.Error())
		return
	}

	// Delete the PV from Kubernetes
	if err := c.client.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, metav1.DeleteOptions{}); err != nil {
		log.Error().Err(err).Str("pv", pv.Name).Msg("Failed to delete PV object from K8s")
		c.emitEvent(pv, corev1.EventTypeWarning, "PVDeleteFailed", err.Error())
		return
	}

	log.Info().Str("pv", pv.Name).Msg("Successfully deleted PV object and backend dataset")
}

// loadSecretKey parses "namespace/name#key" and fetches the key from K8s Secret.
func (c *Controller) loadSecretKey(ctx context.Context, defaultNamespace, ref string) ([]byte, error) {
	parts := strings.Split(ref, "#")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid secret reference %q: expected [namespace/]name#key", ref)
	}

	namePart, key := parts[0], parts[1]
	ns := defaultNamespace
	name := namePart
	if strings.Contains(namePart, "/") {
		sub := strings.SplitN(namePart, "/", 2)
		ns, name = sub[0], sub[1]
	}

	secret, err := c.client.CoreV1().Secrets(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get secret %s/%s: %w", ns, name, err)
	}

	val, ok := secret.Data[key]
	if !ok {
		return nil, fmt.Errorf("key %q not found in secret %s/%s", key, ns, name)
	}

	return val, nil
}

func (c *Controller) emitEvent(obj runtime.Object, eventType, reason, message string) {
	if c.recorder != nil && obj != nil {
		c.recorder.Event(obj, eventType, reason, message)
	}
}
