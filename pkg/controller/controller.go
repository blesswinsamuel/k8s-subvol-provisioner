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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	storagelisters "k8s.io/client-go/listers/storage/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
)

// ControllerOptions contains initialization parameters for the provisioner controller.
type ControllerOptions struct {
	Client          kubernetes.Interface
	NodeName        string
	ProvisionerName string
	HostPrefix      string
	Drivers         map[string]driver.Driver
	Recorder        record.EventRecorder
	// ResyncPeriod is the informer resync period, acting as a safety net for
	// missed events. Defaults to 10 minutes.
	ResyncPeriod time.Duration
	// Workers is the number of concurrent reconcile workers. Defaults to 4.
	Workers int
}

// Controller watches PVCs, PVs and StorageClasses with informers and
// reconciles them via an event-driven workqueue.
type Controller struct {
	client          kubernetes.Interface
	nodeName        string
	provisionerName string
	hostPrefix      string
	drivers         map[string]driver.Driver
	recorder        record.EventRecorder
	resyncPeriod    time.Duration
	workers         int

	pvcLister corelisters.PersistentVolumeClaimLister
	pvLister  corelisters.PersistentVolumeLister
	scLister  storagelisters.StorageClassLister

	queue workqueue.TypedRateLimitingInterface[reconcileKey]

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
		opts.ResyncPeriod = 10 * time.Minute
	}
	if opts.Workers <= 0 {
		opts.Workers = 4
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
		workers:         opts.Workers,
		inProgress:      make(map[string]bool),
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[reconcileKey](),
			workqueue.TypedRateLimitingQueueConfig[reconcileKey]{Name: "subvol-provisioner"},
		),
	}, nil
}

// RegisterDriver registers a filesystem driver with the controller.
func (c *Controller) RegisterDriver(d driver.Driver) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.drivers[d.Name()] = d
}

// reconcileKey identifies an object to reconcile: a PVC ("pvc") or PV ("pv").
type reconcileKey struct {
	kind      string
	namespace string
	name      string
}

func (k reconcileKey) String() string {
	if k.namespace != "" {
		return fmt.Sprintf("%s:%s/%s", k.kind, k.namespace, k.name)
	}
	return fmt.Sprintf("%s:%s", k.kind, k.name)
}

// Run starts informers and reconcile workers until ctx is canceled.
func (c *Controller) Run(ctx context.Context) error {
	log.Info().
		Str("nodeName", c.nodeName).
		Str("provisioner", c.provisionerName).
		Int("workers", c.workers).
		Dur("resyncPeriod", c.resyncPeriod).
		Msg("Starting k8s-subvol-provisioner controller")

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stopCh := ctx.Done()
	factory := informers.NewSharedInformerFactory(c.client, c.resyncPeriod)

	pvcInformer := factory.Core().V1().PersistentVolumeClaims().Informer()
	pvInformer := factory.Core().V1().PersistentVolumes().Informer()
	scInformer := factory.Storage().V1().StorageClasses().Informer()

	c.pvcLister = factory.Core().V1().PersistentVolumeClaims().Lister()
	c.pvLister = factory.Core().V1().PersistentVolumes().Lister()
	c.scLister = factory.Storage().V1().StorageClasses().Lister()
	hasSynced := []cache.InformerSynced{pvcInformer.HasSynced, pvInformer.HasSynced, scInformer.HasSynced}

	pvcInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.onPVCAdd,
		UpdateFunc: func(_, newObj any) { c.onPVCAdd(newObj) },
	})
	pvInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.onPVAdd,
		UpdateFunc: func(_, newObj any) { c.onPVAdd(newObj) },
	})
	scInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.onStorageClassAdd,
		UpdateFunc: func(_, newObj any) { c.onStorageClassAdd(newObj) },
	})

	factory.Start(stopCh)
	if !cache.WaitForCacheSync(stopCh, hasSynced...) {
		return fmt.Errorf("timed out waiting for informer caches to sync")
	}
	log.Info().Msg("Informer caches synced")

	var wg sync.WaitGroup
	for i := 0; i < c.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.worker(ctx)
		}()
	}

	<-ctx.Done()
	log.Info().Msg("Shutting down controller")
	c.queue.ShutDown()
	wg.Wait()
	return nil
}

func (c *Controller) onPVCAdd(obj any) {
	pvc, ok := obj.(*corev1.PersistentVolumeClaim)
	if !ok {
		return
	}
	if pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName == "" {
		return
	}
	c.queue.Add(reconcileKey{kind: "pvc", namespace: pvc.Namespace, name: pvc.Name})
}

func (c *Controller) onPVAdd(obj any) {
	pv, ok := obj.(*corev1.PersistentVolume)
	if !ok {
		return
	}
	if pv.Annotations[config.AnnProvisionedBy] != c.provisionerName {
		return
	}
	c.queue.Add(reconcileKey{kind: "pv", name: pv.Name})
}

// onStorageClassAdd re-enqueues PVCs referencing the StorageClass so that
// provisioner/node targeting changes take effect immediately.
func (c *Controller) onStorageClassAdd(obj any) {
	sc, ok := obj.(*storagev1.StorageClass)
	if !ok || c.pvcLister == nil {
		return
	}
	pvcs, err := c.pvcLister.List(labels.Everything())
	if err != nil {
		log.Error().Err(err).Str("storageClass", sc.Name).Msg("Failed to list PVCs for StorageClass event")
		return
	}
	for _, pvc := range pvcs {
		if pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName != sc.Name {
			continue
		}
		c.queue.Add(reconcileKey{kind: "pvc", namespace: pvc.Namespace, name: pvc.Name})
	}
}

func (c *Controller) worker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

func (c *Controller) processNextWorkItem(ctx context.Context) bool {
	key, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	defer c.queue.Done(key)

	err := c.reconcile(ctx, key)
	if err == nil {
		c.queue.Forget(key)
		return true
	}
	if c.queue.NumRequeues(key) < maxRetries {
		log.Error().Err(err).Str("key", key.String()).Msg("Reconcile failed, retrying")
		c.queue.AddRateLimited(key)
		return true
	}
	log.Error().Err(err).Str("key", key.String()).Int("maxRetries", maxRetries).Msg("Reconcile failed, giving up")
	c.queue.Forget(key)
	return true
}

// maxRetries is the maximum number of rate-limited retries per object before
// giving up and waiting for the next informer resync.
const maxRetries = 5

func (c *Controller) reconcile(ctx context.Context, key reconcileKey) error {
	switch key.kind {
	case "pvc":
		pvc, err := c.getPVC(ctx, key.namespace, key.name)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return fmt.Errorf("failed to get PVC %s: %w", key.String(), err)
		}
		return c.processPVC(ctx, pvc)
	case "pv":
		pv, err := c.getPV(ctx, key.name)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return fmt.Errorf("failed to get PV %s: %w", key.String(), err)
		}
		return c.processPV(ctx, pv)
	default:
		return fmt.Errorf("unknown reconcile kind %q", key.kind)
	}
}

// processPVC reconciles a single PVC: pending PVCs are provisioned, bound
// PVCs get their annotation-driven state reconciled onto the dataset.
func (c *Controller) processPVC(ctx context.Context, pvc *corev1.PersistentVolumeClaim) error {
	if pvc.Status.Phase == corev1.ClaimPending && pvc.Spec.VolumeName == "" {
		if pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName == "" {
			return nil
		}
		sc, err := c.getStorageClass(ctx, *pvc.Spec.StorageClassName)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return fmt.Errorf("failed to get StorageClass %q: %w", *pvc.Spec.StorageClassName, err)
		}
		if sc.Provisioner != c.provisionerName || !c.isNodeTargeted(sc, pvc) {
			return nil
		}
		return c.reconcileClaim(ctx, pvc, sc)
	}

	if pvc.Status.Phase == corev1.ClaimBound && pvc.Spec.VolumeName != "" {
		pv, err := c.getPV(ctx, pvc.Spec.VolumeName)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return fmt.Errorf("failed to get PV %q: %w", pvc.Spec.VolumeName, err)
		}
		if pv.Annotations[config.AnnProvisionedBy] != c.provisionerName || !c.isPVOnThisNode(pv) {
			return nil
		}
		return c.reconcileVolumeState(ctx, pvc, pv)
	}

	return nil
}

// processPV reconciles a single PV: Released PVs with the Delete reclaim
// policy provisioned by this provisioner are destroyed.
func (c *Controller) processPV(ctx context.Context, pv *corev1.PersistentVolume) error {
	if pv.Annotations[config.AnnProvisionedBy] != c.provisionerName {
		return nil
	}
	if pv.Status.Phase != corev1.VolumeReleased {
		return nil
	}
	if pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimDelete {
		return nil
	}
	if !c.isPVOnThisNode(pv) {
		return nil
	}
	return c.reconcileVolumeDelete(ctx, pv)
}

func (c *Controller) getPVC(ctx context.Context, namespace, name string) (*corev1.PersistentVolumeClaim, error) {
	if c.pvcLister != nil {
		return c.pvcLister.PersistentVolumeClaims(namespace).Get(name)
	}
	return c.client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{})
}

func (c *Controller) getPV(ctx context.Context, name string) (*corev1.PersistentVolume, error) {
	if c.pvLister != nil {
		return c.pvLister.Get(name)
	}
	return c.client.CoreV1().PersistentVolumes().Get(ctx, name, metav1.GetOptions{})
}

func (c *Controller) getStorageClass(ctx context.Context, name string) (*storagev1.StorageClass, error) {
	if c.scLister != nil {
		return c.scLister.Get(name)
	}
	return c.client.StorageV1().StorageClasses().Get(ctx, name, metav1.GetOptions{})
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

func (c *Controller) reconcileClaim(ctx context.Context, pvc *corev1.PersistentVolumeClaim, sc *storagev1.StorageClass) error {
	key := fmt.Sprintf("pvc:%s/%s", pvc.Namespace, pvc.Name)
	c.mu.Lock()
	if c.inProgress[key] {
		c.mu.Unlock()
		return nil
	}
	c.inProgress[key] = true
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.inProgress, key)
		c.mu.Unlock()
	}()

	// If a PV for this PVC already exists (e.g. a stale-cache re-run after a
	// previous successful provision), skip provisioning entirely to keep this
	// reconcile idempotent.
	pvName := fmt.Sprintf("pvc-%s", pvc.UID)
	if existingPV, err := c.getPV(ctx, pvName); err == nil &&
		existingPV.Spec.ClaimRef != nil && existingPV.Spec.ClaimRef.UID == pvc.UID {
		log.Info().Str("pvc", pvc.Name).Str("pv", pvName).Msg("PV already provisioned for PVC, skipping provisioning")
		return nil
	}

	driverName := sc.Parameters[config.ParamDriver]
	if driverName == "" {
		driverName = "zfs" // default
	}

	d, ok := c.drivers[driverName]
	if !ok {
		log.Error().Str("driver", driverName).Str("pvc", pvc.Name).Msg("Driver not found")
		c.emitEvent(pvc, corev1.EventTypeWarning, "DriverNotFound", fmt.Sprintf("Storage driver %q is not registered", driverName))
		return fmt.Errorf("storage driver %q is not registered", driverName)
	}

	if pvc.Spec.VolumeMode != nil && *pvc.Spec.VolumeMode == corev1.PersistentVolumeBlock {
		log.Error().Str("pvc", pvc.Name).Msg("Raw block volume mode is not supported")
		c.emitEvent(pvc, corev1.EventTypeWarning, "VolumeModeUnsupported", "Raw block volumes are not supported by this provisioner; use a filesystem-mode PVC")
		return fmt.Errorf("raw block volume mode is not supported for PVC %s/%s", pvc.Namespace, pvc.Name)
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
			return fmt.Errorf("failed to evaluate path template: %w", err)
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
	quotaPtr, err := resolveQuota(pvc)
	if err != nil {
		log.Error().Err(err).Str("pvc", pvc.Name).Msg("Failed to resolve quota")
		c.emitEvent(pvc, corev1.EventTypeWarning, "QuotaResolutionFailed", err.Error())
		return err
	}
	var quotaBytes int64
	if quotaPtr != nil {
		quotaBytes = *quotaPtr
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

	// Properties: StorageClass base, overlaid by the PVC annotation.
	properties := config.ParseKeyValueLines(sc.Parameters[config.ParamProperties])
	for k, v := range config.ParseKeyValueLines(pvc.Annotations[config.AnnProperties]) {
		properties[k] = v
	}

	// Encryption
	encConfig := &config.EncryptionConfig{}
	if rawEnc, ok := sc.Parameters[config.ParamEncryption]; ok && rawEnc != "" {
		encMap := config.ParseKeyValueLines(rawEnc)
		encConfig.Enabled = true
		encConfig.KeyFormat = encMap["keyformat"]
		encConfig.KeyLocation = encMap["keylocation"]
	}
	// StorageClass-level secret reference: resolves the keylocation URL from a K8s
	// secret ("namespace/name#key"), keeping tokenized key-server URLs out of manifests.
	if secretRef, ok := sc.Parameters[config.ParamKeyLocationSecret]; ok && secretRef != "" {
		data, err := c.loadSecretKey(ctx, pvc.Namespace, secretRef)
		if err != nil {
			log.Error().Err(err).Str("pvc", pvc.Name).Msg("Failed to load keylocation secret")
			c.emitEvent(pvc, corev1.EventTypeWarning, "SecretLoadFailed", err.Error())
			return err
		}
		if loc := strings.TrimSpace(string(data)); loc != "" {
			encConfig.Enabled = true
			encConfig.KeyLocation = loc
		}
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
			return err
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
		return err
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
		return err
	}

	createdPV, err := c.client.CoreV1().PersistentVolumes().Create(ctx, pv, metav1.CreateOptions{})
	if err != nil {
		// Another reconcile may have created the PV between our existence
		// check and this Create. Treat it as success only if it belongs to
		// this PVC.
		if apierrors.IsAlreadyExists(err) {
			existingPV, getErr := c.getPV(ctx, pv.Name)
			if getErr == nil && existingPV.Spec.ClaimRef != nil && existingPV.Spec.ClaimRef.UID == pvc.UID {
				log.Info().Str("pv", pv.Name).Str("pvc", pvc.Name).Msg("PV already created for PVC by concurrent reconcile")
				createdPV = existingPV
			} else {
				log.Error().Err(err).Str("pv", pv.Name).Msg("Failed to create PV in API")
				c.emitEvent(pvc, corev1.EventTypeWarning, "PVCreateFailed", err.Error())
				return err
			}
		} else {
			log.Error().Err(err).Str("pv", pv.Name).Msg("Failed to create PV in API")
			c.emitEvent(pvc, corev1.EventTypeWarning, "PVCreateFailed", err.Error())
			return err
		}
	}

	log.Info().Str("pv", createdPV.Name).Str("pvc", pvc.Name).Msg("Successfully provisioned and created PV")
	c.emitEvent(pvc, corev1.EventTypeNormal, "ProvisioningSucceeded", fmt.Sprintf("Successfully provisioned volume %s", createdPV.Name))
	return nil
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

func (c *Controller) reconcileVolumeDelete(ctx context.Context, pv *corev1.PersistentVolume) error {
	key := fmt.Sprintf("pv:%s", pv.Name)
	c.mu.Lock()
	if c.inProgress[key] {
		c.mu.Unlock()
		return nil
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
		return fmt.Errorf("driver %q not found for PV deletion", driverName)
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
		return err
	}

	// Delete the PV from Kubernetes. A NotFound means a concurrent reconcile
	// already deleted it, which is a successful outcome for this reconcile.
	if err := c.client.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, metav1.DeleteOptions{}); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info().Str("pv", pv.Name).Msg("PV object already deleted from K8s")
			return nil
		}
		log.Error().Err(err).Str("pv", pv.Name).Msg("Failed to delete PV object from K8s")
		c.emitEvent(pv, corev1.EventTypeWarning, "PVDeleteFailed", err.Error())
		return err
	}

	log.Info().Str("pv", pv.Name).Msg("Successfully deleted PV object and backend dataset")
	return nil
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

// dummyStorageRequest is the requests.storage value that signals "size is a
// placeholder, do not derive the quota from it". Use it together with the
// subvol.io/quota annotation (or with no quota at all).
const dummyStorageRequest = int64(1)

// resolveQuota resolves the quota for a PVC:
//  1. subvol.io/quota annotation (quantity, or "none"/"unlimited" for no quota)
//  2. requests.storage, unless it equals the dummy value 1 (unmanaged)
//
// A nil result means the dataset should have no quota.
func resolveQuota(pvc *corev1.PersistentVolumeClaim) (*int64, error) {
	if raw, ok := pvc.Annotations[config.AnnQuota]; ok {
		raw = strings.TrimSpace(raw)
		if raw == "none" || raw == "unlimited" {
			return nil, nil
		}
		q, err := resource.ParseQuantity(raw)
		if err != nil {
			return nil, fmt.Errorf("invalid %s annotation %q: %w", config.AnnQuota, raw, err)
		}
		v := q.Value()
		return &v, nil
	}
	if req, ok := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; ok && req.Value() != dummyStorageRequest {
		v := req.Value()
		return &v, nil
	}
	return nil, nil
}

func (c *Controller) reconcileVolumeState(ctx context.Context, pvc *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume) error {
	key := fmt.Sprintf("state:%s/%s", pvc.Namespace, pvc.Name)
	c.mu.Lock()
	if c.inProgress[key] {
		c.mu.Unlock()
		return nil
	}
	c.inProgress[key] = true
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.inProgress, key)
		c.mu.Unlock()
	}()

	d, ok := c.drivers[pv.Annotations[config.AnnSubvolDriver]]
	if !ok {
		return nil
	}
	rec, ok := d.(driver.Reconciler)
	if !ok {
		log.Debug().Str("driver", d.Name()).Msg("Driver does not support live reconciliation, skipping")
		return nil
	}

	datasetName := pv.Annotations[config.AnnDatasetName]
	if datasetName == "" {
		return nil
	}
	mountPath := pv.Annotations[config.AnnHostMountPath]

	// Apply policy: plan-only by default; apply when the StorageClass sets
	// autoApply or the PVC carries the one-shot subvol.io/apply annotation.
	// subvol.io/dry-run always forces plan-only.
	var sc *storagev1.StorageClass
	if scName := pvc.Spec.StorageClassName; scName != nil && *scName != "" {
		if got, err := c.client.StorageV1().StorageClasses().Get(ctx, *scName, metav1.GetOptions{}); err == nil {
			sc = got
		}
	}
	applyRequested := pvc.Annotations[config.AnnApply] == "true"
	dryRun := pvc.Annotations[config.AnnDryRun] == "true"
	autoApply := sc != nil && sc.Parameters[config.ParamAutoApply] == "true"
	apply := (autoApply || applyRequested) && !dryRun
	runDryRun := !apply

	var changes []string
	fail := func(err error, what string) {
		log.Error().Err(err).Str("pvc", pvc.Name).Str("dataset", datasetName).Msgf("Failed to reconcile volume state: %s", what)
		c.emitEvent(pvc, corev1.EventTypeWarning, "VolumeUpdateFailed", fmt.Sprintf("%s: %v", what, err))
	}

	// Quota
	quota, err := resolveQuota(pvc)
	if err != nil {
		fail(err, "resolve quota")
		return err
	}
	qc, err := rec.ReconcileQuota(ctx, datasetName, quota, runDryRun)
	if err != nil {
		fail(err, "quota")
	} else {
		changes = append(changes, qc...)
	}

	// Owner/mode: only PVC annotations are reconciled. StorageClass
	// defaultOwner/defaultMode are provision-time defaults — enforcing them
	// live would chown/chmod historical datasets that never declared them.
	owner, _ := config.ParseOwnership(pvc.Annotations[config.AnnOwner])
	mode, _ := config.ParseFileMode(pvc.Annotations[config.AnnMode])
	oc, err := rec.ReconcileOwnerMode(ctx, driver.OwnerModeOptions{
		Name:       datasetName,
		MountPath:  mountPath,
		HostPrefix: c.hostPrefix,
		Owner:      owner,
		Mode:       mode,
		Recursive:  pvc.Annotations[config.AnnRecursive] == "true",
		DryRun:     runDryRun,
	})
	if err != nil {
		fail(err, "owner/mode")
	} else {
		changes = append(changes, oc...)
	}

	// Properties: StorageClass base, overlaid by the PVC annotation.
	props := map[string]string{}
	if sc != nil {
		for k, v := range config.ParseKeyValueLines(sc.Parameters[config.ParamProperties]) {
			props[k] = v
		}
	}
	for k, v := range config.ParseKeyValueLines(pvc.Annotations[config.AnnProperties]) {
		props[k] = v
	}
	pc, err := rec.ReconcileProperties(ctx, datasetName, props, runDryRun)
	if err != nil {
		fail(err, "properties")
	} else {
		changes = append(changes, pc...)
	}

	if len(changes) == 0 {
		// No drift: emit VolumeUpToDate so consumers (e.g. the kubeploy UI)
		// can retire a previously pending VolumeDryRun plan that is no longer
		// applicable — events are append-only and cannot be retracted.
		log.Debug().Str("pvc", pvc.Name).Str("dataset", datasetName).Msg("Volume state is up to date")
		c.emitEvent(pvc, corev1.EventTypeNormal, "VolumeUpToDate", "No pending changes: dataset matches the desired configuration")
		if applyRequested {
			// The authorized apply found nothing to do; still consume the
			// one-shot annotation so it never lingers as drift.
			patch := fmt.Sprintf(`{"metadata":{"annotations":{%q:null}}}`, config.AnnApply)
			if _, err := c.client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Patch(ctx, pvc.Name, types.StrategicMergePatchType, []byte(patch), metav1.PatchOptions{}); err != nil {
				log.Error().Err(err).Str("pvc", pvc.Name).Msg("Failed to remove apply annotation")
				c.emitEvent(pvc, corev1.EventTypeWarning, "ApplyAnnotationCleanupFailed", fmt.Sprintf("No changes to apply but failed to remove %s annotation: %v", config.AnnApply, err))
				return err
			}
		}
		return nil
	}
	summary := strings.Join(changes, ", ")
	if !apply {
		log.Info().Str("pvc", pvc.Name).Str("dataset", datasetName).Str("changes", summary).Msg("DRY RUN: planned volume changes (annotate with subvol.io/apply to authorize)")
		c.emitEvent(pvc, corev1.EventTypeNormal, "VolumeDryRun", fmt.Sprintf("Dry run: would apply: %s (annotate with %s to apply)", summary, config.AnnApply))
		return nil
	}
	log.Info().Str("pvc", pvc.Name).Str("dataset", datasetName).Str("changes", summary).Msg("Reconciled volume state")
	c.emitEvent(pvc, corev1.EventTypeNormal, "VolumeUpdated", fmt.Sprintf("Applied: %s", summary))

	// One-shot semantics: the apply annotation authorizes exactly one apply pass.
	// Remove it so the volume is ready for the next change and the annotation
	// never lingers as drift against the declarative rendered state.
	if applyRequested {
		patch := fmt.Sprintf(`{"metadata":{"annotations":{%q:null}}}`, config.AnnApply)
		if _, err := c.client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Patch(ctx, pvc.Name, types.StrategicMergePatchType, []byte(patch), metav1.PatchOptions{}); err != nil {
			log.Error().Err(err).Str("pvc", pvc.Name).Msg("Failed to remove apply annotation")
			c.emitEvent(pvc, corev1.EventTypeWarning, "ApplyAnnotationCleanupFailed", fmt.Sprintf("Applied changes but failed to remove %s annotation: %v", config.AnnApply, err))
			return err
		}
	}
	return nil
}
