package config

const (
	// DefaultProvisionerName is the default provisioner name registered in StorageClasses.
	DefaultProvisionerName = "subvol.io/provisioner"

	// Annotation Prefix
	AnnotationPrefix = "subvol.io/"

	// PVC Annotations
	AnnSubvolPath           = "subvol.io/subvol"                 // Custom subvolume/dataset name override (relative to parent)
	AnnOwner                = "subvol.io/owner"                  // "UID:GID" e.g. "1000:1000"
	AnnMode                 = "subvol.io/mode"                   // Directory permissions e.g. "0750"
	AnnKeyLocation          = "subvol.io/key-location"           // Encryption key location override (e.g. "http://vault:8200/..." or "file:///...")
	AnnSecretKeyRef         = "subvol.io/secret-key-ref"         // Reference to K8s secret containing key: "namespace/name#key"
	AnnSnapshotBeforeDelete = "subvol.io/snapshot-before-delete" // "true" to preserve the volume before deletion (rename or snapshot depending on backend)
	AnnAdoptExisting        = "subvol.io/adopt-existing"         // "true" to adopt an already-existing dataset without mutation
	AnnQuota                = "subvol.io/quota"                  // Live quota override, e.g. "10Gi" or "none"; overrides requests.storage unless it is the dummy value 1
	AnnProperties           = "subvol.io/properties"             // Live filesystem property overrides (multi-line key=val), overlaid on top of StorageClass properties
	AnnDryRun               = "subvol.io/dry-run"                // "true" to force dry-run (plan only) even when apply is otherwise authorized
	AnnApply                = "subvol.io/apply"                  // "true" to authorize applying the pending plan for this volume; removed by the provisioner after applying (one-shot)
	AnnSelectedNode         = "volume.kubernetes.io/selected-node"

	// PV Annotations / Labels added by provisioner
	AnnProvisionedBy = "pv.kubernetes.io/provisioned-by"
	AnnSubvolDriver  = "subvol.io/driver"
	AnnDatasetName   = "subvol.io/dataset"
	AnnHostMountPath = "subvol.io/mount-path"

	// StorageClass Parameter Keys
	ParamDriver            = "driver"            // "zfs", "btrfs", or "dir"
	ParamNode              = "node"              // Pinned node name (used with Immediate volumeBindingMode)
	ParamParent            = "parent"            // Parent dataset / subvolume path (e.g. "tank/k8s")
	ParamMountPrefix       = "mountPrefix"       // Base host mount directory prefix (e.g. "/mnt/tank/k8s")
	ParamPathTemplate      = "pathTemplate"      // Path template, default: "{{ .Namespace }}/{{ .PVC }}"
	ParamDefaultOwner      = "defaultOwner"      // Default "UID:GID" if not specified on PVC
	ParamDefaultMode       = "defaultMode"       // Default permission bits e.g. "0750"
	ParamAutoApply         = "autoApply"         // "true" to apply annotation-driven reconcile changes immediately instead of planning them (VolumeDryRun events) until subvol.io/apply is set
	ParamProperties        = "properties"        // Multi-line key=val or YAML of filesystem properties (e.g. compression, atime)
	ParamEncryption        = "encryption"        // Multi-line key=val or YAML of encryption settings (keyformat, keylocation)
	ParamKeyLocationSecret = "keylocationSecret" // K8s secret ref "namespace/name#key" containing the keylocation URL
)
