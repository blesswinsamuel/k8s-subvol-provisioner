# k8s-subvol-provisioner

[![Test](https://github.com/blesswinsamuel/k8s-subvol-provisioner/actions/workflows/test.yml/badge.svg)](https://github.com/blesswinsamuel/k8s-subvol-provisioner/actions/workflows/test.yml)
[![Publish Container Image](https://github.com/blesswinsamuel/k8s-subvol-provisioner/actions/workflows/docker-publish.yml/badge.svg)](https://github.com/blesswinsamuel/k8s-subvol-provisioner/actions/workflows/docker-publish.yml)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Go Version](https://img.shields.io/github/go-mod/go-version/blesswinsamuel/k8s-subvol-provisioner)](https://go.dev)

`k8s-subvol-provisioner` is a lightweight, Kubernetes-native dynamic volume provisioner designed to provision native filesystem **datasets** (ZFS), **subvolumes** (Btrfs), and **directories** (any host filesystem, e.g. ext4, XFS) as `local` PersistentVolumes.

Unlike traditional local path provisioners that run ephemeral helper pods, `k8s-subvol-provisioner` runs directly within a DaemonSet:
- **Hard Quotas**: Real ZFS `quota` and Btrfs `qgroups`.
- **Subvolume & Directory Isolation**: Dedicated datasets/subvolumes/directories with pre-deletion safety backups.
- **Hierarchical Path Templating**: Clean organization for namespaces, PVC names, and StatefulSet replica indices (`{{ .Namespace }}/{{ .Owner }}/{{ .Index }}`).
- **Native Encryption**: Transparent ZFS encryption via HTTP/HTTPS key endpoints or Kubernetes Secrets.
- **POSIX Permissions**: Declarative UID:GID ownership and permission mode bits (`0750`).
- **Sub-millisecond Local Execution**: Runs as a DaemonSet directly on storage-capable nodes—no ephemeral helper pod churn or scheduler delays.

---

## Architecture

```text
  [ StorageClass ]           [ PersistentVolumeClaim ]
         |                              |
         +------------------------------+
                        |
            [ k8s-subvol-provisioner ] (DaemonSet on node)
                        |
           +------------+------------+--------------------+
           |                         |                    |
     [ ZFS Driver ]           [ Btrfs Driver ]      [ Dir Driver ]
   (zfs create -o ...)      (btrfs subvol create)    (mkdir -p)
           |                         |                    |
           +------------+------------+--------------------+
                        v
   [ Local PersistentVolume (spec.local + nodeAffinity) ]
```

When a `PersistentVolumeClaim` is submitted:
1. If using `volumeBindingMode: Immediate`: The StorageClass specifies the target node via `parameters.node: <node-name>`.
2. If using `volumeBindingMode: WaitForFirstConsumer`: The Kubernetes scheduler chooses the node for the scheduling pod and annotates the PVC with `volume.kubernetes.io/selected-node: <node-name>`.
3. The `k8s-subvol-provisioner` pod running on that node detects the claim, creates the dataset or subvolume on the local pool, applies quotas and ownership, and creates a bound `PersistentVolume` with `local: { path: mountPath }` and `spec.nodeAffinity`.

---

## Configuration

### StorageClass Parameters

| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `driver` | string | `zfs` | Filesystem driver (`zfs`, `btrfs`, or `dir`). |
| `node` | string | `""` | Target node name (required when using `Immediate` binding mode). |
| `parent` | string | `""` | Parent dataset path (e.g. `tank/k8s`). |
| `mountPrefix` | string | `""` | Host mount root where the datasets are mounted (e.g. `/mnt/tank/k8s`). |
| `pathTemplate` | string | `{{ .Namespace }}/{{ .PVC }}` | Path hierarchy template. |
| `defaultOwner` | string | `""` | Default `UID:GID` (e.g. `1000:1000`). |
| `defaultMode` | string | `""` | Default octal permissions (e.g. `0750`). |
| `properties` | multiline | `""` | Key-value lines of filesystem properties (e.g. `compression=zstd`). |
| `encryption` | multiline | `""` | Key-value lines of encryption options (`keyformat`, `keylocation`). |
| `keylocationSecret` | string | `""` | K8s secret ref `namespace/name#key` whose value is the `keylocation` URL (resolved at provision time; overridden by the `subvol.io/key-location` PVC annotation). |

### PVC Annotations

| Annotation | Description |
| :--- | :--- |
| `subvol.io/subvol` | Overrides the dataset/subvolume relative path instead of evaluating `pathTemplate`. |
| `subvol.io/owner` | Explicit `UID:GID` ownership (e.g. `1000:1000`). |
| `subvol.io/mode` | Explicit permission mode bits (e.g. `0770`). |
| `subvol.io/key-location` | HTTP/HTTPS or local key location for ZFS encryption. |
| `subvol.io/secret-key-ref` | Reference to a Kubernetes secret containing the key (`namespace/secret#key`). |
| `subvol.io/snapshot-before-delete` | When set to `"true"` and reclaim policy is `Delete`, creates a pre-deletion snapshot before destroying. |
| `subvol.io/adopt-existing` | When set to `"true"`, adopts an already-existing dataset/subvolume as-is (no create, quota, ownership, or property mutations) and binds a new PV to it. Without this annotation, an existing dataset is an error. |

---

## StatefulSet Path Templating

For StatefulSets using `volumeClaimTemplates` (which produce PVC names such as `data-redis-0`), the template context provides:

- `{{ .Namespace }}`: Namespace of the PVC
- `{{ .PVC }}`: Name of the PVC
- `{{ .ClaimName }}`: Extracted volume claim template name (e.g. `data`)
- `{{ .Owner }}`: Extracted StatefulSet workload name (e.g. `redis`)
- `{{ .Index }}`: Extracted replica index (e.g. `0`, `1`)

#### Example Template:
```yaml
parameters:
  pathTemplate: "{{ .Namespace }}/{{ if .Index }}{{ .Owner }}/{{ .Index }}{{ else }}{{ .PVC }}{{ end }}"
```
This organizes storage into:
- `/mnt/tank/k8s/databases/redis/0`
- `/mnt/tank/k8s/databases/redis/1`
- `/mnt/tank/k8s/databases/postgres/0`

---

## Example Usage

### 1. ZFS StorageClass
```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: zfs-fast
provisioner: subvol.io/provisioner
volumeBindingMode: Immediate
reclaimPolicy: Retain
parameters:
  driver: zfs
  node: nas-pc
  parent: tank/k8s
  mountPrefix: /mnt/tank/k8s
  pathTemplate: "{{ .Namespace }}/{{ if .Index }}{{ .Owner }}/{{ .Index }}{{ else }}{{ .PVC }}{{ end }}"
  defaultOwner: "1000:1000"
  defaultMode: "0750"
  properties: |
    compression=zstd
    atime=off
  encryption: |
    keyformat=hex
    keylocation=http://vault.homelab:8200/v1/secret/data/zfs-keys/tank
```

### 2. Generic Directory StorageClass (Ext4, XFS, etc.)
```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: local-dir
provisioner: subvol.io/provisioner
volumeBindingMode: WaitForFirstConsumer
reclaimPolicy: Delete
parameters:
  driver: dir
  mountPrefix: /mnt/storage
  pathTemplate: "{{ .Namespace }}/{{ if .Index }}{{ .Owner }}/{{ .Index }}{{ else }}{{ .PVC }}{{ end }}"
  defaultOwner: "1000:1000"
  defaultMode: "0750"
```

### 3. PersistentVolumeClaim
```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: media-storage
  namespace: media
  annotations:
    subvol.io/owner: "1000:1000"
    subvol.io/mode: "0770"
spec:
  accessModes:
    - ReadWriteOnce
  storageClassName: zfs-fast
  resources:
    requests:
      storage: 100Gi
```

---

## Deployment

Deploy the RBAC and DaemonSet to your cluster:

```bash
kubectl apply -f deploy/rbac.yaml
kubectl apply -f deploy/daemonset.yaml
```

---

## Development & Testing

Run unit tests with race detection:
```bash
make test
```

Build local binary:
```bash
make build
```

Run test suite via Docker:
```bash
make docker-build
```

---

## Contributing & Community

Contributions and feedback are welcome!
- **Contributing Guidelines**: Please read [CONTRIBUTING.md](CONTRIBUTING.md) for branch workflows and development instructions.
- **Code of Conduct**: We expect contributors to adhere to our [Code of Conduct](CODE_OF_CONDUCT.md).
- **Security**: For reporting security vulnerabilities, please refer to [SECURITY.md](SECURITY.md).

---

## License

This project is licensed under the Apache License 2.0. See [LICENSE](LICENSE) for details.
