# Contributing to k8s-subvol-provisioner

Thank you for your interest in contributing to `k8s-subvol-provisioner`! We welcome contributions, bug reports, feature requests, and pull requests.

## Development Workflow

### Prerequisites
- Go 1.24+
- Docker (for multi-platform container builds)
- A working Kubernetes cluster or `minikube` / `kind` for integration testing

### Building & Running Tests

1. Clone the repository:
   ```bash
   git clone https://github.com/blesswinsamuel/k8s-subvol-provisioner.git
   cd k8s-subvol-provisioner
   ```

2. Run tests with race detection:
   ```bash
   make test
   ```

3. Run linter and static analysis:
   ```bash
   make vet
   ```

4. Compile the binary:
   ```bash
   make build
   ```

### Code Style & Guidelines
- **Conventional Commits**: Commit messages must follow [Conventional Commits](https://www.conventionalcommits.org/) (e.g. `feat: ...`, `fix: ...`, `chore: ...`, `docs: ...`, `test: ...`).
- **No Heavy Out-of-Tree Dependencies**: Keep dependencies minimal and lightweight.
- **Root Causes Over Symptoms**: Fix underlying architectural issues rather than layering workarounds.
- **Test Coverage**: All drivers and reconciler logic must include unit tests.

### Submitting a Pull Request
1. Fork the repository and create a feature branch (`git checkout -b feat/my-feature`).
2. Ensure all tests pass (`make test`).
3. Commit your changes following conventional commit syntax.
4. Push your branch to GitHub and open a Pull Request.
5. Provide a clear description of what the PR accomplishes and any relevant issue references.
