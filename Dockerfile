# Build stage
FROM golang:1.24-bookworm AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG COMMIT=none
ARG DATE=unknown

RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION} -X main.commit=${COMMIT} -X main.date=${DATE}" \
    -o /bin/subvol-provisioner ./cmd/subvol-provisioner

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    zfsutils-linux \
    btrfs-progs \
    util-linux \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /bin/subvol-provisioner /usr/local/bin/subvol-provisioner

ENTRYPOINT ["/usr/local/bin/subvol-provisioner"]
