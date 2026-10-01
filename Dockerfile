# syntax=docker/dockerfile:1

# Base images are pinned by tag and digest so that a rebuild next year uses
# the same toolchain and the same C library as the build that was tested.
ARG GO_IMAGE=golang:1.27.1-alpine@sha256:8a5910f31396cd4d89662f56c68b3ae31d374308270a1c3bd96672ee5ed43414
ARG RUNTIME_IMAGE=alpine:3.23@sha256:85fe1e81d6758c208f3e1eed4338a1997e19d4be002d4dd32d3100c9a8c010a0

# ---- build -------------------------------------------------------------------
FROM ${GO_IMAGE} AS build
WORKDIR /src
# Dependencies first: this layer is reused until go.mod or go.sum changes.
COPY go.mod go.sum ./
RUN go mod download && go mod verify
COPY . .
ARG VERSION=dev
# Static binaries (CGO disabled) so the runtime image needs nothing but libc-free
# executables. -trimpath keeps local paths out of the binaries.
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/ ./cmd/kvserver ./cmd/kvctl ./cmd/kvbench

# ---- runtime -----------------------------------------------------------------
# The image a node runs from. No compiler, no package manager cache, and the
# process does not run as root.
FROM ${RUNTIME_IMAGE} AS runtime
RUN addgroup -S -g 10001 raftkv \
 && adduser -S -u 10001 -G raftkv -H raftkv \
 && mkdir -p /var/lib/raftkv \
 && chown raftkv:raftkv /var/lib/raftkv \
 && chmod 0700 /var/lib/raftkv
COPY --from=build /out/kvserver /out/kvctl /out/kvbench /usr/local/bin/
USER raftkv:raftkv
VOLUME ["/var/lib/raftkv"]
# 7000: peer protocol, 8000: client API, 9000: metrics and pprof.
EXPOSE 7000 8000 9000
ENTRYPOINT ["/usr/local/bin/kvserver"]

# ---- chaos -------------------------------------------------------------------
# Test-only variant used by the fault-injection scripts. It adds iptables so a
# script can cut a node off from its peers. The server process still runs as
# the unprivileged user; packet filtering is done by `docker exec -u 0`, and
# only when the Compose override grants the container NET_ADMIN. Never deploy
# this target.
FROM runtime AS chaos
USER root
RUN apk add --no-cache iptables
USER raftkv:raftkv
