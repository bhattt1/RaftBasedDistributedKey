#!/usr/bin/env bash
# Installs the pinned code-generation and analysis tools used by `make generate`
# and `make lint`. Versions are pinned here on purpose: regenerating with a
# different protoc or plugin produces different bytes and CI would flag a diff.
set -euo pipefail

PROTOC_VERSION="36.2"
PROTOC_GEN_GO_VERSION="v1.36.12"
PROTOC_GEN_GO_GRPC_VERSION="v1.6.2"
STATICCHECK_VERSION="v0.8.1"
GOVULNCHECK_VERSION="v1.8.0"

PREFIX="${PREFIX:-$HOME/.local}"
mkdir -p "$PREFIX/bin"

have_protoc="$("$PREFIX/bin/protoc" --version 2>/dev/null || true)"
if [[ "$have_protoc" != "libprotoc $PROTOC_VERSION" ]]; then
  case "$(uname -m)" in
    x86_64) arch="x86_64" ;;
    aarch64 | arm64) arch="aarch_64" ;;
    *) echo "unsupported architecture $(uname -m)" >&2; exit 1 ;;
  esac
  tmp="$(mktemp -d)"
  trap 'rm -rf "$tmp"' EXIT
  url="https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-${arch}.zip"
  echo "downloading $url"
  curl -fsSL -o "$tmp/protoc.zip" "$url"
  unzip -q -o "$tmp/protoc.zip" -d "$tmp/protoc"
  install -m 0755 "$tmp/protoc/bin/protoc" "$PREFIX/bin/protoc"
  mkdir -p "$PREFIX/include"
  cp -r "$tmp/protoc/include/." "$PREFIX/include/"
fi

go install "google.golang.org/protobuf/cmd/protoc-gen-go@${PROTOC_GEN_GO_VERSION}"
go install "google.golang.org/grpc/cmd/protoc-gen-go-grpc@${PROTOC_GEN_GO_GRPC_VERSION}"
go install "honnef.co/go/tools/cmd/staticcheck@${STATICCHECK_VERSION}"
go install "golang.org/x/vuln/cmd/govulncheck@${GOVULNCHECK_VERSION}"

echo "protoc:             $("$PREFIX/bin/protoc" --version)"
echo "protoc-gen-go:      $(protoc-gen-go --version)"
echo "protoc-gen-go-grpc: $(protoc-gen-go-grpc --version)"
echo "staticcheck:        $(staticcheck --version)"
