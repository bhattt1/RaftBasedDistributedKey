#!/usr/bin/env bash
# Regenerates the Go code for the peer protocol from api/raft/v1/raft.proto.
#
# The generated files are committed. CI runs this script and fails if the
# result differs from what is in the repository, so the tool versions pinned
# in scripts/install-tools.sh are the ones that must be used.
set -euo pipefail

cd "$(dirname "$0")/.."
export PATH="$HOME/.local/bin:$(go env GOPATH)/bin:$PATH"

MODULE="$(go list -m)"

want_protoc="libprotoc 36.2"
have_protoc="$(protoc --version)"
if [[ "$have_protoc" != "$want_protoc" ]]; then
  echo "protoc is '$have_protoc', expected '$want_protoc'. Run scripts/install-tools.sh." >&2
  exit 1
fi

protoc \
  --proto_path=. \
  --go_out=. --go_opt=module="$MODULE" \
  --go-grpc_out=. --go-grpc_opt=module="$MODULE" \
  api/raft/v1/raft.proto

gofmt -l internal/gen >/dev/null
echo "generated internal/gen/raft/v1 with $(protoc --version), $(protoc-gen-go --version), $(protoc-gen-go-grpc --version)"
