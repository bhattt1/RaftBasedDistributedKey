//go:build !unix

package storage

import (
	"errors"
	"io"
)

// The server depends on flock and on fsync of directories, which this
// platform does not provide in the form the durability argument needs.
// Refusing to start is safer than running with weaker guarantees than the
// documentation promises. Use Linux, or WSL2 on Windows.
func lockFile(string) (io.Closer, error) {
	return nil, errors.New("storage: this platform is not supported for running a node; use Linux or WSL2")
}
