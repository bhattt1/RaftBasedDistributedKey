//go:build unix

package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
)

// lockFile takes an exclusive, non-blocking flock on name. The kernel drops
// the lock when the process exits, however it exits, so a crashed node never
// leaves a stale lock behind.
func lockFile(name string) (io.Closer, error) {
	f, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("%w: %s", ErrLocked, name)
		}
		return nil, fmt.Errorf("locking %s: %w", name, err)
	}
	return f, nil
}
