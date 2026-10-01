// Package storage keeps a node's durable state: the write-ahead log holding
// the Raft term, vote and log entries, and the snapshot files that let the
// log be truncated.
//
// Everything goes through the small FS interface below so that tests can
// substitute a filesystem that fails or "loses power" at a chosen operation.
package storage

import (
	"io"
	"os"
	"path/filepath"
	"sort"
)

// File is the subset of *os.File the storage code uses.
type File interface {
	io.Reader
	io.Writer
	io.Seeker
	io.Closer
	// Sync flushes the file's contents to stable storage.
	Sync() error
	Truncate(size int64) error
}

// FS is the filesystem as seen by the storage code.
type FS interface {
	OpenFile(name string, flag int, perm os.FileMode) (File, error)
	Rename(oldname, newname string) error
	Remove(name string) error
	MkdirAll(path string, perm os.FileMode) error
	// ReadDir returns the names in dir, sorted.
	ReadDir(dir string) ([]string, error)
	// SyncDir flushes directory entries. Creating, renaming or removing a
	// file is not durable until its directory has been synced.
	SyncDir(dir string) error
	// Lock takes an exclusive lock that is released by closing the result.
	// It fails immediately if another holder exists.
	Lock(name string) (io.Closer, error)
}

// OSFS is the real filesystem.
type OSFS struct{}

func (OSFS) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	return os.OpenFile(name, flag, perm)
}

func (OSFS) Rename(oldname, newname string) error { return os.Rename(oldname, newname) }

func (OSFS) Remove(name string) error { return os.Remove(name) }

func (OSFS) MkdirAll(path string, perm os.FileMode) error { return os.MkdirAll(path, perm) }

func (OSFS) ReadDir(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	return names, nil
}

func (OSFS) SyncDir(dir string) error {
	d, err := os.Open(filepath.Clean(dir))
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		d.Close()
		return err
	}
	return d.Close()
}

func (OSFS) Lock(name string) (io.Closer, error) { return lockFile(name) }
