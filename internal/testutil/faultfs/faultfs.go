// Package faultfs is an in-memory filesystem that models what survives a
// crash and can fail or "lose power" at a chosen operation.
//
// The model is deliberately pessimistic and follows what POSIX actually
// promises:
//
//   - File contents are durable only up to the last successful Sync.
//   - A file's name (creation, rename, removal) is durable only after its
//     directory has been synced.
//   - Bytes written after the last Sync may survive a crash in part, may be
//     lost, or may come back as zeros.
//
// Code that survives every crash point in this model does not depend on luck
// with the real filesystem.
package faultfs

import (
	"errors"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/bhattt1/RaftBasedDistributedKey/internal/storage"
)

// ErrCrashed is returned by every operation after the simulated crash point
// until Restart is called.
var ErrCrashed = errors.New("faultfs: process crashed")

type inode struct {
	data   []byte
	synced []byte
}

// FS implements storage.FS in memory.
type FS struct {
	mu      sync.Mutex
	rng     *rand.Rand
	live    map[string]*inode // names as the running process sees them
	durable map[string]*inode // names that would survive a crash
	dirs    map[string]bool
	locks   map[string]bool
	gen     int // incremented on Restart; invalidates old handles

	ops     int
	crashAt int
	crashed bool
	failAt  int
	failErr error
}

var _ storage.FS = (*FS)(nil)

// New returns an empty filesystem. The seed drives every random choice about
// what a crash preserves.
func New(seed int64) *FS {
	return &FS{
		rng:     rand.New(rand.NewSource(seed)),
		live:    map[string]*inode{},
		durable: map[string]*inode{},
		dirs:    map[string]bool{},
		locks:   map[string]bool{},
	}
}

// Ops returns how many mutating operations have run since the last Restart.
func (fs *FS) Ops() int {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.ops
}

// CrashAt makes the n-th mutating operation (counting from 1 since the last
// Restart) the moment the process dies. That operation may take partial
// effect; it and every later operation return ErrCrashed.
func (fs *FS) CrashAt(n int) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.crashAt = n
}

// FailAt makes the n-th mutating operation fail with err once, without
// crashing. A failed write may still have written a prefix.
func (fs *FS) FailAt(n int, err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.failAt, fs.failErr = n, err
}

// Crashed reports whether the crash point has been reached.
func (fs *FS) Crashed() bool {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.crashed
}

// Restart simulates power loss followed by a reboot: only durable names
// remain, and each file keeps its synced contents plus, at random, some of
// the bytes written after the last sync.
func (fs *FS) Restart() {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.gen++
	fs.live = make(map[string]*inode, len(fs.durable))
	seen := map[*inode]bool{}
	for name, ino := range fs.durable {
		fs.live[name] = ino
		if seen[ino] {
			continue
		}
		seen[ino] = true
		tail := 0
		appendOnly := len(ino.data) >= len(ino.synced) && string(ino.data[:len(ino.synced)]) == string(ino.synced)
		if appendOnly {
			tail = len(ino.data) - len(ino.synced)
		}
		kept := append([]byte(nil), ino.synced...)
		if tail > 0 {
			switch fs.rng.Intn(3) {
			case 0: // nothing written after the last sync survived
			case 1: // a prefix survived
				n := fs.rng.Intn(tail + 1)
				kept = append(kept, ino.data[len(ino.synced):len(ino.synced)+n]...)
			case 2: // the file grew but the data blocks never made it
				kept = append(kept, make([]byte, fs.rng.Intn(tail+1))...)
			}
		}
		ino.data = kept
		ino.synced = append([]byte(nil), kept...)
	}
	fs.locks = map[string]bool{}
	fs.ops, fs.crashAt, fs.crashed, fs.failAt, fs.failErr = 0, 0, false, 0, nil
}

// step accounts for one mutating operation. It returns an error if the
// operation must fail, and partial=true if the operation should still take
// partial effect before failing.
func (fs *FS) step() (err error, partial bool) {
	if fs.crashed {
		return ErrCrashed, false
	}
	fs.ops++
	if fs.crashAt > 0 && fs.ops >= fs.crashAt {
		fs.crashed = true
		return ErrCrashed, true
	}
	if fs.failAt > 0 && fs.ops == fs.failAt {
		return fs.failErr, true
	}
	return nil, false
}

func notExist(op, name string) error {
	return &os.PathError{Op: op, Path: name, Err: os.ErrNotExist}
}

// ---- storage.FS ------------------------------------------------------------

func (fs *FS) OpenFile(name string, flag int, _ os.FileMode) (storage.File, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.crashed {
		return nil, ErrCrashed
	}
	name = filepath.Clean(name)
	ino, ok := fs.live[name]
	switch {
	case ok && flag&os.O_CREATE != 0 && flag&os.O_EXCL != 0:
		return nil, &os.PathError{Op: "open", Path: name, Err: os.ErrExist}
	case !ok && flag&os.O_CREATE == 0:
		return nil, notExist("open", name)
	case !ok:
		if !fs.dirs[filepath.Dir(name)] {
			return nil, notExist("open", name)
		}
		if err, _ := fs.step(); err != nil {
			return nil, err
		}
		ino = &inode{}
		fs.live[name] = ino
	}
	if flag&os.O_TRUNC != 0 && len(ino.data) > 0 {
		if err, _ := fs.step(); err != nil {
			return nil, err
		}
		ino.data = nil
	}
	return &file{fs: fs, ino: ino, gen: fs.gen, readOnly: flag&(os.O_WRONLY|os.O_RDWR) == 0}, nil
}

func (fs *FS) Rename(oldname, newname string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	oldname, newname = filepath.Clean(oldname), filepath.Clean(newname)
	ino, ok := fs.live[oldname]
	if !ok {
		return notExist("rename", oldname)
	}
	if err, _ := fs.step(); err != nil {
		return err
	}
	delete(fs.live, oldname)
	fs.live[newname] = ino
	return nil
}

func (fs *FS) Remove(name string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	name = filepath.Clean(name)
	if _, ok := fs.live[name]; !ok {
		return notExist("remove", name)
	}
	if err, _ := fs.step(); err != nil {
		return err
	}
	delete(fs.live, name)
	return nil
}

// MkdirAll creates directories. Directories are treated as durable at once;
// the interesting failures are about files.
func (fs *FS) MkdirAll(path string, _ os.FileMode) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.crashed {
		return ErrCrashed
	}
	for p := filepath.Clean(path); ; p = filepath.Dir(p) {
		fs.dirs[p] = true
		if p == filepath.Dir(p) {
			return nil
		}
	}
}

func (fs *FS) ReadDir(dir string) ([]string, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.crashed {
		return nil, ErrCrashed
	}
	dir = filepath.Clean(dir)
	if !fs.dirs[dir] {
		return nil, notExist("readdir", dir)
	}
	var names []string
	for name := range fs.live {
		if filepath.Dir(name) == dir {
			names = append(names, filepath.Base(name))
		}
	}
	sort.Strings(names)
	return names, nil
}

func (fs *FS) SyncDir(dir string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	dir = filepath.Clean(dir)
	err, partial := fs.step()
	if err != nil && !(partial && fs.rng.Intn(2) == 0) {
		return err
	}
	// Make the directory's current entries the durable ones.
	for name := range fs.durable {
		if filepath.Dir(name) == dir {
			if _, ok := fs.live[name]; !ok {
				delete(fs.durable, name)
			}
		}
	}
	for name, ino := range fs.live {
		if filepath.Dir(name) == dir {
			fs.durable[name] = ino
		}
	}
	return err
}

type lock struct {
	fs   *FS
	name string
	gen  int
}

func (l *lock) Close() error {
	l.fs.mu.Lock()
	defer l.fs.mu.Unlock()
	if l.gen == l.fs.gen {
		delete(l.fs.locks, l.name)
	}
	return nil
}

func (fs *FS) Lock(name string) (io.Closer, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.crashed {
		return nil, ErrCrashed
	}
	name = filepath.Clean(name)
	if fs.locks[name] {
		return nil, storage.ErrLocked
	}
	fs.locks[name] = true
	return &lock{fs: fs, name: name, gen: fs.gen}, nil
}

// ---- files -----------------------------------------------------------------

type file struct {
	fs       *FS
	ino      *inode
	gen      int
	pos      int64
	readOnly bool
	closed   bool
}

func (f *file) check() error {
	if f.closed {
		return os.ErrClosed
	}
	if f.fs.crashed || f.gen != f.fs.gen {
		return ErrCrashed
	}
	return nil
}

func (f *file) Read(p []byte) (int, error) {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	if err := f.check(); err != nil {
		return 0, err
	}
	if f.pos >= int64(len(f.ino.data)) {
		return 0, io.EOF
	}
	n := copy(p, f.ino.data[f.pos:])
	f.pos += int64(n)
	return n, nil
}

func (f *file) Write(p []byte) (int, error) {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	if err := f.check(); err != nil {
		return 0, err
	}
	if f.readOnly {
		return 0, &os.PathError{Op: "write", Err: os.ErrPermission}
	}
	err, partial := f.fs.step()
	n := len(p)
	if err != nil {
		if !partial {
			return 0, err
		}
		n = f.fs.rng.Intn(len(p) + 1) // a short write
	}
	end := f.pos + int64(n)
	if end > int64(len(f.ino.data)) {
		f.ino.data = append(f.ino.data, make([]byte, end-int64(len(f.ino.data)))...)
	}
	copy(f.ino.data[f.pos:end], p[:n])
	f.pos = end
	return n, err
}

func (f *file) Seek(offset int64, whence int) (int64, error) {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	if err := f.check(); err != nil {
		return 0, err
	}
	switch whence {
	case io.SeekStart:
		f.pos = offset
	case io.SeekCurrent:
		f.pos += offset
	case io.SeekEnd:
		f.pos = int64(len(f.ino.data)) + offset
	}
	if f.pos < 0 {
		f.pos = 0
	}
	return f.pos, nil
}

func (f *file) Truncate(size int64) error {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	if err := f.check(); err != nil {
		return err
	}
	if err, _ := f.fs.step(); err != nil {
		return err
	}
	if size < int64(len(f.ino.data)) {
		f.ino.data = f.ino.data[:size]
	} else {
		f.ino.data = append(f.ino.data, make([]byte, size-int64(len(f.ino.data)))...)
	}
	return nil
}

func (f *file) Sync() error {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	if err := f.check(); err != nil {
		return err
	}
	err, partial := f.fs.step()
	// A sync interrupted by the crash may or may not have reached the disk;
	// either way the caller never learns that it succeeded.
	if err != nil && !(partial && f.fs.crashed && f.fs.rng.Intn(2) == 0) {
		return err
	}
	f.ino.synced = append([]byte(nil), f.ino.data...)
	return err
}

func (f *file) Close() error {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	if f.closed {
		return os.ErrClosed
	}
	f.closed = true
	return nil
}

// ---- helpers for tests -----------------------------------------------------

// ReadFile returns a copy of a file's current contents.
func (fs *FS) ReadFile(name string) ([]byte, bool) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	ino, ok := fs.live[filepath.Clean(name)]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), ino.data...), true
}

// WriteFile replaces a file's contents and makes them fully durable. Tests
// use it to plant damaged or hand-built files.
func (fs *FS) WriteFile(name string, data []byte) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	name = filepath.Clean(name)
	ino, ok := fs.live[name]
	if !ok {
		ino = &inode{}
		fs.live[name] = ino
	}
	ino.data = append([]byte(nil), data...)
	ino.synced = append([]byte(nil), data...)
	fs.durable[name] = ino
	for p := filepath.Dir(name); ; p = filepath.Dir(p) {
		fs.dirs[p] = true
		if p == filepath.Dir(p) {
			break
		}
	}
}

// Names returns every file name the running process can see, sorted.
func (fs *FS) Names() []string {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	names := make([]string, 0, len(fs.live))
	for name := range fs.live {
		names = append(names, filepath.ToSlash(name))
	}
	sort.Strings(names)
	return names
}
