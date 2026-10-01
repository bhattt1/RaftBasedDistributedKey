package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"strings"

	"github.com/bhattt1/RaftBasedDistributedKey/internal/raft"
)

// A snapshot file is laid out as:
//
//	"RKVSNAP\x01"            8 bytes, magic and format version
//	last included index      8 bytes
//	last included term       8 bytes
//	payload length           8 bytes
//	cluster ID length        2 bytes
//	cluster ID               n bytes
//	header CRC-32C           4 bytes, over everything above
//	payload                  the state machine's own encoding
//	payload CRC-32C          4 bytes
//
// The file name repeats the index and term so the newest snapshot can be
// found without opening files, but the header is what is trusted.
const (
	snapMagic          = "RKVSNAP\x01"
	snapFixedHeader    = 8 + 8 + 8 + 8 + 2
	maxClusterIDLength = 256
)

// SnapshotHeader is the metadata stored at the start of a snapshot file.
type SnapshotHeader struct {
	Meta       raft.SnapshotMeta
	ClusterID  string
	PayloadLen int64
}

func (h SnapshotHeader) encode() []byte {
	buf := make([]byte, 0, snapFixedHeader+len(h.ClusterID)+4)
	buf = append(buf, snapMagic...)
	buf = binary.LittleEndian.AppendUint64(buf, h.Meta.Index)
	buf = binary.LittleEndian.AppendUint64(buf, h.Meta.Term)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(h.PayloadLen))
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(h.ClusterID)))
	buf = append(buf, h.ClusterID...)
	return binary.LittleEndian.AppendUint32(buf, crc32.Checksum(buf, castagnoli))
}

// fileSize is the total size of a snapshot file with this header.
func (h SnapshotHeader) fileSize() int64 {
	return int64(snapFixedHeader+len(h.ClusterID)+4) + h.PayloadLen + 4
}

func snapshotName(meta raft.SnapshotMeta) string {
	return fmt.Sprintf("snap-%016x-%016x.snap", meta.Index, meta.Term)
}

func parseSnapshotName(name string) (raft.SnapshotMeta, bool) {
	var meta raft.SnapshotMeta
	if !strings.HasPrefix(name, "snap-") || !strings.HasSuffix(name, ".snap") {
		return meta, false
	}
	if _, err := fmt.Sscanf(name, "snap-%016x-%016x.snap", &meta.Index, &meta.Term); err != nil {
		return meta, false
	}
	return meta, snapshotName(meta) == name
}

// readSnapshotHeader reads and validates the header, leaving r positioned at
// the first payload byte.
func readSnapshotHeader(r io.Reader) (SnapshotHeader, error) {
	var h SnapshotHeader
	fixed := make([]byte, snapFixedHeader)
	if _, err := io.ReadFull(r, fixed); err != nil {
		return h, corruptf("snapshot header: %v", err)
	}
	if string(fixed[:8]) != snapMagic {
		return h, corruptf("bad snapshot magic")
	}
	h.Meta.Index = binary.LittleEndian.Uint64(fixed[8:])
	h.Meta.Term = binary.LittleEndian.Uint64(fixed[16:])
	payloadLen := binary.LittleEndian.Uint64(fixed[24:])
	idLen := int(binary.LittleEndian.Uint16(fixed[32:]))
	if idLen > maxClusterIDLength {
		return h, corruptf("snapshot cluster ID of %d bytes", idLen)
	}
	rest := make([]byte, idLen+4)
	if _, err := io.ReadFull(r, rest); err != nil {
		return h, corruptf("snapshot header: %v", err)
	}
	sum := crc32.Update(crc32.Checksum(fixed, castagnoli), castagnoli, rest[:idLen])
	if sum != binary.LittleEndian.Uint32(rest[idLen:]) {
		return h, corruptf("snapshot header checksum mismatch")
	}
	if payloadLen > 1<<62 {
		return h, corruptf("snapshot payload length %d", payloadLen)
	}
	h.ClusterID = string(rest[:idLen])
	h.PayloadLen = int64(payloadLen)
	return h, nil
}

// verifySnapshotFile reads a whole snapshot file and checks its checksums,
// its length, the cluster it belongs to and the size limit. It never holds
// more than a small buffer in memory.
func verifySnapshotFile(fs FS, path, clusterID string, maxPayload int64) (SnapshotHeader, error) {
	f, err := fs.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return SnapshotHeader{}, err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 256<<10)
	h, err := readSnapshotHeader(r)
	if err != nil {
		return h, err
	}
	if h.ClusterID != clusterID {
		return h, fmt.Errorf("snapshot belongs to cluster %q, this node is in cluster %q", h.ClusterID, clusterID)
	}
	if maxPayload > 0 && h.PayloadLen > maxPayload {
		return h, fmt.Errorf("snapshot payload of %d bytes exceeds the limit of %d", h.PayloadLen, maxPayload)
	}
	sum := crc32.New(castagnoli)
	if n, err := io.CopyN(sum, r, h.PayloadLen); err != nil {
		return h, corruptf("snapshot payload is %d bytes, header says %d", n, h.PayloadLen)
	}
	var trailer [4]byte
	if _, err := io.ReadFull(r, trailer[:]); err != nil {
		return h, corruptf("snapshot trailer: %v", err)
	}
	if sum.Sum32() != binary.LittleEndian.Uint32(trailer[:]) {
		return h, corruptf("snapshot payload checksum mismatch")
	}
	if _, err := r.ReadByte(); err != io.EOF {
		return h, corruptf("snapshot file has trailing data")
	}
	return h, nil
}

// writeSnapshotFile writes a complete snapshot file and syncs it.
func writeSnapshotFile(fs FS, path string, h SnapshotHeader, payload []byte) error {
	f, err := fs.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	var trailer [4]byte
	binary.LittleEndian.PutUint32(trailer[:], crc32.Checksum(payload, castagnoli))
	for _, part := range [][]byte{h.encode(), payload, trailer[:]} {
		if _, err := f.Write(part); err != nil {
			f.Close()
			return err
		}
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// payloadReader exposes just the payload of an open snapshot file.
type payloadReader struct {
	io.Reader
	f File
}

func (p *payloadReader) Close() error { return p.f.Close() }
