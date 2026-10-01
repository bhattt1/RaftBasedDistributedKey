package sim

import (
	"encoding/binary"
	"errors"
	"hash/fnv"

	"github.com/bhattt1/RaftBasedDistributedKey/internal/raft"
)

// HashMachine is a minimal state machine for consensus tests. Its state is a
// running hash of every command it applied, in order, plus a count. Two
// machines agree if and only if they applied the same commands in the same
// order.
type HashMachine struct {
	Sum   uint64
	Count uint64
}

func (m *HashMachine) Apply(e raft.Entry) any {
	h := fnv.New64a()
	var prev [8]byte
	binary.BigEndian.PutUint64(prev[:], m.Sum)
	h.Write(prev[:])
	h.Write(e.Data)
	m.Sum = h.Sum64()
	m.Count++
	return nil
}

func (m *HashMachine) Snapshot() []byte {
	out := make([]byte, 16)
	binary.BigEndian.PutUint64(out[0:8], m.Sum)
	binary.BigEndian.PutUint64(out[8:16], m.Count)
	return out
}

func (m *HashMachine) Restore(data []byte) error {
	if len(data) != 16 {
		return errors.New("sim: bad HashMachine snapshot")
	}
	m.Sum = binary.BigEndian.Uint64(data[0:8])
	m.Count = binary.BigEndian.Uint64(data[8:16])
	return nil
}
