// Package kv is the replicated state machine: a key-value map with atomic
// compare-and-swap and a table that remembers the outcome of each client's
// most recent mutation so that retries are not applied twice.
//
// Everything here is deterministic. Given the same sequence of commands,
// every node produces the same state and the same results. The package never
// reads a clock, never iterates a map where order could leak into state, and
// takes all its inputs from the log.
package kv

import (
	"encoding/binary"
	"errors"
	"fmt"
	"unicode/utf8"
)

// Op is the kind of a command.
type Op uint8

const (
	OpPut Op = iota + 1
	OpDelete
	OpCAS
	// OpRead is replicated like a write. Its result is captured when the
	// entry is applied, which is what makes the read linearizable.
	OpRead
)

func (o Op) String() string {
	switch o {
	case OpPut:
		return "PUT"
	case OpDelete:
		return "DELETE"
	case OpCAS:
		return "CAS"
	case OpRead:
		return "READ"
	}
	return fmt.Sprintf("op(%d)", uint8(o))
}

// IsMutation reports whether the op can change state.
func (o Op) IsMutation() bool { return o == OpPut || o == OpDelete || o == OpCAS }

// Expect is the kind of condition a CAS carries.
type Expect uint8

const (
	// ExpectAbsent succeeds only if the key does not exist.
	ExpectAbsent Expect = iota + 1
	// ExpectValue succeeds only if the key exists with exactly this value.
	ExpectValue
	// ExpectRevision succeeds only if the key exists at exactly this
	// revision.
	ExpectRevision
)

// Command is one operation on the store.
type Command struct {
	Op    Op
	Key   string
	Value []byte // PUT and CAS: the value to store

	// CAS only.
	Expect         Expect
	ExpectValue    []byte
	ExpectRevision uint64

	// ClientID and Seq identify a mutation for de-duplication. A client
	// numbers its mutations 1, 2, 3, ... and sends them one at a time; a
	// retry reuses the same number. Reads carry no identity.
	ClientID string
	Seq      uint64
}

// Limits bound what the state machine accepts and how large it can grow.
//
// They are part of the replicated behaviour: a PUT that one node rejects as
// too large must be rejected by every node, or their states would diverge.
// The server therefore always runs with DefaultLimits. Changing a limit is a
// protocol change that needs every node upgraded together.
type Limits struct {
	MaxKeyBytes   int
	MaxValueBytes int
	// MaxStateBytes bounds the total size of all keys and values.
	MaxStateBytes int64
	// MaxSessions bounds the de-duplication table. When it is full the
	// session that has gone longest without a mutation is forgotten.
	MaxSessions int
}

// DefaultLimits are the limits every server uses.
var DefaultLimits = Limits{
	MaxKeyBytes:   256,
	MaxValueBytes: 64 << 10,
	MaxStateBytes: 64 << 20,
	MaxSessions:   4096,
}

// MaxClientIDBytes bounds a client identifier.
const MaxClientIDBytes = 64

// Hard caps used while decoding, before any Limits are consulted, so that a
// malformed length can never cause a large allocation.
const (
	hardMaxKey   = 4 << 10
	hardMaxValue = 1 << 20
)

const codecVersion = 1

// ErrInvalid is wrapped by every validation and decoding error.
var ErrInvalid = errors.New("kv: invalid command")

func invalidf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, args...))
}

// Validate checks a command against the limits. The HTTP layer calls it
// before proposing; the state machine calls it again when applying, because
// what is in the log is what counts.
func (c *Command) Validate(l Limits) error {
	switch {
	case c.Key == "":
		return invalidf("empty key")
	case len(c.Key) > l.MaxKeyBytes:
		return invalidf("key is %d bytes, the limit is %d", len(c.Key), l.MaxKeyBytes)
	case !utf8.ValidString(c.Key):
		return invalidf("key is not valid UTF-8")
	}
	for _, r := range c.Key {
		if r < 0x20 || r == 0x7f {
			return invalidf("key contains a control character")
		}
	}
	if len(c.Value) > l.MaxValueBytes {
		return invalidf("value is %d bytes, the limit is %d", len(c.Value), l.MaxValueBytes)
	}
	switch c.Op {
	case OpPut:
	case OpDelete, OpRead:
		if len(c.Value) != 0 {
			return invalidf("%s does not take a value", c.Op)
		}
	case OpCAS:
		switch c.Expect {
		case ExpectAbsent:
		case ExpectValue:
			if len(c.ExpectValue) > l.MaxValueBytes {
				return invalidf("expected value is %d bytes, the limit is %d", len(c.ExpectValue), l.MaxValueBytes)
			}
		case ExpectRevision:
			if c.ExpectRevision == 0 {
				return invalidf("expected revision must be positive")
			}
		default:
			return invalidf("CAS needs exactly one condition")
		}
	default:
		return invalidf("unknown operation %d", uint8(c.Op))
	}
	if c.Op != OpCAS && (c.Expect != 0 || len(c.ExpectValue) != 0 || c.ExpectRevision != 0) {
		return invalidf("%s does not take a condition", c.Op)
	}
	if c.Op.IsMutation() {
		switch {
		case c.ClientID == "" || c.Seq == 0:
			return invalidf("a mutation needs a client ID and a sequence number")
		case len(c.ClientID) > MaxClientIDBytes:
			return invalidf("client ID is %d bytes, the limit is %d", len(c.ClientID), MaxClientIDBytes)
		}
	} else if c.ClientID != "" || c.Seq != 0 {
		return invalidf("a read does not carry a request identity")
	}
	return nil
}

// Encode serialises the command for the log. The encoding is canonical: equal
// commands produce equal bytes, which is what lets the store detect a request
// identity being reused for a different operation.
//
//	version(1) op(1) key client seq
//	PUT:  value
//	CAS:  expect(1) [expected value | expected revision] value
//
// Strings and byte slices are a uvarint length followed by the bytes; numbers
// are uvarints.
func (c Command) Encode() []byte {
	buf := make([]byte, 0, 16+len(c.Key)+len(c.ClientID)+len(c.Value)+len(c.ExpectValue))
	buf = append(buf, codecVersion, byte(c.Op))
	buf = appendBytes(buf, []byte(c.Key))
	buf = appendBytes(buf, []byte(c.ClientID))
	buf = binary.AppendUvarint(buf, c.Seq)
	switch c.Op {
	case OpPut:
		buf = appendBytes(buf, c.Value)
	case OpCAS:
		buf = append(buf, byte(c.Expect))
		switch c.Expect {
		case ExpectValue:
			buf = appendBytes(buf, c.ExpectValue)
		case ExpectRevision:
			buf = binary.AppendUvarint(buf, c.ExpectRevision)
		}
		buf = appendBytes(buf, c.Value)
	}
	return buf
}

// Decode parses a command. It rejects anything it does not fully understand:
// unknown versions and operations, lengths beyond the hard caps, and trailing
// bytes.
func Decode(data []byte) (Command, error) {
	var c Command
	d := decoder{buf: data}
	if v := d.byte(); v != codecVersion {
		return c, invalidf("unsupported command encoding version %d", v)
	}
	c.Op = Op(d.byte())
	c.Key = string(d.bytes(hardMaxKey))
	c.ClientID = string(d.bytes(MaxClientIDBytes))
	c.Seq = d.uvarint()
	switch c.Op {
	case OpPut:
		c.Value = d.bytes(hardMaxValue)
	case OpCAS:
		c.Expect = Expect(d.byte())
		switch c.Expect {
		case ExpectAbsent:
		case ExpectValue:
			c.ExpectValue = d.bytes(hardMaxValue)
		case ExpectRevision:
			c.ExpectRevision = d.uvarint()
		default:
			d.fail("unknown CAS condition")
		}
		c.Value = d.bytes(hardMaxValue)
	case OpDelete, OpRead:
	default:
		d.fail("unknown operation")
	}
	if d.err == nil && len(d.buf) != 0 {
		d.fail("trailing bytes")
	}
	if d.err != nil {
		return Command{}, d.err
	}
	return c, nil
}

func appendBytes(buf, b []byte) []byte {
	buf = binary.AppendUvarint(buf, uint64(len(b)))
	return append(buf, b...)
}

// decoder reads from a byte slice and remembers the first error, so callers
// can decode a whole structure and check once at the end.
type decoder struct {
	buf []byte
	err error
}

func (d *decoder) fail(msg string) {
	if d.err == nil {
		d.err = invalidf("%s", msg)
	}
}

func (d *decoder) byte() byte {
	if d.err != nil {
		return 0
	}
	if len(d.buf) == 0 {
		d.fail("unexpected end of data")
		return 0
	}
	b := d.buf[0]
	d.buf = d.buf[1:]
	return b
}

func (d *decoder) uvarint() uint64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Uvarint(d.buf)
	if n <= 0 {
		d.fail("bad varint")
		return 0
	}
	// Reject non-minimal encodings so that each value has one encoding.
	if n > 1 && d.buf[n-1] == 0 {
		d.fail("non-canonical varint")
		return 0
	}
	d.buf = d.buf[n:]
	return v
}

// bytes reads a length-prefixed slice of at most max bytes. The result is a
// copy, so it does not keep the input alive or alias it.
func (d *decoder) bytes(max int) []byte {
	n := d.uvarint()
	if d.err != nil {
		return nil
	}
	if n > uint64(max) {
		d.fail("length exceeds the limit")
		return nil
	}
	if uint64(len(d.buf)) < n {
		d.fail("unexpected end of data")
		return nil
	}
	if n == 0 {
		return nil
	}
	out := append([]byte(nil), d.buf[:n]...)
	d.buf = d.buf[n:]
	return out
}
