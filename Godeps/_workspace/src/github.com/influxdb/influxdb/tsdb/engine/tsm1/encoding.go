package tsm1

import (
	"encoding/binary"
	"fmt"
	"sort"
	"time"

	"github.com/influxdb/influxdb/tsdb"
)

const (
	// BlockFloat64 designates a block encodes float64 values
	BlockFloat64 = 0

	// BlockInt64 designates a block encodes int64 values
	BlockInt64 = 1

	// BlockBool designates a block encodes bool values
	BlockBool = 2

	// BlockString designates a block encodes string values
	BlockString = 3

	// encodedBlockHeaderSize is the size of the header for an encoded block.  The first 8 bytes
	// are the minimum timestamp of the block.  The next byte is a block encoding type indicator.
	encodedBlockHeaderSize = 9
)

type Value interface {
	Time() time.Time
	UnixNano() int64
	Value() interface{}
	Size() int
}

func NewValue(t time.Time, value interface{}) Value {
	switch v := value.(type) {
	case int64:
		return &Int64Value{time: t, value: v}
	case float64:
		return &FloatValue{time: t, value: v}
	case bool:
		return &BoolValue{time: t, value: v}
	case string:
		return &StringValue{time: t, value: v}
	}
	return &EmptyValue{}
}

type EmptyValue struct {
}

func (e *EmptyValue) UnixNano() int64    { return tsdb.EOF }
func (e *EmptyValue) Time() time.Time    { return time.Unix(0, tsdb.EOF) }
func (e *EmptyValue) Value() interface{} { return nil }
func (e *EmptyValue) Size() int          { return 0 }

// Values represented a time ascending sorted collection of Value types.
// the underlying type should be the same across all values, but the interface
// makes the code cleaner.
type Values []Value

func (a Values) MinTime() int64 {
	return a[0].Time().UnixNano()
}

func (a Values) MaxTime() int64 {
	return a[len(a)-1].Time().UnixNano()
}

// Encode converts the values to a byte slice.  If there are no values,
// this function panics.
func (a Values) Encode(buf []byte) ([]byte, error) {
	if len(a) == 0 {
		panic("unable to encode block type")
	}

	switch a[0].(type) {
	case *FloatValue:
		return encodeFloatBlock(buf, a)
	case *Int64Value:
		return encodeInt64Block(buf, a)
	case *BoolValue:
		return encodeBoolBlock(buf, a)
	case *StringValue:
		return encodeStringBlock(buf, a)
	}

	return nil, fmt.Errorf("unsupported value type %T", a[0])
}

// DecodeBlock takes a byte array and will decode into values of the appropriate type
// based on the block
func DecodeBlock(block []byte, vals *[]Value) error {
	if len(block) <= encodedBlockHeaderSize {
		panic(fmt.Sprintf("decode of short block: got %v, exp %v", len(block), encodedBlockHeaderSize))
	}

	blockType := block[8]
	switch blockType {
	case BlockFloat64:
		return decodeFloatBlock(block, vals)
	case BlockInt64:
		return decodeInt64Block(block, vals)
	case BlockBool:
		return decodeBoolBlock(block, vals)
	case BlockString:
		return decodeStringBlock(block, vals)
	default:
		panic(fmt.Sprintf("unknown block type: %d", blockType))
	}
}

// Deduplicate returns a new Values slice with any values
// that have the same  timestamp removed. The Value that appears
// last in the slice is the one that is kept. The returned slice is in ascending order
func (a Values) Deduplicate() Values {
	m := make(map[int64]Value)
	for _, val := range a {
		m[val.UnixNano()] = val
	}

	other := make([]Value, 0, len(m))
	for _, val := range m {
		other = append(other, val)
	}
	sort.Sort(Values(other))

	return other
}

// Sort methods
func (a Values) Len() int           { return len(a) }
func (a Values) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a Values) Less(i, j int) bool { return a[i].Time().UnixNano() < a[j].Time().UnixNano() }

type FloatValue struct {
	time  time.Time
	value float64
}

func (f *FloatValue) Time() time.Time {
	return f.time
}

func (f *FloatValue) UnixNano() int64 {
	return f.time.UnixNano()
}

func (f *FloatValue) Value() interface{} {
	return f.value
}

func (f *FloatValue) Size() int {
	return 16
}

func encodeFloatBlock(buf []byte, values []Value) ([]byte, error) {
	if len(values) == 0 {
		return nil, nil
	}

	// A float block is encoded using different compression strategies
	// for timestamps and values.

	// Encode values using Gorilla float compression
	venc := NewFloatEncoder()

	// Encode timestamps using an adaptive encoder that uses delta-encoding,
	// frame-or-reference and run length encoding.
	tsenc := NewTimeEncoder()

	for _, v := range values {
		tsenc.Write(v.Time())
		venc.Push(v.(*FloatValue).value)
	}
	venc.Finish()

	// Encoded timestamp values
	tb, err := tsenc.Bytes()
	if err != nil {
		return nil, err
	}
	// Encoded float values
	vb, err := venc.Bytes()
	if err != nil {
		return nil, err
	}

	// Prepend the first timestamp of the block in the first 8 bytes and the block
	// in the next byte, followed by the block
	block := packBlockHeader(values[0].Time(), BlockFloat64)
	block = append(block, packBlock(tb, vb)...)
	return block, nil
}

func decodeFloatBlock(block []byte, a *[]Value) error {
	// The first 8 bytes is the minimum timestamp of the block
	block = block[8:]

	// Block type is the next block, make sure we actually have a float block
	blockType := block[0]
	if blockType != BlockFloat64 {
		return fmt.Errorf("invalid block type: exp %d, got %d", BlockFloat64, blockType)
	}
	block = block[1:]

	tb, vb := unpackBlock(block)

	// Setup our timestamp and value decoders
	dec := NewTimeDecoder(tb)
	iter, err := NewFloatDecoder(vb)
	if err != nil {
		return err
	}

	// Decode both a timestamp and value
	for dec.Next() && iter.Next() {
		ts := dec.Read()
		v := iter.Values()
		*a = append(*a, &FloatValue{ts, v})
	}

	// Did timestamp decoding have an error?
	if dec.Error() != nil {
		return dec.Error()
	}
	// Did float decoding have an error?
	if iter.Error() != nil {
		return iter.Error()
	}

	return nil
}

type BoolValue struct {
	time  time.Time
	value bool
}

func (b *BoolValue) Time() time.Time {
	return b.time
}

func (b *BoolValue) Size() int {
	return 9
}

func (b *BoolValue) UnixNano() int64 {
	return b.time.UnixNano()
}

func (b *BoolValue) Value() interface{} {
	return b.value
}

func encodeBoolBlock(buf []byte, values []Value) ([]byte, error) {
	if len(values) == 0 {
		return nil, nil
	}

	// A bool block is encoded using different compression strategies
	// for timestamps and values.

	// Encode values using Gorilla float compression
	venc := NewBoolEncoder()

	// Encode timestamps using an adaptive encoder
	tsenc := NewTimeEncoder()

	for _, v := range values {
		tsenc.Write(v.Time())
		venc.Write(v.(*BoolValue).value)
	}

	// Encoded timestamp values
	tb, err := tsenc.Bytes()
	if err != nil {
		return nil, err
	}
	// Encoded float values
	vb, err := venc.Bytes()
	if err != nil {
		return nil, err
	}

	// Prepend the first timestamp of the block in the first 8 bytes and the block
	// in the next byte, followed by the block
	block := packBlockHeader(values[0].Time(), BlockBool)
	block = append(block, packBlock(tb, vb)...)
	return block, nil
}

func decodeBoolBlock(block []byte, a *[]Value) error {
	// The first 8 bytes is the minimum timestamp of the block
	block = block[8:]

	// Block type is the next block, make sure we actually have a float block
	blockType := block[0]
	if blockType != BlockBool {
		return fmt.Errorf("invalid block type: exp %d, got %d", BlockBool, blockType)
	}
	block = block[1:]

	tb, vb := unpackBlock(block)

	// Setup our timestamp and value decoders
	dec := NewTimeDecoder(tb)
	vdec := NewBoolDecoder(vb)

	// Decode both a timestamp and value
	for dec.Next() && vdec.Next() {
		ts := dec.Read()
		v := vdec.Read()
		*a = append(*a, &BoolValue{ts, v})
	}

	// Did timestamp decoding have an error?
	if dec.Error() != nil {
		return dec.Error()
	}
	// Did bool decoding have an error?
	if vdec.Error() != nil {
		return vdec.Error()
	}

	return nil
}

type Int64Value struct {
	time  time.Time
	value int64
}

func (v *Int64Value) Time() time.Time {
	return v.time
}

func (v *Int64Value) Value() interface{} {
	return v.value
}

func (v *Int64Value) UnixNano() int64 {
	return v.time.UnixNano()
}

func (v *Int64Value) Size() int {
	return 16
}

func (v *Int64Value) String() string { return fmt.Sprintf("%v", v.value) }

func encodeInt64Block(buf []byte, values []Value) ([]byte, error) {
	tsEnc := NewTimeEncoder()
	vEnc := NewInt64Encoder()
	for _, v := range values {
		tsEnc.Write(v.Time())
		vEnc.Write(v.(*Int64Value).value)
	}

	// Encoded timestamp values
	tb, err := tsEnc.Bytes()
	if err != nil {
		return nil, err
	}
	// Encoded int64 values
	vb, err := vEnc.Bytes()
	if err != nil {
		return nil, err
	}

	// Prepend the first timestamp of the block in the first 8 bytes
	block := packBlockHeader(values[0].Time(), BlockInt64)
	return append(block, packBlock(tb, vb)...), nil
}

func decodeInt64Block(block []byte, a *[]Value) error {
	// slice off the first 8 bytes (min timestmap for the block)
	block = block[8:]

	blockType := block[0]
	if blockType != BlockInt64 {
		return fmt.Errorf("invalid block type: exp %d, got %d", BlockInt64, blockType)
	}

	block = block[1:]

	// The first 8 bytes is the minimum timestamp of the block
	tb, vb := unpackBlock(block)

	// Setup our timestamp and value decoders
	tsDec := NewTimeDecoder(tb)
	vDec := NewInt64Decoder(vb)

	// Decode both a timestamp and value
	for tsDec.Next() && vDec.Next() {
		ts := tsDec.Read()
		v := vDec.Read()
		*a = append(*a, &Int64Value{ts, v})
	}

	// Did timestamp decoding have an error?
	if tsDec.Error() != nil {
		return tsDec.Error()
	}
	// Did int64 decoding have an error?
	if vDec.Error() != nil {
		return vDec.Error()
	}

	return nil
}

type StringValue struct {
	time  time.Time
	value string
}

func (v *StringValue) Time() time.Time {
	return v.time
}

func (v *StringValue) Value() interface{} {
	return v.value
}

func (v *StringValue) UnixNano() int64 {
	return v.time.UnixNano()
}

func (v *StringValue) Size() int {
	return 8 + len(v.value)
}

func (v *StringValue) String() string { return v.value }

func encodeStringBlock(buf []byte, values []Value) ([]byte, error) {
	tsEnc := NewTimeEncoder()
	vEnc := NewStringEncoder()
	for _, v := range values {
		tsEnc.Write(v.Time())
		vEnc.Write(v.(*StringValue).value)
	}

	// Encoded timestamp values
	tb, err := tsEnc.Bytes()
	if err != nil {
		return nil, err
	}
	// Encoded string values
	vb, err := vEnc.Bytes()
	if err != nil {
		return nil, err
	}

	// Prepend the first timestamp of the block in the first 8 bytes
	block := packBlockHeader(values[0].Time(), BlockString)
	return append(block, packBlock(tb, vb)...), nil
}

func decodeStringBlock(block []byte, a *[]Value) error {
	// slice off the first 8 bytes (min timestmap for the block)
	block = block[8:]

	blockType := block[0]
	if blockType != BlockString {
		return fmt.Errorf("invalid block type: exp %d, got %d", BlockString, blockType)
	}

	block = block[1:]

	// The first 8 bytes is the minimum timestamp of the block
	tb, vb := unpackBlock(block)

	// Setup our timestamp and value decoders
	tsDec := NewTimeDecoder(tb)
	vDec, err := NewStringDecoder(vb)
	if err != nil {
		return err
	}

	// Decode both a timestamp and value
	for tsDec.Next() && vDec.Next() {
		ts := tsDec.Read()
		v := vDec.Read()
		*a = append(*a, &StringValue{ts, v})
	}

	// Did timestamp decoding have an error?
	if tsDec.Error() != nil {
		return tsDec.Error()
	}
	// Did string decoding have an error?
	if vDec.Error() != nil {
		return vDec.Error()
	}

	return nil
}

func packBlockHeader(firstTime time.Time, blockType byte) []byte {
	return append(u64tob(uint64(firstTime.UnixNano())), blockType)
}

func packBlock(ts []byte, values []byte) []byte {
	// We encode the length of the timestamp block using a variable byte encoding.
	// This allows small byte slices to take up 1 byte while larger ones use 2 or more.
	b := make([]byte, 10)
	i := binary.PutUvarint(b, uint64(len(ts)))

	// block is <len timestamp bytes>, <ts bytes>, <value bytes>
	block := append(b[:i], ts...)

	// We don't encode the value length because we know it's the rest of the block after
	// the timestamp block.
	return append(block, values...)
}

func unpackBlock(buf []byte) (ts, values []byte) {
	// Unpack the timestamp block length
	tsLen, i := binary.Uvarint(buf)

	// Unpack the timestamp bytes
	ts = buf[int(i) : int(i)+int(tsLen)]

	// Unpack the value bytes
	values = buf[int(i)+int(tsLen):]
	return
}

// ZigZagEncode converts a int64 to a uint64 by zig zagging negative and positive values
// across even and odd numbers.  Eg. [0,-1,1,-2] becomes [0, 1, 2, 3]
func ZigZagEncode(x int64) uint64 {
	return uint64(uint64(x<<1) ^ uint64((int64(x) >> 63)))
}

// ZigZagDecode converts a previously zigzag encoded uint64 back to a int64
func ZigZagDecode(v uint64) int64 {
	return int64((v >> 1) ^ uint64((int64(v&1)<<63)>>63))
}
