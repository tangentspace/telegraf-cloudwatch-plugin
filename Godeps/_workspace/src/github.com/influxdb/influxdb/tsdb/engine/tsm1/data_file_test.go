package tsm1_test

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"

	"github.com/influxdb/influxdb/tsdb/engine/tsm1"
)

func TestTSMWriter_Write_Empty(t *testing.T) {
	var b bytes.Buffer
	w, err := tsm1.NewTSMWriter(&b)
	if err != nil {
		t.Fatalf("unexpected error created writer: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("unexpeted error closing: %v", err)
	}

	if got, exp := len(b.Bytes()), 5; got < exp {
		t.Fatalf("file size mismatch: got %v, exp %v", got, exp)
	}
	if got := binary.BigEndian.Uint32(b.Bytes()[0:4]); got != tsm1.MagicNumber {
		t.Fatalf("magic number mismatch: got %v, exp %v", got, tsm1.MagicNumber)
	}
}

func TestTSMWriter_Write_Single(t *testing.T) {
	var b bytes.Buffer
	w, err := tsm1.NewTSMWriter(&b)
	if err != nil {
		t.Fatalf("unexpected error creating writer: %v", err)
	}

	values := []tsm1.Value{tsm1.NewValue(time.Unix(0, 0), 1.0)}
	if err := w.Write("cpu", values); err != nil {
		t.Fatalf("unexpeted error writing: %v", err)

	}
	if err := w.Close(); err != nil {
		t.Fatalf("unexpeted error closing: %v", err)
	}

	if got, exp := len(b.Bytes()), 5; got < exp {
		t.Fatalf("file size mismatch: got %v, exp %v", got, exp)
	}
	if got := binary.BigEndian.Uint32(b.Bytes()[0:4]); got != tsm1.MagicNumber {
		t.Fatalf("magic number mismatch: got %v, exp %v", got, tsm1.MagicNumber)
	}

	r, err := tsm1.NewTSMReader(bytes.NewReader(b.Bytes()))
	if err != nil {
		t.Fatalf("unexpected error created reader: %v", err)
	}

	readValues, err := r.ReadAll("cpu")
	if err != nil {
		t.Fatalf("unexpeted error readin: %v", err)
	}

	if len(readValues) != len(values) {
		t.Fatalf("read values length mismatch: got %v, exp %v", len(readValues), len(values))
	}

	for i, v := range values {
		if v.Value() != readValues[i].Value() {
			t.Fatalf("read value mismatch(%d): got %v, exp %d", i, readValues[i].Value(), v.Value())
		}
	}
}

func TestTSMWriter_Write_Multiple(t *testing.T) {
	var b bytes.Buffer
	w, err := tsm1.NewTSMWriter(&b)
	if err != nil {
		t.Fatalf("unexpected error creating writer: %v", err)
	}

	var data = []struct {
		key    string
		values []tsm1.Value
	}{
		{"cpu", []tsm1.Value{tsm1.NewValue(time.Unix(0, 0), 1.0)}},
		{"mem", []tsm1.Value{tsm1.NewValue(time.Unix(1, 0), 2.0)}},
	}

	for _, d := range data {
		if err := w.Write(d.key, d.values); err != nil {
			t.Fatalf("unexpeted error writing: %v", err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("unexpeted error closing: %v", err)
	}

	r, err := tsm1.NewTSMReader(bytes.NewReader(b.Bytes()))
	if err != nil {
		t.Fatalf("unexpected error created reader: %v", err)
	}

	for _, d := range data {
		readValues, err := r.ReadAll(d.key)
		if err != nil {
			t.Fatalf("unexpeted error readin: %v", err)
		}

		if exp := len(d.values); exp != len(readValues) {
			t.Fatalf("read values length mismatch: got %v, exp %v", len(readValues), exp)
		}

		for i, v := range d.values {
			if v.Value() != readValues[i].Value() {
				t.Fatalf("read value mismatch(%d): got %v, exp %d", i, readValues[i].Value(), v.Value())
			}
		}
	}
}

func TestTSMWriter_Write_MultipleKeyValues(t *testing.T) {
	var b bytes.Buffer
	w, err := tsm1.NewTSMWriter(&b)
	if err != nil {
		t.Fatalf("unexpected error creating writer: %v", err)
	}

	var data = []struct {
		key    string
		values []tsm1.Value
	}{
		{"cpu", []tsm1.Value{
			tsm1.NewValue(time.Unix(0, 0), 1.0),
			tsm1.NewValue(time.Unix(1, 0), 2.0)},
		},
		{"mem", []tsm1.Value{
			tsm1.NewValue(time.Unix(0, 0), 1.5),
			tsm1.NewValue(time.Unix(1, 0), 2.5)},
		},
	}

	for _, d := range data {
		if err := w.Write(d.key, d.values); err != nil {
			t.Fatalf("unexpeted error writing: %v", err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("unexpeted error closing: %v", err)
	}

	r, err := tsm1.NewTSMReader(bytes.NewReader(b.Bytes()))
	if err != nil {
		t.Fatalf("unexpected error created reader: %v", err)
	}

	for _, d := range data {
		readValues, err := r.ReadAll(d.key)
		if err != nil {
			t.Fatalf("unexpeted error readin: %v", err)
		}

		if exp := len(d.values); exp != len(readValues) {
			t.Fatalf("read values length mismatch: got %v, exp %v", len(readValues), exp)
		}

		for i, v := range d.values {
			if v.Value() != readValues[i].Value() {
				t.Fatalf("read value mismatch(%d): got %v, exp %d", i, readValues[i].Value(), v.Value())
			}
		}
	}
}

// Tests that writing keys in reverse is able to read them back.
func TestTSMWriter_Write_ReverseKeys(t *testing.T) {
	var b bytes.Buffer
	w, err := tsm1.NewTSMWriter(&b)
	if err != nil {
		t.Fatalf("unexpected error creating writer: %v", err)
	}

	var data = []struct {
		key    string
		values []tsm1.Value
	}{
		{"mem", []tsm1.Value{
			tsm1.NewValue(time.Unix(0, 0), 1.5),
			tsm1.NewValue(time.Unix(1, 0), 2.5)},
		},
		{"cpu", []tsm1.Value{
			tsm1.NewValue(time.Unix(0, 0), 1.0),
			tsm1.NewValue(time.Unix(1, 0), 2.0)},
		},
	}

	for _, d := range data {
		if err := w.Write(d.key, d.values); err != nil {
			t.Fatalf("unexpeted error writing: %v", err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("unexpeted error closing: %v", err)
	}

	r, err := tsm1.NewTSMReader(bytes.NewReader(b.Bytes()))
	if err != nil {
		t.Fatalf("unexpected error created reader: %v", err)
	}

	for _, d := range data {
		readValues, err := r.ReadAll(d.key)
		if err != nil {
			t.Fatalf("unexpeted error readin: %v", err)
		}

		if exp := len(d.values); exp != len(readValues) {
			t.Fatalf("read values length mismatch: got %v, exp %v", len(readValues), exp)
		}

		for i, v := range d.values {
			if v.Value() != readValues[i].Value() {
				t.Fatalf("read value mismatch(%d): got %v, exp %d", i, readValues[i].Value(), v.Value())
			}
		}
	}
}

// Tests that writing keys in reverse is able to read them back.
func TestTSMWriter_Write_SameKey(t *testing.T) {
	var b bytes.Buffer
	w, err := tsm1.NewTSMWriter(&b)
	if err != nil {
		t.Fatalf("unexpected error creating writer: %v", err)
	}

	var data = []struct {
		key    string
		values []tsm1.Value
	}{
		{"cpu", []tsm1.Value{
			tsm1.NewValue(time.Unix(0, 0), 1.0),
			tsm1.NewValue(time.Unix(1, 0), 2.0)},
		},
		{"cpu", []tsm1.Value{
			tsm1.NewValue(time.Unix(2, 0), 3.0),
			tsm1.NewValue(time.Unix(3, 0), 4.0)},
		},
	}

	for _, d := range data {
		if err := w.Write(d.key, d.values); err != nil {
			t.Fatalf("unexpeted error writing: %v", err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("unexpeted error closing: %v", err)
	}

	r, err := tsm1.NewTSMReader(bytes.NewReader(b.Bytes()))
	if err != nil {
		t.Fatalf("unexpected error created reader: %v", err)
	}

	values := append(data[0].values, data[1].values...)

	readValues, err := r.ReadAll("cpu")
	if err != nil {
		t.Fatalf("unexpeted error readin: %v", err)
	}

	if exp := len(values); exp != len(readValues) {
		t.Fatalf("read values length mismatch: got %v, exp %v", len(readValues), exp)
	}

	for i, v := range values {
		if v.Value() != readValues[i].Value() {
			t.Fatalf("read value mismatch(%d): got %v, exp %d", i, readValues[i].Value(), v.Value())
		}
	}
}

// Tests that calling Read returns all the values for block matching the key
// and timestamp
func TestTSMWriter_Read_Multiple(t *testing.T) {
	var b bytes.Buffer
	w, err := tsm1.NewTSMWriter(&b)
	if err != nil {
		t.Fatalf("unexpected error creating writer: %v", err)
	}

	var data = []struct {
		key    string
		values []tsm1.Value
	}{
		{"cpu", []tsm1.Value{
			tsm1.NewValue(time.Unix(0, 0), 1.0),
			tsm1.NewValue(time.Unix(1, 0), 2.0)},
		},
		{"cpu", []tsm1.Value{
			tsm1.NewValue(time.Unix(2, 0), 3.0),
			tsm1.NewValue(time.Unix(3, 0), 4.0)},
		},
	}

	for _, d := range data {
		if err := w.Write(d.key, d.values); err != nil {
			t.Fatalf("unexpeted error writing: %v", err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("unexpeted error closing: %v", err)
	}

	r, err := tsm1.NewTSMReader(bytes.NewReader(b.Bytes()))
	if err != nil {
		t.Fatalf("unexpected error created reader: %v", err)
	}

	for _, values := range data {
		// Try the first timestamp
		readValues, err := r.Read("cpu", values.values[0].Time())
		if err != nil {
			t.Fatalf("unexpeted error readin: %v", err)
		}

		if exp := len(values.values); exp != len(readValues) {
			t.Fatalf("read values length mismatch: got %v, exp %v", len(readValues), exp)
		}

		for i, v := range values.values {
			if v.Value() != readValues[i].Value() {
				t.Fatalf("read value mismatch(%d): got %v, exp %d", i, readValues[i].Value(), v.Value())
			}
		}

		// Try the last timestamp too
		readValues, err = r.Read("cpu", values.values[1].Time())
		if err != nil {
			t.Fatalf("unexpeted error readin: %v", err)
		}

		if exp := len(values.values); exp != len(readValues) {
			t.Fatalf("read values length mismatch: got %v, exp %v", len(readValues), exp)
		}

		for i, v := range values.values {
			if v.Value() != readValues[i].Value() {
				t.Fatalf("read value mismatch(%d): got %v, exp %d", i, readValues[i].Value(), v.Value())
			}
		}

	}
}

func TestIndirectIndex_Entries(t *testing.T) {
	index := tsm1.NewDirectIndex()
	index.Add("cpu", time.Unix(0, 0), time.Unix(1, 0), 10, 100)
	index.Add("cpu", time.Unix(2, 0), time.Unix(3, 0), 20, 200)
	index.Add("mem", time.Unix(0, 0), time.Unix(1, 0), 10, 100)

	b, err := index.MarshalBinary()
	if err != nil {
		t.Fatalf("unexpected error marshaling index: %v", err)
	}

	indirect := tsm1.NewIndirectIndex()
	if err := indirect.UnmarshalBinary(b); err != nil {
		t.Fatalf("unexpected error unmarshaling index: %v", err)
	}

	exp := index.Entries("cpu")
	entries := indirect.Entries("cpu")

	if got, exp := len(entries), len(exp); got != exp {
		t.Fatalf("entries length mismatch: got %v, exp %v", got, exp)
	}

	for i, exp := range exp {
		got := entries[i]
		if exp.MinTime != got.MinTime {
			t.Fatalf("minTime mismatch: got %v, exp %v", got.MinTime, exp.MinTime)
		}

		if exp.MaxTime != got.MaxTime {
			t.Fatalf("minTime mismatch: got %v, exp %v", got.MaxTime, exp.MaxTime)
		}

		if exp.Size != got.Size {
			t.Fatalf("size mismatch: got %v, exp %v", got.Size, exp.Size)
		}
		if exp.Offset != got.Offset {
			t.Fatalf("size mismatch: got %v, exp %v", got.Offset, exp.Offset)
		}
	}
}

func TestIndirectIndex_Entries_NonExistent(t *testing.T) {
	index := tsm1.NewDirectIndex()
	index.Add("cpu", time.Unix(0, 0), time.Unix(1, 0), 10, 100)
	index.Add("cpu", time.Unix(2, 0), time.Unix(3, 0), 20, 200)

	b, err := index.MarshalBinary()
	if err != nil {
		t.Fatalf("unexpected error marshaling index: %v", err)
	}

	indirect := tsm1.NewIndirectIndex()
	if err := indirect.UnmarshalBinary(b); err != nil {
		t.Fatalf("unexpected error unmarshaling index: %v", err)
	}

	// mem has not been added to the index so we should get now entries back
	// for both
	exp := index.Entries("mem")
	entries := indirect.Entries("mem")

	if got, exp := len(entries), len(exp); got != exp && exp != 0 {
		t.Fatalf("entries length mismatch: got %v, exp %v", got, exp)
	}
}
