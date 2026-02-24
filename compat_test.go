//go:build cgo

package sdbm

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/vvatanabe/go-sdbm/internal/csdbm"
)

// ============================================================================
// Layer 1: Hash Function Compatibility Tests
// ============================================================================

func TestHash_ASCIICompatibility(t *testing.T) {
	tests := [][]byte{
		{},
		[]byte("a"),
		[]byte("hello"),
		[]byte("key1"),
		[]byte("sdbm"),
		[]byte("The quick brown fox jumps over the lazy dog"),
	}

	for _, input := range tests {
		goHash := Hash(input)
		cHashUnsigned := csdbm.HashUnsigned(input)
		cHashSigned := csdbm.Hash(input)

		if goHash != cHashUnsigned {
			t.Errorf("Hash(%q): Go=%d, C-unsigned=%d", input, goHash, cHashUnsigned)
		}
		// For pure ASCII, signed and unsigned should also match
		if cHashSigned != cHashUnsigned {
			t.Errorf("Hash(%q): C-signed=%d != C-unsigned=%d (unexpected for ASCII)", input, cHashSigned, cHashUnsigned)
		}
	}
}

func TestHash_KeyRangeCompatibility(t *testing.T) {
	for i := 1; i <= 100000; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		goHash := Hash(key)
		cHash := csdbm.HashUnsigned(key)

		if goHash != cHash {
			t.Fatalf("Hash(%q): Go=%d, C-unsigned=%d", key, goHash, cHash)
		}
	}
}

func TestHash_LargeInputCompatibility(t *testing.T) {
	sizes := []int{100, 1000, 10000}

	for _, size := range sizes {
		input := bytes.Repeat([]byte("x"), size)
		goHash := Hash(input)
		cHash := csdbm.HashUnsigned(input)

		if goHash != cHash {
			t.Errorf("Hash(repeat 'x' * %d): Go=%d, C-unsigned=%d", size, goHash, cHash)
		}
	}
}

func TestHash_BinaryDataDocumentation(t *testing.T) {
	// Test with bytes >= 0x80 to document signed/unsigned char differences
	inputs := [][]byte{
		{0x80},
		{0xFF},
		{0x00, 0x80, 0xFF},
		{0x7F, 0x80}, // boundary between signed positive and negative
	}

	for _, input := range inputs {
		goHash := Hash(input)
		cHashUnsigned := csdbm.HashUnsigned(input)
		cHashSigned := csdbm.Hash(input)

		if goHash != cHashUnsigned {
			t.Errorf("Hash(%x): Go=%d, C-unsigned=%d — MISMATCH", input, goHash, cHashUnsigned)
		}

		if cHashSigned != cHashUnsigned {
			t.Logf("Hash(%x): C-signed=%d, C-unsigned=%d, Go=%d — signed/unsigned difference documented",
				input, cHashSigned, cHashUnsigned, goHash)
		}
	}
}

// ============================================================================
// Layer 2: Page Operation Binary Compatibility Tests
// ============================================================================

func TestPageOps_PutPairCompatibility(t *testing.T) {
	pairs := [][2][]byte{
		{[]byte("key1"), []byte("val1")},
		{[]byte("hello"), []byte("world")},
		{[]byte("k"), []byte("v")},
	}

	goPage := &Page{}
	cBuf := make([]byte, PBLKSIZ)

	for _, pair := range pairs {
		goPage.PutPair(Datum(pair[0]), Datum(pair[1]))
		csdbm.PutPair(cBuf, pair[0], pair[1])
	}

	if !bytes.Equal(goPage.buf[:], cBuf) {
		t.Error("PutPair: Go and C page buffers differ after inserting same pairs")
		diffPageBuffers(t, goPage.buf[:], cBuf)
	}
}

func TestPageOps_GetPairCompatibility(t *testing.T) {
	keys := [][]byte{
		[]byte("key1"), []byte("key2"), []byte("key3"),
	}
	vals := [][]byte{
		[]byte("val1"), []byte("val2"), []byte("val3"),
	}

	goPage := &Page{}
	cBuf := make([]byte, PBLKSIZ)

	for i := range keys {
		goPage.PutPair(Datum(keys[i]), Datum(vals[i]))
		csdbm.PutPair(cBuf, keys[i], vals[i])
	}

	for _, key := range keys {
		goVal := goPage.GetPair(Datum(key))
		cVal := csdbm.GetPair(cBuf, key)

		if !bytes.Equal(goVal, cVal) {
			t.Errorf("GetPair(%q): Go=%q, C=%q", key, goVal, cVal)
		}
	}

	// Test nonexistent key
	goVal := goPage.GetPair(Datum("nonexistent"))
	cVal := csdbm.GetPair(cBuf, []byte("nonexistent"))
	if goVal != nil && cVal != nil {
		t.Errorf("GetPair(nonexistent): Go=%v, C=%v", goVal, cVal)
	}
}

func TestPageOps_DelPairCompatibility(t *testing.T) {
	keys := [][]byte{
		[]byte("keyA"), []byte("keyB"), []byte("keyC"),
	}
	vals := [][]byte{
		[]byte("valueA_padding_1234567890"),
		[]byte("valueB_padding_1234567890"),
		[]byte("valueC_padding_1234567890"),
	}

	goPage := &Page{}
	cBuf := make([]byte, PBLKSIZ)

	for i := range keys {
		goPage.PutPair(Datum(keys[i]), Datum(vals[i]))
		csdbm.PutPair(cBuf, keys[i], vals[i])
	}

	// Delete middle entry
	goOk := goPage.DelPair(Datum(keys[1]))
	cOk := csdbm.DelPair(cBuf, keys[1])

	if goOk != cOk {
		t.Errorf("DelPair(%q): Go=%v, C=%v", keys[1], goOk, cOk)
	}

	if !bytes.Equal(goPage.buf[:], cBuf) {
		t.Error("DelPair: Go and C page buffers differ after deleting middle entry")
		diffPageBuffers(t, goPage.buf[:], cBuf)
	}

	// Verify remaining entries are intact
	for _, i := range []int{0, 2} {
		goVal := goPage.GetPair(Datum(keys[i]))
		cVal := csdbm.GetPair(cBuf, keys[i])
		if !bytes.Equal(goVal, cVal) {
			t.Errorf("After delete, GetPair(%q): Go=%q, C=%q", keys[i], goVal, cVal)
		}
	}
}

func TestPageOps_SplPageCompatibility(t *testing.T) {
	keys := make([][]byte, 20)
	vals := make([][]byte, 20)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key%03d", i+1))
		vals[i] = []byte(fmt.Sprintf("val%03d", i+1))
	}

	goPage := &Page{}
	cBuf := make([]byte, PBLKSIZ)

	for i := range keys {
		goPage.PutPair(Datum(keys[i]), Datum(vals[i]))
		csdbm.PutPair(cBuf, keys[i], vals[i])
	}

	// Verify pages match before split
	if !bytes.Equal(goPage.buf[:], cBuf) {
		t.Fatal("Pages differ before split")
	}

	goNew := &Page{}
	cNewBuf := make([]byte, PBLKSIZ)

	var sbit int64 = 1

	goPage.SplPage(goNew, sbit)
	csdbm.SplPage(cBuf, cNewBuf, sbit)

	if !bytes.Equal(goPage.buf[:], cBuf) {
		t.Error("SplPage: original pages differ after split")
		diffPageBuffers(t, goPage.buf[:], cBuf)
	}

	if !bytes.Equal(goNew.buf[:], cNewBuf) {
		t.Error("SplPage: new pages differ after split")
		diffPageBuffers(t, goNew.buf[:], cNewBuf)
	}
}

func TestPageOps_FitPairCompatibility(t *testing.T) {
	goPage := &Page{}
	cBuf := make([]byte, PBLKSIZ)

	// Empty page
	needs := []int{0, 10, 100, 500, PAIRMAX}
	for _, need := range needs {
		goFit := goPage.FitPair(need)
		cFit := csdbm.FitPair(cBuf, need)
		if goFit != cFit {
			t.Errorf("FitPair(empty, %d): Go=%v, C=%v", need, goFit, cFit)
		}
	}

	// Partially filled page
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		val := []byte(fmt.Sprintf("value%d", i))
		goPage.PutPair(Datum(key), Datum(val))
		csdbm.PutPair(cBuf, key, val)
	}

	for _, need := range needs {
		goFit := goPage.FitPair(need)
		cFit := csdbm.FitPair(cBuf, need)
		if goFit != cFit {
			t.Errorf("FitPair(partial, %d): Go=%v, C=%v", need, goFit, cFit)
		}
	}
}

func TestPageOps_ChkPageCompatibility(t *testing.T) {
	// Valid empty page
	goPage := &Page{}
	cBuf := make([]byte, PBLKSIZ)
	if goPage.ChkPage() != csdbm.ChkPage(cBuf) {
		t.Error("ChkPage(empty): Go and C disagree")
	}

	// Valid page with entries
	goPage.PutPair(Datum("key1"), Datum("val1"))
	csdbm.PutPair(cBuf, []byte("key1"), []byte("val1"))
	if goPage.ChkPage() != csdbm.ChkPage(cBuf) {
		t.Error("ChkPage(with entry): Go and C disagree")
	}

	// Invalid page: set entry count to impossibly large value
	invalidBuf := make([]byte, PBLKSIZ)
	invalidBuf[0] = 0xFF
	invalidBuf[1] = 0xFF
	goInvalid := &Page{}
	copy(goInvalid.buf[:], invalidBuf)
	goResult := goInvalid.ChkPage()
	cResult := csdbm.ChkPage(invalidBuf)
	if goResult != cResult {
		t.Errorf("ChkPage(invalid large n): Go=%v, C=%v", goResult, cResult)
	}
}

// ============================================================================
// BUG-6 Tests (internal access needed)
// ============================================================================

func TestPageOps_BUG6_OddEntryCount(t *testing.T) {
	// Create a corrupted page with odd entry count
	p := &Page{}
	// Insert two entries normally
	p.PutPair(Datum("key1"), Datum("val1"))
	p.PutPair(Datum("key2"), Datum("val2"))

	// Corrupt: set entry count to 3 (odd)
	p.setN(3)

	newPag := &Page{}

	// SplPage should not panic on corrupted page with odd entry count
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("SplPage panicked on odd entry count page: %v", r)
		}
	}()

	p.SplPage(newPag, 1)
}

func TestPageOps_BUG6_ChkPageOddEntries(t *testing.T) {
	p := &Page{}
	p.PutPair(Datum("key1"), Datum("val1"))

	// Corrupt: set entry count to odd value
	p.setN(1)

	// ChkPage should reject odd entry count
	if p.ChkPage() {
		t.Error("ChkPage should reject page with odd entry count (n=1)")
	}

	p.setN(3)
	if p.ChkPage() {
		t.Error("ChkPage should reject page with odd entry count (n=3)")
	}
}

// ============================================================================
// Helpers
// ============================================================================

func diffPageBuffers(t *testing.T, goBuf, cBuf []byte) {
	t.Helper()
	diffs := 0
	for i := 0; i < len(goBuf) && i < len(cBuf); i++ {
		if goBuf[i] != cBuf[i] {
			if diffs < 10 {
				t.Logf("  byte[%d]: Go=0x%02x, C=0x%02x", i, goBuf[i], cBuf[i])
			}
			diffs++
		}
	}
	if diffs > 10 {
		t.Logf("  ... and %d more byte differences", diffs-10)
	}
}
