//go:build cgo

package sdbm

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
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
	// Test with bytes >= 0x80 to document signed/unsigned char differences.
	// Go's Hash now uses signed char semantics to match C's default behavior.
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

		if goHash != cHashSigned {
			t.Errorf("Hash(%x): Go=%d, C-signed=%d — MISMATCH", input, goHash, cHashSigned)
		}

		if cHashSigned != cHashUnsigned {
			t.Logf("Hash(%x): C-signed=%d, C-unsigned=%d, Go=%d — signed/unsigned difference documented",
				input, cHashSigned, cHashUnsigned, goHash)
		}
	}
}

// ============================================================================
// Layer 1.5: Signed Char Compatibility Tests (TDD Red-Green target)
// ============================================================================

func TestHash_SignedCharCompatibility(t *testing.T) {
	// This test verifies that Go's Hash function produces the same results
	// as C's dbm_hash with the default signed char behavior.
	// On x86-64, C's char is signed by default, so bytes >= 0x80 are
	// interpreted as negative values (-128 to -1).
	//
	// TDD Red: With the current unsigned implementation, this test FAILS.
	// TDD Green: After fixing Hash to use signed char semantics, this test PASSES.
	inputs := [][]byte{
		{0x80},
		{0xFF},
		{0x00, 0x80, 0xFF},
		{0x7F, 0x80},                         // boundary between signed positive and negative
		{0xE6, 0x9D, 0xB1},                   // UTF-8 "東"
		{0xE6, 0x9D, 0xB1, 0xE4, 0xBA, 0xAC}, // UTF-8 "東京"
		{0xC3, 0xA9},                          // UTF-8 "é"
	}

	for _, input := range inputs {
		goHash := Hash(input)
		cHashSigned := csdbm.Hash(input)

		if goHash != cHashSigned {
			t.Errorf("Hash(%x): Go=%d, C-signed=%d — MISMATCH", input, goHash, cHashSigned)
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
// Layer 2 (additional): GetNKey Compatibility Tests
// ============================================================================

func TestPageOps_GetNKeyCompatibility(t *testing.T) {
	keys := [][]byte{
		[]byte("alpha"), []byte("bravo"), []byte("charlie"),
	}
	vals := [][]byte{
		[]byte("val_a"), []byte("val_b"), []byte("val_c"),
	}

	goPage := &Page{}
	cBuf := make([]byte, PBLKSIZ)

	for i := range keys {
		goPage.PutPair(Datum(keys[i]), Datum(vals[i]))
		csdbm.PutPair(cBuf, keys[i], vals[i])
	}

	// Test all valid 1-based indices
	for num := 1; num <= len(keys); num++ {
		goKey := goPage.GetNKey(num)
		cKey := csdbm.GetNKey(cBuf, num)

		if !bytes.Equal(goKey, cKey) {
			t.Errorf("GetNKey(%d): Go=%q, C=%q", num, goKey, cKey)
		}
	}

	// Test out-of-range index
	goKey := goPage.GetNKey(len(keys) + 1)
	cKey := csdbm.GetNKey(cBuf, len(keys)+1)
	if (goKey == nil) != (cKey == nil) {
		t.Errorf("GetNKey(out-of-range): Go=%v, C=%v", goKey, cKey)
	}

	// Test on empty page
	emptyGo := &Page{}
	emptyCBuf := make([]byte, PBLKSIZ)
	goKey = emptyGo.GetNKey(1)
	cKey = csdbm.GetNKey(emptyCBuf, 1)
	if (goKey == nil) != (cKey == nil) {
		t.Errorf("GetNKey(empty, 1): Go=%v, C=%v", goKey, cKey)
	}
}

// ============================================================================
// Layer 2 (additional): DupPair Compatibility Tests
// ============================================================================

func TestPageOps_DupPairCompatibility(t *testing.T) {
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

	// Test existing keys
	for _, key := range keys {
		goDup := goPage.DupPair(Datum(key))
		cDup := csdbm.DupPair(cBuf, key)
		if goDup != cDup {
			t.Errorf("DupPair(%q): Go=%v, C=%v", key, goDup, cDup)
		}
	}

	// Test non-existent key
	goDup := goPage.DupPair(Datum("nonexistent"))
	cDup := csdbm.DupPair(cBuf, []byte("nonexistent"))
	if goDup != cDup {
		t.Errorf("DupPair(nonexistent): Go=%v, C=%v", goDup, cDup)
	}

	// Test on empty page
	emptyGo := &Page{}
	emptyCBuf := make([]byte, PBLKSIZ)
	goDup = emptyGo.DupPair(Datum("key1"))
	cDup = csdbm.DupPair(emptyCBuf, []byte("key1"))
	if goDup != cDup {
		t.Errorf("DupPair(empty, key1): Go=%v, C=%v", goDup, cDup)
	}
}

// ============================================================================
// Layer 2 (additional): DelPair First/Last Entry Compatibility Tests
// ============================================================================

func TestPageOps_DelPairFirstEntryCompatibility(t *testing.T) {
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

	// Delete first entry (i==1 in the internal index, triggers PBLKSIZ path in dst calculation)
	goOk := goPage.DelPair(Datum(keys[0]))
	cOk := csdbm.DelPair(cBuf, keys[0])

	if goOk != cOk {
		t.Errorf("DelPair(%q): Go=%v, C=%v", keys[0], goOk, cOk)
	}

	if !bytes.Equal(goPage.buf[:], cBuf) {
		t.Error("DelPair first entry: Go and C page buffers differ")
		diffPageBuffers(t, goPage.buf[:], cBuf)
	}

	// Verify remaining entries are intact
	for _, i := range []int{1, 2} {
		goVal := goPage.GetPair(Datum(keys[i]))
		cVal := csdbm.GetPair(cBuf, keys[i])
		if !bytes.Equal(goVal, cVal) {
			t.Errorf("After delete first, GetPair(%q): Go=%q, C=%q", keys[i], goVal, cVal)
		}
	}
}

func TestPageOps_DelPairLastEntryCompatibility(t *testing.T) {
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

	// Delete last entry (i==n-1 case, only adjusts entry count without data shift)
	lastKey := keys[len(keys)-1]
	goOk := goPage.DelPair(Datum(lastKey))
	cOk := csdbm.DelPair(cBuf, lastKey)

	if goOk != cOk {
		t.Errorf("DelPair(%q): Go=%v, C=%v", lastKey, goOk, cOk)
	}

	if !bytes.Equal(goPage.buf[:], cBuf) {
		t.Error("DelPair last entry: Go and C page buffers differ")
		diffPageBuffers(t, goPage.buf[:], cBuf)
	}

	// Verify remaining entries are intact
	for _, i := range []int{0, 1} {
		goVal := goPage.GetPair(Datum(keys[i]))
		cVal := csdbm.GetPair(cBuf, keys[i])
		if !bytes.Equal(goVal, cVal) {
			t.Errorf("After delete last, GetPair(%q): Go=%q, C=%q", keys[i], goVal, cVal)
		}
	}
}

// ============================================================================
// Layer 2 (additional): Empty Value Compatibility Tests
// ============================================================================

func TestPageOps_EmptyValueCompatibility(t *testing.T) {
	goPage := &Page{}
	cBuf := make([]byte, PBLKSIZ)

	key := []byte("mykey")
	val := []byte{} // empty value

	goPage.PutPair(Datum(key), Datum(val))
	csdbm.PutPair(cBuf, key, val)

	if !bytes.Equal(goPage.buf[:], cBuf) {
		t.Error("PutPair with empty value: Go and C page buffers differ")
		diffPageBuffers(t, goPage.buf[:], cBuf)
	}

	goVal := goPage.GetPair(Datum(key))
	cVal := csdbm.GetPair(cBuf, key)

	if len(goVal) != 0 || len(cVal) != 0 {
		t.Errorf("GetPair(empty value): Go=%q (len=%d), C=%q (len=%d)", goVal, len(goVal), cVal, len(cVal))
	}

	// Mix empty and non-empty values
	goPage2 := &Page{}
	cBuf2 := make([]byte, PBLKSIZ)

	pairs := [][2][]byte{
		{[]byte("k1"), []byte("v1")},
		{[]byte("k2"), {}},
		{[]byte("k3"), []byte("v3")},
	}

	for _, p := range pairs {
		goPage2.PutPair(Datum(p[0]), Datum(p[1]))
		csdbm.PutPair(cBuf2, p[0], p[1])
	}

	if !bytes.Equal(goPage2.buf[:], cBuf2) {
		t.Error("PutPair with mixed empty/non-empty values: buffers differ")
		diffPageBuffers(t, goPage2.buf[:], cBuf2)
	}

	for _, p := range pairs {
		goVal := goPage2.GetPair(Datum(p[0]))
		cVal := csdbm.GetPair(cBuf2, p[0])
		if !bytes.Equal(goVal, cVal) {
			t.Errorf("GetPair(%q): Go=%q, C=%q", p[0], goVal, cVal)
		}
	}
}

// ============================================================================
// Layer 5: Differential Random Operation Tests
// ============================================================================

// TestDB_DifferentialRandomOps exercises Store, Fetch, and Delete in random
// order on both Go and C SDBM implementations, then compares the resulting
// .dir and .pag file hashes to verify byte-level compatibility.
func TestDB_DifferentialRandomOps(t *testing.T) {
	seeds := []int64{42, 12345, 99999}
	for _, seed := range seeds {
		seed := seed
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			testDifferentialRandomOps(t, seed, 200, 2000, false)
		})
	}
}

func TestDB_DifferentialRandomOps_WithBinaryKeys(t *testing.T) {
	seeds := []int64{7, 314, 65536}
	for _, seed := range seeds {
		seed := seed
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			testDifferentialRandomOps(t, seed, 200, 2000, true)
		})
	}
}

func TestDB_DifferentialRandomOps_WithReplace(t *testing.T) {
	seeds := []int64{111, 222, 333}
	for _, seed := range seeds {
		seed := seed
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			testDifferentialRandomOpsWithReplace(t, seed, 150, 1500)
		})
	}
}

func testDifferentialRandomOps(t *testing.T, seed int64, numKeys, numOps int, binaryKeys bool) {
	t.Helper()

	rng := rand.New(rand.NewSource(seed))

	goDir := t.TempDir()
	cDir := t.TempDir()
	goPath := filepath.Join(goDir, "test")
	cPath := filepath.Join(cDir, "test")

	goDB, err := Open(goPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("Go Open: %v", err)
	}
	cDB, err := csdbm.OpenCDB(cPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("C Open: %v", err)
	}

	// Pre-generate keys
	keys := make([][]byte, numKeys)
	for i := 0; i < numKeys; i++ {
		if binaryKeys && i >= numKeys/2 {
			// Binary keys containing bytes >= 0x80
			klen := rng.Intn(10) + 1
			k := make([]byte, klen)
			for j := range k {
				k[j] = byte(rng.Intn(256))
			}
			// Avoid all-zero keys which would be rejected by bad()
			if allZero(k) {
				k[0] = 0x80
			}
			keys[i] = k
		} else {
			keys[i] = []byte(fmt.Sprintf("key%04d", i))
		}
	}

	// Run random operations
	var storeCount, fetchCount, deleteCount int
	var fetchMismatch int

	for i := 0; i < numOps; i++ {
		op := rng.Intn(10) // 0-4: store, 5-7: fetch, 8-9: delete
		key := keys[rng.Intn(numKeys)]

		switch {
		case op < 5: // 50% store (INSERT mode)
			valLen := rng.Intn(50) + 1
			val := make([]byte, valLen)
			for j := range val {
				val[j] = byte(rng.Intn(256))
			}
			goOk, goErr := goDB.Store(Datum(key), Datum(val), 0)
			cRet := cDB.Store(key, val, 0)

			// Map C return to Go semantics: 0=success, 1=dup(ok), -1=error
			if goErr != nil && cRet != -1 {
				t.Fatalf("op %d: Store(%x) Go err=%v but C ret=%d", i, key, goErr, cRet)
			}
			if goErr == nil {
				cOk := cRet == 0 || cRet == 1
				if goOk != cOk {
					t.Fatalf("op %d: Store(%x) Go ok=%v but C ret=%d", i, key, goOk, cRet)
				}
			}
			storeCount++

		case op < 8: // 30% fetch
			goVal, goErr := goDB.Fetch(Datum(key))
			cVal := cDB.Fetch(key)

			if goErr != nil {
				t.Fatalf("op %d: Fetch(%x) Go err=%v", i, key, goErr)
			}
			if !bytes.Equal(goVal, cVal) {
				fetchMismatch++
				if fetchMismatch <= 5 {
					t.Errorf("op %d: Fetch(%x) Go=%x, C=%x", i, key, goVal, cVal)
				}
			}
			fetchCount++

		default: // 20% delete
			goOk, goErr := goDB.Delete(Datum(key))
			cRet := cDB.Delete(key)

			if goErr != nil && cRet != -1 {
				t.Fatalf("op %d: Delete(%x) Go err=%v but C ret=%d", i, key, goErr, cRet)
			}
			// C returns 0 on success, -1 on not-found/error
			if goErr == nil {
				cOk := cRet == 0
				if goOk != cOk {
					t.Fatalf("op %d: Delete(%x) Go ok=%v but C ret=%d", i, key, goOk, cRet)
				}
			}
			deleteCount++
		}
	}

	goDB.Close()
	cDB.Close()

	t.Logf("Operations: store=%d fetch=%d delete=%d (fetchMismatch=%d)",
		storeCount, fetchCount, deleteCount, fetchMismatch)

	// Compare file hashes
	for _, ext := range []string{".dir", ".pag"} {
		goHash := diffFileHash(t, goPath+ext)
		cHash := diffFileHash(t, cPath+ext)
		if goHash != cHash {
			t.Errorf("test%s: Go SHA256=%s, C SHA256=%s — files differ", ext, goHash, cHash)
		}
	}
}

func testDifferentialRandomOpsWithReplace(t *testing.T, seed int64, numKeys, numOps int) {
	t.Helper()

	rng := rand.New(rand.NewSource(seed))

	goDir := t.TempDir()
	cDir := t.TempDir()
	goPath := filepath.Join(goDir, "test")
	cPath := filepath.Join(cDir, "test")

	goDB, err := Open(goPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("Go Open: %v", err)
	}
	cDB, err := csdbm.OpenCDB(cPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("C Open: %v", err)
	}

	// Mix of ASCII and binary keys
	keys := make([][]byte, numKeys)
	for i := 0; i < numKeys; i++ {
		if i < numKeys/2 {
			keys[i] = []byte(fmt.Sprintf("rkey%04d", i))
		} else {
			klen := rng.Intn(8) + 1
			k := make([]byte, klen)
			for j := range k {
				k[j] = byte(rng.Intn(256))
			}
			if allZero(k) {
				k[0] = 0xFF
			}
			keys[i] = k
		}
	}

	for i := 0; i < numOps; i++ {
		op := rng.Intn(10)
		key := keys[rng.Intn(numKeys)]

		switch {
		case op < 4: // 40% store INSERT
			val := randomVal(rng)
			goDB.Store(Datum(key), Datum(val), 0)
			cDB.Store(key, val, 0)

		case op < 7: // 30% store REPLACE
			val := randomVal(rng)
			goDB.Store(Datum(key), Datum(val), StoreREPLACE)
			cDB.Store(key, val, 1) // CSDB_REPLACE = 1

		case op < 9: // 20% fetch (verify)
			goVal, _ := goDB.Fetch(Datum(key))
			cVal := cDB.Fetch(key)
			if !bytes.Equal(goVal, cVal) {
				t.Errorf("op %d: Fetch(%x) Go=%x, C=%x", i, key, goVal, cVal)
			}

		default: // 10% delete
			goDB.Delete(Datum(key))
			cDB.Delete(key)
		}
	}

	goDB.Close()
	cDB.Close()

	for _, ext := range []string{".dir", ".pag"} {
		goHash := diffFileHash(t, goPath+ext)
		cHash := diffFileHash(t, cPath+ext)
		if goHash != cHash {
			t.Errorf("test%s: Go SHA256=%s, C SHA256=%s — files differ", ext, goHash, cHash)
		}
	}
}

func allZero(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}
	return true
}

func randomVal(rng *rand.Rand) []byte {
	vlen := rng.Intn(50) + 1
	v := make([]byte, vlen)
	for j := range v {
		v[j] = byte(rng.Intn(256))
	}
	return v
}

func diffFileHash(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		// File may not exist if no writes occurred (e.g., empty .dir)
		if os.IsNotExist(err) {
			return "empty"
		}
		t.Fatalf("Failed to read %s: %v", path, err)
	}
	h := sha256.Sum256(data)
	return fmt.Sprintf("%x", h)
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
