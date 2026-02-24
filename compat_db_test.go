package sdbm_test

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	sdbm "github.com/vvatanabe/go-sdbm"
)

// ============================================================================
// Layer 3: Golden File Compatibility Tests
// ============================================================================

func TestDB_GoldenFile_AsciiSmall_FetchAll(t *testing.T) {
	dir := t.TempDir()
	copyGoldenFiles(t, "ascii_small", dir)

	db, err := sdbm.Open(filepath.Join(dir, "ascii_small"), os.O_RDONLY, 0644)
	if err != nil {
		t.Fatalf("Open golden file: %v", err)
	}
	defer db.Close()

	for i := 1; i <= 100; i++ {
		key := sdbm.Datum(fmt.Sprintf("key%03d", i))
		expectedVal := sdbm.Datum(fmt.Sprintf("val%03d", i))

		val, err := db.Fetch(key)
		if err != nil {
			t.Fatalf("Fetch(%s): %v", key, err)
		}
		if string(val) != string(expectedVal) {
			t.Errorf("Fetch(%s) = %q, want %q", key, val, expectedVal)
		}
	}
}

func TestDB_GoldenFile_AsciiSmall_Iteration(t *testing.T) {
	dir := t.TempDir()
	copyGoldenFiles(t, "ascii_small", dir)

	db, err := sdbm.Open(filepath.Join(dir, "ascii_small"), os.O_RDONLY, 0644)
	if err != nil {
		t.Fatalf("Open golden file: %v", err)
	}
	defer db.Close()

	count := 0
	key, err := db.FirstKey()
	if err != nil {
		t.Fatalf("FirstKey: %v", err)
	}
	for key != nil {
		count++
		key, err = db.NextKey()
		if err != nil {
			t.Fatalf("NextKey: %v", err)
		}
	}

	if count != 100 {
		t.Errorf("Iteration count = %d, want 100", count)
	}
}

func TestDB_GoldenFile_ManySplits_FetchAll(t *testing.T) {
	dir := t.TempDir()
	copyGoldenFiles(t, "many_splits", dir)

	db, err := sdbm.Open(filepath.Join(dir, "many_splits"), os.O_RDONLY, 0644)
	if err != nil {
		t.Fatalf("Open golden file: %v", err)
	}
	defer db.Close()

	for i := 1; i <= 10000; i++ {
		key := sdbm.Datum(fmt.Sprintf("key%05d", i))
		expectedVal := sdbm.Datum(fmt.Sprintf("val%05d", i))

		val, err := db.Fetch(key)
		if err != nil {
			t.Fatalf("Fetch(%s): %v", key, err)
		}
		if string(val) != string(expectedVal) {
			t.Errorf("Fetch(%s) = %q, want %q", key, val, expectedVal)
		}
	}
}

func TestDB_GoldenFile_ManySplits_Iteration(t *testing.T) {
	dir := t.TempDir()
	copyGoldenFiles(t, "many_splits", dir)

	db, err := sdbm.Open(filepath.Join(dir, "many_splits"), os.O_RDONLY, 0644)
	if err != nil {
		t.Fatalf("Open golden file: %v", err)
	}
	defer db.Close()

	count := 0
	key, err := db.FirstKey()
	if err != nil {
		t.Fatalf("FirstKey: %v", err)
	}
	for key != nil {
		count++
		key, err = db.NextKey()
		if err != nil {
			t.Fatalf("NextKey: %v", err)
		}
	}

	if count != 10000 {
		t.Errorf("Iteration count = %d, want 10000", count)
	}
}

// ============================================================================
// BUG-1: getNext blkptr increment missing
// ============================================================================

func TestDB_BUG1_BasicIteration(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bug1_basic")

	db, err := sdbm.Open(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Insert enough entries to span multiple pages
	const numEntries = 1000
	for i := 1; i <= numEntries; i++ {
		key := sdbm.Datum(fmt.Sprintf("key%04d", i))
		val := sdbm.Datum(fmt.Sprintf("val%04d", i))
		ok, err := db.Store(key, val, 0)
		if err != nil {
			t.Fatalf("Store(%s): %v", key, err)
		}
		if !ok {
			t.Fatalf("Store(%s) returned false", key)
		}
	}

	// Basic iteration without interleaving — all keys must be found
	seen := make(map[string]bool)
	key, err := db.FirstKey()
	if err != nil {
		t.Fatalf("FirstKey: %v", err)
	}

	for key != nil {
		seen[string(key)] = true
		key, err = db.NextKey()
		if err != nil {
			t.Fatalf("NextKey: %v", err)
		}
	}

	if len(seen) != numEntries {
		t.Errorf("Iterated %d unique keys, want %d", len(seen), numEntries)
	}
}

func TestDB_BUG1_IterationWithInterleavedFetch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bug1")

	db, err := sdbm.Open(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Insert enough entries to span multiple pages
	const numEntries = 1000
	for i := 1; i <= numEntries; i++ {
		key := sdbm.Datum(fmt.Sprintf("key%04d", i))
		val := sdbm.Datum(fmt.Sprintf("val%04d", i))
		ok, err := db.Store(key, val, 0)
		if err != nil {
			t.Fatalf("Store(%s): %v", key, err)
		}
		if !ok {
			t.Fatalf("Store(%s) returned false", key)
		}
	}

	// Iterate and interleave Fetch calls.
	// Note: Interleaving Fetch during iteration is a known limitation of SDBM's design.
	// Fetch overwrites the page buffer, so some keys from the Fetch page may be returned
	// as duplicates, and some keys from the original page may be lost.
	// The key invariant tested here is that iteration TERMINATES (blkptr advances correctly)
	// rather than looping infinitely (which was the BUG-1 symptom).
	seen := make(map[string]bool)
	key, err := db.FirstKey()
	if err != nil {
		t.Fatalf("FirstKey: %v", err)
	}

	// Safety limit to prevent infinite loop (BUG-1 causes blkptr to not advance)
	const maxIterations = numEntries * 20
	iterations := 0

	for key != nil {
		iterations++
		if iterations > maxIterations {
			t.Fatalf("Iteration exceeded safety limit (%d) — likely stuck due to blkptr not advancing (BUG-1)", maxIterations)
		}

		seen[string(key)] = true

		// Interleave a Fetch call — this changes pagbno and should not cause infinite loop
		fetchKey := sdbm.Datum("key0500")
		_, err := db.Fetch(fetchKey)
		if err != nil {
			t.Fatalf("Interleaved Fetch: %v", err)
		}

		key, err = db.NextKey()
		if err != nil {
			t.Fatalf("NextKey: %v", err)
		}
	}

	t.Logf("Iteration completed: %d iterations, %d unique keys seen (of %d total)", iterations, len(seen), numEntries)

	// With interleaved Fetch on every NextKey call, most iteration time is spent
	// walking through the Fetch page's keys. We see keys from the Fetch page plus
	// one key from each sequential page before Fetch reloads. This is inherent
	// to SDBM's single-page-buffer design. The important invariant is that
	// iteration terminates and we see keys from multiple different pages.
	if len(seen) < 10 {
		t.Errorf("Too few unique keys seen: %d (expected at least 10)", len(seen))
	}
}

// ============================================================================
// BUG-2: makeRoom loop count off by one
// ============================================================================

func TestDB_BUG2_ManyCollisions(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bug2")

	db, err := sdbm.Open(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	const numEntries = 100000
	for i := 1; i <= numEntries; i++ {
		key := sdbm.Datum("key" + strconv.Itoa(i))
		val := sdbm.Datum("val" + strconv.Itoa(i))
		ok, err := db.Store(key, val, 0)
		if err != nil {
			t.Fatalf("Store(%s): %v", key, err)
		}
		if !ok {
			t.Fatalf("Store(%s) returned false", key)
		}
	}

	// Verify all entries can be fetched
	for i := 1; i <= numEntries; i++ {
		key := sdbm.Datum("key" + strconv.Itoa(i))
		val, err := db.Fetch(key)
		if err != nil {
			t.Fatalf("Fetch(%s): %v", key, err)
		}
		if val == nil || len(val) == 0 {
			t.Fatalf("Fetch(%s) returned nil/empty", key)
		}
	}
}

// ============================================================================
// BUG-3: bad() allows zero-length key
// ============================================================================

func TestDB_BUG3_ZeroLengthKey(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bug3")

	db, err := sdbm.Open(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	emptyKey := sdbm.Datum([]byte{})

	// Fetch with zero-length key should return error
	_, err = db.Fetch(emptyKey)
	if !errors.Is(err, sdbm.ErrInvalidArgument) {
		t.Errorf("Fetch(empty key) error = %v, want ErrInvalidArgument", err)
	}

	// Store with zero-length key should return error
	_, err = db.Store(emptyKey, sdbm.Datum("val"), 0)
	if !errors.Is(err, sdbm.ErrInvalidArgument) {
		t.Errorf("Store(empty key) error = %v, want ErrInvalidArgument", err)
	}

	// Delete with zero-length key should return error
	_, err = db.Delete(emptyKey)
	if !errors.Is(err, sdbm.ErrInvalidArgument) {
		t.Errorf("Delete(empty key) error = %v, want ErrInvalidArgument", err)
	}
}

// ============================================================================
// BUG-4: Store duplicate detection logic differs
// ============================================================================

func TestDB_BUG4_DuplicateDetectionDefaultFlags(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bug4")

	db, err := sdbm.Open(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	key := sdbm.Datum("testkey")
	val1 := sdbm.Datum("value1")
	val2 := sdbm.Datum("value2")

	// Store key with val1 using flags=0
	ok, err := db.Store(key, val1, 0)
	if err != nil {
		t.Fatalf("Store(val1): %v", err)
	}
	if !ok {
		t.Fatal("Store(val1) returned false")
	}

	// Store same key with val2 using flags=0
	// With C-compatible behavior, this should detect the duplicate and return success
	// without creating a second entry
	ok, err = db.Store(key, val2, 0)
	if err != nil {
		t.Fatalf("Store(val2): %v", err)
	}
	if !ok {
		t.Fatal("Store(val2) returned false")
	}

	// Fetch should return val1 (the first value, since duplicate was detected)
	got, err := db.Fetch(key)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if string(got) != string(val1) {
		t.Errorf("Fetch after duplicate Store = %q, want %q (first value)", got, val1)
	}

	// Delete and verify no ghost entries remain
	deleted, err := db.Delete(key)
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if !deleted {
		t.Error("Delete returned false")
	}

	// After delete, Fetch should return nil (no ghost entry from duplicate)
	got, err = db.Fetch(key)
	if err != nil {
		t.Fatalf("Fetch after Delete: %v", err)
	}
	if got != nil {
		t.Errorf("Fetch after Delete = %q, want nil (ghost entry detected)", got)
	}
}

// ============================================================================
// BUG-5: makeRoom failure leads to PutPair on overfull page
// ============================================================================

func TestDB_BUG5_NoPanicOnOverfull(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bug5")

	db, err := sdbm.Open(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Insert many large key/val pairs to trigger SPLTMAX exhaustion
	panicDetected := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				panicDetected = true
				t.Errorf("PANIC detected during large insertions: %v", r)
			}
		}()

		for i := 0; i < 10000; i++ {
			// Use large keys/values that fill pages quickly
			key := sdbm.Datum(fmt.Sprintf("largekey_%05d_%s", i, strings.Repeat("x", 100)))
			val := sdbm.Datum(fmt.Sprintf("largeval_%05d_%s", i, strings.Repeat("y", 100)))

			ok, err := db.Store(key, val, 0)
			if err != nil {
				// Error is acceptable (SPLTMAX exhaustion should return error, not panic)
				break
			}
			if !ok {
				break
			}
		}
	}()

	if panicDetected {
		t.Error("Store should return error instead of panicking when page is overfull")
	}
}

// ============================================================================
// BUG-7: getNext EOF partial read
// ============================================================================

func TestDB_BUG7_TruncatedPagFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bug7")

	// Create a DB with some entries
	db, err := sdbm.Open(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	for i := 1; i <= 100; i++ {
		key := sdbm.Datum(fmt.Sprintf("key%03d", i))
		val := sdbm.Datum(fmt.Sprintf("val%03d", i))
		if _, err := db.Store(key, val, 0); err != nil {
			t.Fatalf("Store: %v", err)
		}
	}
	db.Close()

	// Truncate the .pag file at a non-page boundary
	pagPath := path + ".pag"
	info, err := os.Stat(pagPath)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}

	// Truncate to a size that's not a multiple of PBLKSIZ (1024)
	truncSize := info.Size() - 512 // cut off half a page
	if truncSize < 1024 {
		truncSize = 512 // at least have some data
	}
	if err := os.Truncate(pagPath, truncSize); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	// Reopen and iterate — should not return garbage data
	db2, err := sdbm.Open(path, os.O_RDONLY, 0644)
	if err != nil {
		t.Fatalf("Open truncated: %v", err)
	}
	defer db2.Close()

	key, err := db2.FirstKey()
	if err != nil {
		// Error is acceptable for truncated file
		return
	}

	count := 0
	for key != nil {
		count++
		if count > 100 {
			t.Fatal("Iteration returned more keys than were stored — possible garbage data from truncated page")
		}
		key, err = db2.NextKey()
		if err != nil {
			// Error is acceptable for truncated file
			break
		}
	}

	t.Logf("Iterated %d keys from truncated DB (original had 100 entries)", count)
}

// ============================================================================
// BUG-8: O_RDONLY detection with extra flags
// ============================================================================

func TestDB_BUG8_ReadOnlyWithExtraFlags(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bug8")

	// Create a DB first
	db, err := sdbm.Open(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("Open for write: %v", err)
	}
	if _, err := db.Store(sdbm.Datum("key1"), sdbm.Datum("val1"), 0); err != nil {
		t.Fatalf("Store: %v", err)
	}
	db.Close()

	// Reopen with O_RDONLY | O_SYNC — should be detected as read-only
	db2, err := sdbm.Open(path, os.O_RDONLY|os.O_SYNC, 0644)
	if err != nil {
		t.Fatalf("Open RDONLY|SYNC: %v", err)
	}
	defer db2.Close()

	// Fetch should work
	val, err := db2.Fetch(sdbm.Datum("key1"))
	if err != nil {
		t.Errorf("Fetch on RDONLY|SYNC: %v", err)
	}
	if string(val) != "val1" {
		t.Errorf("Fetch = %q, want %q", val, "val1")
	}

	// Store should return ErrDBMRDOnly
	_, err = db2.Store(sdbm.Datum("key2"), sdbm.Datum("val2"), 0)
	if !errors.Is(err, sdbm.ErrDBMRDOnly) {
		t.Errorf("Store on RDONLY|SYNC error = %v, want ErrDBMRDOnly", err)
	}

	// Delete should return ErrDBMRDOnly
	_, err = db2.Delete(sdbm.Datum("key1"))
	if !errors.Is(err, sdbm.ErrDBMRDOnly) {
		t.Errorf("Delete on RDONLY|SYNC error = %v, want ErrDBMRDOnly", err)
	}
}

// ============================================================================
// Layer 4: File Hash Comparison Tests (Write-Path End-to-End Verification)
// ============================================================================

func TestDB_FileHash_AsciiSmall(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ascii_small")

	db, err := sdbm.Open(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	for i := 1; i <= 100; i++ {
		key := sdbm.Datum(fmt.Sprintf("key%03d", i))
		val := sdbm.Datum(fmt.Sprintf("val%03d", i))
		ok, err := db.Store(key, val, 0)
		if err != nil {
			t.Fatalf("Store(%s): %v", key, err)
		}
		if !ok {
			t.Fatalf("Store(%s) returned false", key)
		}
	}
	db.Close()

	goldenDir := filepath.Join("testdata", "golden")

	for _, ext := range []string{".dir", ".pag"} {
		goHash := fileHash(t, path+ext)
		cHash := fileHash(t, filepath.Join(goldenDir, "ascii_small"+ext))

		if goHash != cHash {
			t.Errorf("ascii_small%s: Go SHA256=%s, C SHA256=%s — files differ", ext, goHash, cHash)
		}
	}
}

func TestDB_FileHash_ManySplits(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "many_splits")

	db, err := sdbm.Open(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	for i := 1; i <= 10000; i++ {
		key := sdbm.Datum(fmt.Sprintf("key%05d", i))
		val := sdbm.Datum(fmt.Sprintf("val%05d", i))
		ok, err := db.Store(key, val, 0)
		if err != nil {
			t.Fatalf("Store(%s): %v", key, err)
		}
		if !ok {
			t.Fatalf("Store(%s) returned false", key)
		}
	}
	db.Close()

	goldenDir := filepath.Join("testdata", "golden")

	for _, ext := range []string{".dir", ".pag"} {
		goHash := fileHash(t, path+ext)
		cHash := fileHash(t, filepath.Join(goldenDir, "many_splits"+ext))

		if goHash != cHash {
			t.Errorf("many_splits%s: Go SHA256=%s, C SHA256=%s — files differ", ext, goHash, cHash)
		}
	}
}

// ============================================================================
// Helpers
// ============================================================================

func fileHash(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("Failed to read %s: %v", path, err)
	}
	h := sha256.Sum256(data)
	return fmt.Sprintf("%x", h)
}

func copyGoldenFiles(t *testing.T, name, destDir string) {
	t.Helper()
	goldenDir := filepath.Join("testdata", "golden")

	for _, ext := range []string{".dir", ".pag"} {
		src := filepath.Join(goldenDir, name+ext)
		dst := filepath.Join(destDir, name+ext)

		data, err := os.ReadFile(src)
		if err != nil {
			t.Fatalf("Failed to read golden file %s: %v", src, err)
		}
		if err := os.WriteFile(dst, data, 0644); err != nil {
			t.Fatalf("Failed to write golden file %s: %v", dst, err)
		}
	}
}
