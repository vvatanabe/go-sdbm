// Command sdbm-walkthrough reproduces the examples.
// Each run creates a fresh temporary directory and leaves the files for inspection.
package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	sdbm "github.com/vvatanabe/go-sdbm"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	dir, err := os.MkdirTemp("", "sdbm-walkthrough-")
	if err != nil {
		return err
	}
	fmt.Printf("Files: %s\n", dir)
	if err := smallExample(filepath.Join(dir, "small")); err != nil {
		return err
	}
	return growthExample(filepath.Join(dir, "growth"))
}

func store(db *sdbm.DBM, key string, value []byte) error {
	ok, err := db.Store(sdbm.Datum(key), sdbm.Datum(value), sdbm.StoreREPLACE)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("Store(%q) returned false", key)
	}
	return nil
}

func remove(db *sdbm.DBM, key string) error {
	ok, err := db.Delete(sdbm.Datum(key))
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("Delete(%q) did not find the key", key)
	}
	return nil
}

func smallExample(path string) (err error) {
	db, err := sdbm.Open(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := db.Close(); err == nil {
			err = closeErr
		}
	}()
	if _, err := snapshot(path, "small: empty"); err != nil {
		return err
	}
	if err := store(db, "a", []byte("apple")); err != nil {
		return err
	}
	first, err := snapshot(path, "small: a -> apple")
	if err != nil {
		return err
	}
	wantHeader := []byte{0x02, 0x00, 0xff, 0x03, 0xfa, 0x03}
	if len(first) != sdbm.PBLKSIZ || !bytes.Equal(first[:6], wantHeader) || string(first[1018:]) != "applea" {
		return fmt.Errorf("unexpected first-page layout")
	}
	fmt.Printf("  header=% x tail=%q\n", first[:6], first[1018:])
	if err := store(db, "b", []byte("banana")); err != nil {
		return err
	}
	if err := store(db, "c", []byte("cherry")); err != nil {
		return err
	}
	if _, err := snapshot(path, "small: a, b, c"); err != nil {
		return err
	}
	value, err := db.Fetch(sdbm.Datum("b"))
	if err != nil {
		return err
	}
	if string(value) != "banana" {
		return fmt.Errorf("Fetch(b) = %q", value)
	}
	saved := bytes.Clone(value)
	if err := remove(db, "b"); err != nil {
		return err
	}
	if _, err := snapshot(path, "small: delete b"); err != nil {
		return err
	}
	fmt.Printf("  after Delete(b): borrowed=%q copied=%q\n", value, saved)
	if string(value) != "cherry" || string(saved) != "banana" {
		return fmt.Errorf("unexpected buffer-sharing result")
	}
	return nil
}

func growthExample(path string) error {
	if err := buildGrowth(path); err != nil {
		return err
	}
	if err := reopenGrowth(path); err != nil {
		return err
	}
	return emptyPage(path)
}

func buildGrowth(path string) (err error) {
	db, err := sdbm.Open(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := db.Close(); err == nil {
			err = closeErr
		}
	}()
	var before []byte
	for _, key := range []string{"a", "b", "c", "d", "e", "g"} {
		if err := store(db, key, bytes.Repeat([]byte(key), 300)); err != nil {
			return err
		}
		data, err := snapshot(path, "growth: insert "+key)
		if err != nil {
			return err
		}
		if key == "e" {
			before = bytes.Clone(data[:sdbm.PBLKSIZ])
		}
		if key == "g" {
			if len(data) != 4096 || !bytes.Equal(before, data[:1024]) || !bytes.Equal(data[2048:3072], make([]byte, 1024)) {
				return fmt.Errorf("unexpected split: page 0 changed, or page 2 is not empty")
			}
			dir, err := os.ReadFile(path + ".dir")
			if err != nil {
				return err
			}
			if len(dir) != 4096 || dir[0] != 0x05 {
				return fmt.Errorf("unexpected directory after split")
			}
			fmt.Println("  page 0 unchanged: true")
		}
	}
	return nil
}

func reopenGrowth(path string) (err error) {
	db, err := sdbm.Open(path, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := db.Close(); err == nil {
			err = closeErr
		}
	}()
	for _, key := range []string{"a", "b", "c", "d", "e", "g"} {
		value, err := db.Fetch(sdbm.Datum(key))
		if err != nil {
			return err
		}
		if !bytes.Equal(value, bytes.Repeat([]byte(key), 300)) {
			return fmt.Errorf("reopened Fetch(%q) returned unexpected data", key)
		}
	}
	fmt.Println("reopen: all six values verified")
	var keys []string
	key, err := db.FirstKey()
	for err == nil && key != nil {
		keys = append(keys, string(key))
		key, err = db.NextKey()
	}
	if err != nil {
		return err
	}
	fmt.Printf("iteration: %v\n", keys)
	if !slices.Equal(keys, []string{"b", "d", "a", "e", "c", "g"}) {
		return fmt.Errorf("unexpected iteration order for this example")
	}
	return nil
}

func emptyPage(path string) (err error) {
	db, err := sdbm.Open(path, os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := db.Close(); err == nil {
			err = closeErr
		}
	}()
	for _, key := range []string{"c", "g"} {
		if err := remove(db, key); err != nil {
			return err
		}
	}
	data, err := snapshot(path, "growth: delete c and g")
	if err != nil {
		return err
	}
	dir, err := os.ReadFile(path + ".dir")
	if err != nil {
		return err
	}
	if len(data) != 4096 || binary.LittleEndian.Uint16(data[3072:]) != 0 || len(dir) != 4096 || dir[0] != 0x05 {
		return fmt.Errorf("unexpected layout after emptying page 3")
	}
	return nil
}

// snapshot decodes the files produced by this example, independently of Page's
// internal buffer. It checks bounds before using offsets from the file.
func snapshot(path, label string) ([]byte, error) {
	data, err := os.ReadFile(path + ".pag")
	if err != nil {
		return nil, err
	}
	dir, err := os.ReadFile(path + ".dir")
	if err != nil {
		return nil, err
	}
	if len(data)%sdbm.PBLKSIZ != 0 {
		return nil, fmt.Errorf("incomplete page in %s.pag", path)
	}
	first := "empty"
	if len(dir) > 0 {
		first = fmt.Sprintf("0x%02x", dir[0])
	}
	fmt.Printf("\n%s: pag=%d dir=%d dir[0]=%s\n", label, len(data), len(dir), first)
	for start := 0; start < len(data); start += sdbm.PBLKSIZ {
		page := data[start : start+sdbm.PBLKSIZ]
		n := int(binary.LittleEndian.Uint16(page))
		headerEnd := (n + 1) * 2
		if n%2 != 0 || headerEnd > len(page) {
			return nil, fmt.Errorf("invalid entry count at page %d", start/sdbm.PBLKSIZ)
		}
		end := len(page)
		var keys []string
		var offsets []int
		for i := 1; i < n; i += 2 {
			keyStart := int(binary.LittleEndian.Uint16(page[i*2:]))
			valueStart := int(binary.LittleEndian.Uint16(page[(i+1)*2:]))
			if valueStart < headerEnd || valueStart > keyStart || keyStart > end {
				return nil, fmt.Errorf("invalid offsets at page %d", start/sdbm.PBLKSIZ)
			}
			keys = append(keys, string(page[keyStart:end]))
			offsets = append(offsets, keyStart, valueStart)
			end = valueStart
		}
		fmt.Printf("  page %d: n=%d keys=%v offsets=%v free=%d\n", start/sdbm.PBLKSIZ, n, keys, offsets, end-headerEnd)
	}
	return data, nil
}
