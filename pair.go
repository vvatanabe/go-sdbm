package sdbm

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

/*
 * page format:
 *      +------------------------------+
 * ino  | n | keyOff | datOff | keyOff |
 *      +------------+--------+--------+
 *      | datOff | - - - ---->         |
 *      +--------+---------------------+
 *      |        F R E E A R E A       |
 *      +--------------+---------------+
 *      |  <---- - - - | data          |
 *      +--------+-----+----+----------+
 *      |  key   | data     | key      |
 *      +--------+----------+----------+
 *
 * calculating the offsets for free area:  if the number
 * of entries (ino[0]) is zero, the offset to the END of
 * the free area is the block size. Otherwise, it is the
 * nth (ino[ino[0]]) entry's offset.
 */

// Page represents a database page for SDBM, handling the storage of keys and values.
// Each page contains metadata about key and value offsets and a free area for storing data.
// The free area begins at the highest offset in the page. The key/value pairs
// are stored in reverse order with their offsets stored at the beginning of the page.
type Page struct {
	buf [PBLKSIZ]byte
}

// FitPair checks if there is enough space in the page to store a new key-value pair.
// It calculates the free area and compares it to the required space for the pair.
func (p *Page) FitPair(need int) bool {
	n := int(p.getN())
	off := PBLKSIZ
	if n > 0 {
		off = int(p.getIno(n))
	}
	free := off - (n+1)*SHORTSIZE
	need += 2 * SHORTSIZE

	if debug {
		fmt.Printf("free %d need %d\n", free, need)
	}

	return need <= free
}

// PutPair stores a key-value pair in the page. It updates the offset table
// and copies the key and value into the free area in reverse order.
func (p *Page) PutPair(key Datum, val Datum) {
	n := int(p.getN())
	off := PBLKSIZ
	if n > 0 {
		off = int(p.getIno(n))
	}

	// enter the key first
	off -= key.Size()
	copy(p.buf[off:], key)
	p.setIno(n+1, uint16(off))

	// now the data
	off -= val.Size()
	copy(p.buf[off:], val)
	p.setIno(n+2, uint16(off))

	// adjust item count
	p.setN(uint16(n + 2))
}

// GetPair retrieves the value corresponding to a given key from the page.
// If the key is found, it returns the associated value. If not, it returns Nullitem.
func (p *Page) GetPair(key Datum) Datum {
	n := int(p.getN())
	if n == 0 {
		return Nullitem
	}

	i := p.seePair(n, key)
	if i == 0 {
		return Nullitem
	}

	start := p.getIno(i + 1)
	end := p.getIno(i)

	val := Datum(p.buf[start:end])

	return val
}

// DupPair checks if a duplicate of the given key exists in the page.
func (p *Page) DupPair(key Datum) bool {
	n := int(p.getN())
	if n == 0 {
		return false
	}
	return p.seePair(n, key) > 0
}

// GetNKey retrieves the nth key from the page, using the index to locate its position.
func (p *Page) GetNKey(num int) Datum {
	var key Datum
	num = num*2 - 1

	n := int(p.getN())
	if n == 0 || num > n {
		return Nullitem
	}

	off := PBLKSIZ
	if num > 1 {
		off = int(p.getIno(num - 1))
	}

	start := int(p.getIno(num))
	key = p.buf[start:off]

	return key
}

// DelPair deletes the key-value pair from the page.
func (p *Page) DelPair(key Datum) bool {
	n := int(p.getN())
	if n == 0 {
		return false
	}

	i := p.seePair(n, key)
	if i == 0 {
		return false
	}

	// found the key. if it is the last entry
	// [i.e. i == n - 1] we just adjust the entry count.
	// hard case: move all data down onto the deleted pair,
	// shift offsets onto deleted offsets, and adjust them.
	// [note: 0 < i < n]
	if i < n-1 {
		var dst int
		if i == 1 {
			dst = PBLKSIZ
		} else {
			dst = int(p.getIno(i - 1))
		}
		src := int(p.getIno(i + 1))
		zoo := dst - src

		if debug {
			fmt.Printf("free-up %d\n", zoo)
		}

		// shift data/keys down
		m := int(p.getIno(i+1) - p.getIno(n))
		copy(p.buf[dst-m:], p.buf[src-m:])

		// Adjust offset index up
		for i < n-1 {
			p.setIno(i, p.getIno(i+2)+uint16(zoo))
			i++
		}
	}
	p.setN(p.getN() - 2)
	return true
}

// search for the key in the page.
// return offset index in the range 0 < i < n.
// return 0 if not found.
func (p *Page) seePair(n int, key []byte) int {
	off := PBLKSIZ
	for i := 1; i < n; i += 2 {
		cur := p.getIno(i)
		if len(key) == off-int(cur) && bytes.Equal(key, p.buf[cur:cur+uint16(len(key))]) {
			return i
		}
		off = int(p.getIno(i + 1))
	}
	return 0
}

// SplPage splits the current page into two, distributing the key-value pairs
// between the original page and the new page based on the provided hash bit (sbit).
func (p *Page) SplPage(newPag *Page, sbit int64) {
	var (
		key, val Datum
		cur      Page
	)
	off := PBLKSIZ

	copy(cur.buf[:], p.buf[:])
	copy(p.buf[:], make([]byte, PBLKSIZ))
	copy(newPag.buf[:], make([]byte, PBLKSIZ))

	n := cur.getIno(0)
	for i := 1; n > 0; i += 2 {
		keyOff := int(cur.getIno(i))
		valOff := int(cur.getIno(i + 1))

		key = cur.buf[keyOff:off]
		val = cur.buf[valOff:keyOff]

		// select the page pointer (by looking at sbit) and insert
		if exHash(key)&sbit != 0 {
			newPag.PutPair(key, val)
		} else {
			p.PutPair(key, val)
		}

		off = valOff
		n -= 2
	}

	if debug {
		fmt.Printf("%d split %d/%d\n", cur.getIno(0)/2, len(newPag.buf)/2, len(p.buf)/2)
	}
}

// ChkPage checks the integrity of the page by verifying that the number of entries
// and the order of offsets are valid. Returns false if the page is invalid.
func (p *Page) ChkPage() bool {
	n := int(p.getN())
	if n < 0 || n > PBLKSIZ/SHORTSIZE {
		return false
	}
	if n > 0 {
		off := PBLKSIZ
		for i := 1; n > 0; i += 2 {
			keyOff := int(p.getIno(i))
			valOff := int(p.getIno(i + 1))
			if keyOff > off || valOff > off || valOff > keyOff {
				return false
			}
			off = valOff
			n -= 2
		}
	}
	return true
}

func (p *Page) getN() uint16 {
	return p.getIno(0)
}

func (p *Page) setN(val uint16) {
	p.setIno(0, val)
}

func (p *Page) getIno(i int) uint16 {
	return binary.LittleEndian.Uint16(p.buf[i*2 : i*2+2])
}

func (p *Page) setIno(i int, val uint16) {
	binary.LittleEndian.PutUint16(p.buf[i*2:], val)
}
