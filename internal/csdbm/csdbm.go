//go:build cgo

package csdbm

/*
#include <stdlib.h>
#include <string.h>

#define DBLKSIZ 4096
#define PBLKSIZ 1024
#define PAIRMAX 1008
#define SPLTMAX 10

typedef struct {
	char *dptr;
	int dsize;
} datum;

static datum nullitem = {(char *)0, 0};

// Forward declarations
static int seepair(char *pag, int n, char *key, int siz);

// ---- hash.c (ANSI C) ----

long dbm_hash(char *str, int len) {
	unsigned long n = 0;
	while (len--)
		n = *str++ + 65599 * n;
	return (long)n;
}

static long dbm_hash_unsigned(unsigned char *str, int len) {
	unsigned long n = 0;
	while (len--)
		n = *str++ + 65599 * n;
	return (long)n;
}

// ---- pair.c (ANSI C) ----

static int seepair(char *pag, int n, char *key, int siz) {
	int i;
	int off = PBLKSIZ;
	short *ino = (short *) pag;

	for (i = 1; i < n; i += 2) {
		if (siz == off - ino[i] &&
		    memcmp(key, pag + ino[i], siz) == 0)
			return i;
		off = ino[i + 1];
	}
	return 0;
}

int fitpair(char *pag, int need) {
	int n;
	int off;
	int avail;
	short *ino = (short *) pag;

	off = ((n = ino[0]) > 0) ? ino[n] : PBLKSIZ;
	avail = off - (n + 1) * (int)sizeof(short);
	need += 2 * (int)sizeof(short);

	return need <= avail;
}

void putpair(char *pag, datum key, datum val) {
	int n;
	int off;
	short *ino = (short *) pag;

	off = ((n = ino[0]) > 0) ? ino[n] : PBLKSIZ;
	off -= key.dsize;
	(void) memcpy(pag + off, key.dptr, key.dsize);
	ino[n + 1] = off;

	off -= val.dsize;
	(void) memcpy(pag + off, val.dptr, val.dsize);
	ino[n + 2] = off;

	ino[0] += 2;
}

datum getpair(char *pag, datum key) {
	int i;
	int n;
	datum val;
	short *ino = (short *) pag;

	if ((n = ino[0]) == 0)
		return nullitem;

	if ((i = seepair(pag, n, key.dptr, key.dsize)) == 0)
		return nullitem;

	val.dptr = pag + ino[i + 1];
	val.dsize = ino[i] - ino[i + 1];
	return val;
}

int delpair(char *pag, datum key) {
	int n;
	int i;
	short *ino = (short *) pag;

	if ((n = ino[0]) == 0)
		return 0;

	if ((i = seepair(pag, n, key.dptr, key.dsize)) == 0)
		return 0;

	if (i < n - 1) {
		int m;
		char *dst = pag + (i == 1 ? PBLKSIZ : ino[i - 1]);
		char *src = pag + ino[i + 1];
		int   zoo = (int)(dst - src);

		m = ino[i + 1] - ino[n];
		while (m--)
			*--dst = *--src;

		while (i < n - 1) {
			ino[i] = ino[i + 2] + zoo;
			i++;
		}
	}
	ino[0] -= 2;
	return 1;
}

void splpage(char *pag, char *newbuf, long sbit) {
	datum key;
	datum val;

	int n;
	int off = PBLKSIZ;
	char cur[PBLKSIZ];
	short *ino = (short *) cur;

	(void) memcpy(cur, pag, PBLKSIZ);
	(void) memset(pag, 0, PBLKSIZ);
	(void) memset(newbuf, 0, PBLKSIZ);

	n = ino[0];
	for (ino++; n > 0; ino += 2) {
		key.dptr = cur + ino[0];
		key.dsize = off - ino[0];
		val.dptr = cur + ino[1];
		val.dsize = ino[0] - ino[1];

		(void) putpair((dbm_hash(key.dptr, key.dsize) & sbit) ? newbuf : pag, key, val);

		off = ino[1];
		n -= 2;
	}
}

int chkpage(char *pag) {
	int n;
	int off;
	short *ino = (short *) pag;

	if ((n = ino[0]) < 0 || n > PBLKSIZ / (int)sizeof(short))
		return 0;

	if (n > 0) {
		off = PBLKSIZ;
		for (ino++; n > 0; ino += 2) {
			if (ino[0] > off || ino[1] > off ||
			    ino[1] > ino[0])
				return 0;
			off = ino[1];
			n -= 2;
		}
	}
	return 1;
}
*/
import "C"

import "unsafe"

const PBLKSIZ = 1024

// Hash calls C dbm_hash with the default (signed) char behavior.
func Hash(data []byte) int64 {
	if len(data) == 0 {
		return int64(C.dbm_hash((*C.char)(unsafe.Pointer(nil)), C.int(0)))
	}
	return int64(C.dbm_hash((*C.char)(unsafe.Pointer(&data[0])), C.int(len(data))))
}

// HashUnsigned calls the unsigned char variant of dbm_hash.
// This should match Go's Hash() for all inputs since Go's byte is uint8.
func HashUnsigned(data []byte) int64 {
	if len(data) == 0 {
		return int64(C.dbm_hash_unsigned((*C.uchar)(unsafe.Pointer(nil)), C.int(0)))
	}
	return int64(C.dbm_hash_unsigned((*C.uchar)(unsafe.Pointer(&data[0])), C.int(len(data))))
}

// PutPair calls C putpair on the given page buffer.
// pag must be exactly PBLKSIZ bytes.
func PutPair(pag, key, val []byte) {
	ckey := C.datum{}
	if len(key) > 0 {
		ckey.dptr = (*C.char)(unsafe.Pointer(&key[0]))
	}
	ckey.dsize = C.int(len(key))

	cval := C.datum{}
	if len(val) > 0 {
		cval.dptr = (*C.char)(unsafe.Pointer(&val[0]))
	}
	cval.dsize = C.int(len(val))

	C.putpair((*C.char)(unsafe.Pointer(&pag[0])), ckey, cval)
}

// GetPair calls C getpair on the given page buffer.
// Returns nil if the key is not found.
func GetPair(pag []byte, key []byte) []byte {
	ckey := C.datum{}
	if len(key) > 0 {
		ckey.dptr = (*C.char)(unsafe.Pointer(&key[0]))
	}
	ckey.dsize = C.int(len(key))

	result := C.getpair((*C.char)(unsafe.Pointer(&pag[0])), ckey)
	if result.dptr == nil {
		return nil
	}
	return C.GoBytes(unsafe.Pointer(result.dptr), result.dsize)
}

// DelPair calls C delpair on the given page buffer.
func DelPair(pag []byte, key []byte) bool {
	ckey := C.datum{}
	if len(key) > 0 {
		ckey.dptr = (*C.char)(unsafe.Pointer(&key[0]))
	}
	ckey.dsize = C.int(len(key))

	return C.delpair((*C.char)(unsafe.Pointer(&pag[0])), ckey) != 0
}

// SplPage calls C splpage on the given page buffers.
// Both pag and newPag must be exactly PBLKSIZ bytes.
func SplPage(pag, newPag []byte, sbit int64) {
	C.splpage(
		(*C.char)(unsafe.Pointer(&pag[0])),
		(*C.char)(unsafe.Pointer(&newPag[0])),
		C.long(sbit),
	)
}

// FitPair calls C fitpair on the given page buffer.
func FitPair(pag []byte, need int) bool {
	return C.fitpair((*C.char)(unsafe.Pointer(&pag[0])), C.int(need)) != 0
}

// ChkPage calls C chkpage on the given page buffer.
func ChkPage(pag []byte) bool {
	return C.chkpage((*C.char)(unsafe.Pointer(&pag[0]))) != 0
}
