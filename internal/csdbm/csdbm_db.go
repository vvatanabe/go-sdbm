//go:build cgo

package csdbm

/*
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

// ============================================================================
// Self-contained C SDBM implementation for DB-level differential testing.
// All functions are prefixed with csdb_ and marked static (where possible)
// to avoid linker conflicts with the page-level functions in csdbm.go.
// ============================================================================

#define CSDB_DBLKSIZ 4096
#define CSDB_PBLKSIZ 1024
#define CSDB_PAIRMAX 1008
#define CSDB_SPLTMAX 10
#define CSDB_BYTESIZ 8

#define CSDB_RDONLY  0x1
#define CSDB_IOERR   0x2
#define CSDB_INSERT   0
#define CSDB_REPLACE  1

typedef struct {
	char *dptr;
	int dsize;
} csdb_datum;

static csdb_datum csdb_nullitem = {(char *)0, 0};

#define csdb_bad(x)      ((x).dptr == NULL || (x).dsize <= 0)
#define csdb_exhash(item) csdb_hash((item).dptr, (item).dsize)
#define csdb_ioerr(db)   ((db)->flags |= CSDB_IOERR)

#define CSDB_OFF_PAG(off) (long)(off) * CSDB_PBLKSIZ
#define CSDB_OFF_DIR(off) (long)(off) * CSDB_DBLKSIZ

typedef struct {
	int dirf;
	int pagf;
	int flags;
	long maxbno;
	long curbit;
	long hmask;
	long blkptr;
	int keyptr;
	long blkno;
	long pagbno;
	char pagbuf[CSDB_PBLKSIZ];
	long dirbno;
	char dirbuf[CSDB_DBLKSIZ];
} CSDB;

static long csdb_masks[] = {
	000000000000, 000000000001, 000000000003, 000000000007,
	000000000017, 000000000037, 000000000077, 000000000177,
	000000000377, 000000000777, 000000001777, 000000003777,
	000000007777, 000000017777, 000000037777, 000000077777,
	000000177777, 000000377777, 000000777777, 000001777777,
	000003777777, 000007777777, 000017777777, 000037777777,
	000077777777, 000177777777, 000377777777, 000777777777,
	001777777777, 003777777777, 007777777777, 017777777777
};

// ---- hash ----

static long csdb_hash(char *str, int len) {
	unsigned long n = 0;
	while (len--)
		n = *str++ + 65599 * n;
	return (long)n;
}

// ---- pair operations ----

static int csdb_seepair(char *pag, int n, char *key, int siz) {
	int i;
	int off = CSDB_PBLKSIZ;
	short *ino = (short *) pag;
	for (i = 1; i < n; i += 2) {
		if (siz == off - ino[i] &&
		    memcmp(key, pag + ino[i], siz) == 0)
			return i;
		off = ino[i + 1];
	}
	return 0;
}

static int csdb_fitpair(char *pag, int need) {
	int n, off, avail;
	short *ino = (short *) pag;
	off = ((n = ino[0]) > 0) ? ino[n] : CSDB_PBLKSIZ;
	avail = off - (n + 1) * (int)sizeof(short);
	need += 2 * (int)sizeof(short);
	return need <= avail;
}

static void csdb_putpair(char *pag, csdb_datum key, csdb_datum val) {
	int n, off;
	short *ino = (short *) pag;
	off = ((n = ino[0]) > 0) ? ino[n] : CSDB_PBLKSIZ;
	off -= key.dsize;
	memcpy(pag + off, key.dptr, key.dsize);
	ino[n + 1] = off;
	off -= val.dsize;
	memcpy(pag + off, val.dptr, val.dsize);
	ino[n + 2] = off;
	ino[0] += 2;
}

static csdb_datum csdb_getpair(char *pag, csdb_datum key) {
	int i, n;
	csdb_datum val;
	short *ino = (short *) pag;
	if ((n = ino[0]) == 0)
		return csdb_nullitem;
	if ((i = csdb_seepair(pag, n, key.dptr, key.dsize)) == 0)
		return csdb_nullitem;
	val.dptr = pag + ino[i + 1];
	val.dsize = ino[i] - ino[i + 1];
	return val;
}

static int csdb_delpair(char *pag, csdb_datum key) {
	int n, i;
	short *ino = (short *) pag;
	if ((n = ino[0]) == 0)
		return 0;
	if ((i = csdb_seepair(pag, n, key.dptr, key.dsize)) == 0)
		return 0;
	if (i < n - 1) {
		int m;
		char *dst = pag + (i == 1 ? CSDB_PBLKSIZ : ino[i - 1]);
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

static int csdb_duppair(char *pag, csdb_datum key) {
	short *ino = (short *) pag;
	return ino[0] > 0 && csdb_seepair(pag, ino[0], key.dptr, key.dsize) > 0;
}

static int csdb_chkpage(char *pag) {
	int n, off;
	short *ino = (short *) pag;
	if ((n = ino[0]) < 0 || n > CSDB_PBLKSIZ / (int)sizeof(short))
		return 0;
	if (n > 0) {
		off = CSDB_PBLKSIZ;
		for (ino++; n > 0; ino += 2) {
			if (ino[0] > off || ino[1] > off || ino[1] > ino[0])
				return 0;
			off = ino[1];
			n -= 2;
		}
	}
	return 1;
}

static void csdb_splpage(char *pag, char *newbuf, long sbit) {
	csdb_datum key, val;
	int n;
	int off = CSDB_PBLKSIZ;
	char cur[CSDB_PBLKSIZ];
	short *ino = (short *) cur;
	memcpy(cur, pag, CSDB_PBLKSIZ);
	memset(pag, 0, CSDB_PBLKSIZ);
	memset(newbuf, 0, CSDB_PBLKSIZ);
	n = ino[0];
	for (ino++; n > 0; ino += 2) {
		key.dptr = cur + ino[0];
		key.dsize = off - ino[0];
		val.dptr = cur + ino[1];
		val.dsize = ino[0] - ino[1];
		csdb_putpair((csdb_hash(key.dptr, key.dsize) & sbit) ? newbuf : pag, key, val);
		off = ino[1];
		n -= 2;
	}
}

// ---- DB core routines ----

static int csdb_getdbit(CSDB *db, long dbit) {
	long c = dbit / CSDB_BYTESIZ;
	long dirb = c / CSDB_DBLKSIZ;
	if (dirb != db->dirbno) {
		if (lseek(db->dirf, CSDB_OFF_DIR(dirb), SEEK_SET) < 0
		    || read(db->dirf, db->dirbuf, CSDB_DBLKSIZ) < 0)
			return 0;
		db->dirbno = dirb;
	}
	return db->dirbuf[c % CSDB_DBLKSIZ] & (1 << dbit % CSDB_BYTESIZ);
}

static int csdb_setdbit(CSDB *db, long dbit) {
	long c = dbit / CSDB_BYTESIZ;
	long dirb = c / CSDB_DBLKSIZ;
	if (dirb != db->dirbno) {
		if (lseek(db->dirf, CSDB_OFF_DIR(dirb), SEEK_SET) < 0
		    || read(db->dirf, db->dirbuf, CSDB_DBLKSIZ) < 0)
			return 0;
		db->dirbno = dirb;
	}
	db->dirbuf[c % CSDB_DBLKSIZ] |= (1 << dbit % CSDB_BYTESIZ);
	if (dbit >= db->maxbno)
		db->maxbno += CSDB_DBLKSIZ * CSDB_BYTESIZ;
	if (lseek(db->dirf, CSDB_OFF_DIR(dirb), SEEK_SET) < 0
	    || write(db->dirf, db->dirbuf, CSDB_DBLKSIZ) < 0)
		return 0;
	return 1;
}

static int csdb_getpage(CSDB *db, long hash) {
	int hbit = 0;
	long dbit = 0;
	long pagb;
	while (dbit < db->maxbno && csdb_getdbit(db, dbit))
		dbit = 2 * dbit + ((hash & (1 << hbit++)) ? 2 : 1);
	db->curbit = dbit;
	db->hmask = csdb_masks[hbit];
	pagb = hash & db->hmask;
	if (pagb != db->pagbno) {
		if (lseek(db->pagf, CSDB_OFF_PAG(pagb), SEEK_SET) < 0
		    || read(db->pagf, db->pagbuf, CSDB_PBLKSIZ) < 0)
			return 0;
		if (!csdb_chkpage(db->pagbuf))
			return 0;
		db->pagbno = pagb;
	}
	return 1;
}

static int csdb_makroom(CSDB *db, long hash, int need) {
	long newp;
	char twin[CSDB_PBLKSIZ];
	char *pag = db->pagbuf;
	char *nb = twin;
	int smax = CSDB_SPLTMAX;
	do {
		csdb_splpage(pag, nb, db->hmask + 1);
		newp = (hash & db->hmask) | (db->hmask + 1);
		if (hash & (db->hmask + 1)) {
			if (lseek(db->pagf, CSDB_OFF_PAG(db->pagbno), SEEK_SET) < 0
			    || write(db->pagf, db->pagbuf, CSDB_PBLKSIZ) < 0)
				return 0;
			db->pagbno = newp;
			memcpy(pag, nb, CSDB_PBLKSIZ);
		}
		else if (lseek(db->pagf, CSDB_OFF_PAG(newp), SEEK_SET) < 0
		         || write(db->pagf, nb, CSDB_PBLKSIZ) < 0)
			return 0;
		if (!csdb_setdbit(db, db->curbit))
			return 0;
		if (csdb_fitpair(pag, need))
			return 1;
		db->curbit = 2 * db->curbit +
			((hash & (db->hmask + 1)) ? 2 : 1);
		db->hmask |= db->hmask + 1;
		if (lseek(db->pagf, CSDB_OFF_PAG(db->pagbno), SEEK_SET) < 0
		    || write(db->pagf, db->pagbuf, CSDB_PBLKSIZ) < 0)
			return 0;
	} while (--smax);
	return 0;
}

// ---- public DB API ----

CSDB *csdb_open(const char *file, int flags, int mode) {
	CSDB *db;
	struct stat dstat;
	char dirname[1024], pagname[1024];
	snprintf(dirname, sizeof(dirname), "%s.dir", file);
	snprintf(pagname, sizeof(pagname), "%s.pag", file);
	db = (CSDB *) malloc(sizeof(CSDB));
	if (db == NULL)
		return NULL;
	db->flags = 0;
	db->hmask = 0;
	db->blkptr = 0;
	db->keyptr = 0;
	if (flags & O_WRONLY)
		flags = (flags & ~O_WRONLY) | O_RDWR;
	else if ((flags & 03) == O_RDONLY)
		db->flags = CSDB_RDONLY;
	db->pagf = open(pagname, flags, mode);
	if (db->pagf < 0) { free(db); return NULL; }
	db->dirf = open(dirname, flags, mode);
	if (db->dirf < 0) { close(db->pagf); free(db); return NULL; }
	if (fstat(db->dirf, &dstat) != 0) {
		close(db->dirf); close(db->pagf); free(db); return NULL;
	}
	db->dirbno = (!dstat.st_size) ? 0 : -1;
	db->pagbno = -1;
	db->maxbno = dstat.st_size * CSDB_BYTESIZ;
	memset(db->pagbuf, 0, CSDB_PBLKSIZ);
	memset(db->dirbuf, 0, CSDB_DBLKSIZ);
	return db;
}

int csdb_store(CSDB *db, csdb_datum key, csdb_datum val, int flags) {
	int need;
	long hash;
	if (db == NULL || csdb_bad(key))
		return -1;
	if (db->flags & CSDB_RDONLY)
		return -1;
	need = key.dsize + val.dsize;
	if (need < 0 || need > CSDB_PAIRMAX)
		return -1;
	if (csdb_getpage(db, (hash = csdb_exhash(key)))) {
		if (flags == CSDB_REPLACE)
			csdb_delpair(db->pagbuf, key);
		else if (csdb_duppair(db->pagbuf, key))
			return 1;
		if (!csdb_fitpair(db->pagbuf, need))
			if (!csdb_makroom(db, hash, need))
				return csdb_ioerr(db), -1;
		csdb_putpair(db->pagbuf, key, val);
		if (lseek(db->pagf, CSDB_OFF_PAG(db->pagbno), SEEK_SET) < 0
		    || write(db->pagf, db->pagbuf, CSDB_PBLKSIZ) < 0)
			return csdb_ioerr(db), -1;
		return 0;
	}
	return csdb_ioerr(db), -1;
}

csdb_datum csdb_fetch(CSDB *db, csdb_datum key) {
	if (db == NULL || csdb_bad(key))
		return csdb_nullitem;
	if (csdb_getpage(db, csdb_exhash(key)))
		return csdb_getpair(db->pagbuf, key);
	return csdb_ioerr(db), csdb_nullitem;
}

int csdb_delete(CSDB *db, csdb_datum key) {
	if (db == NULL || csdb_bad(key))
		return -1;
	if (db->flags & CSDB_RDONLY)
		return -1;
	if (csdb_getpage(db, csdb_exhash(key))) {
		if (!csdb_delpair(db->pagbuf, key))
			return -1;
		if (lseek(db->pagf, CSDB_OFF_PAG(db->pagbno), SEEK_SET) < 0
		    || write(db->pagf, db->pagbuf, CSDB_PBLKSIZ) < 0)
			return csdb_ioerr(db), -1;
		return 0;
	}
	return csdb_ioerr(db), -1;
}

void csdb_close(CSDB *db) {
	if (db == NULL)
		return;
	close(db->dirf);
	close(db->pagf);
	free(db);
}
*/
import "C"

import (
	"fmt"
	"unsafe"
)

// CDB wraps a C-level SDBM database handle for differential testing.
type CDB struct {
	ptr *C.CSDB
}

// OpenCDB opens (or creates) an SDBM database using the C implementation.
func OpenCDB(path string, flags, mode int) (*CDB, error) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	ptr := C.csdb_open(cpath, C.int(flags), C.int(mode))
	if ptr == nil {
		return nil, fmt.Errorf("csdb_open failed: %s", path)
	}
	return &CDB{ptr: ptr}, nil
}

// Store inserts or replaces a key-value pair. Returns the C return code:
// 0 = success, 1 = duplicate (INSERT mode), -1 = error.
func (db *CDB) Store(key, val []byte, flags int) int {
	ck := toCDatum(key)
	cv := toCDatum(val)
	return int(C.csdb_store(db.ptr, ck, cv, C.int(flags)))
}

// Fetch retrieves the value for a key. Returns nil if not found.
func (db *CDB) Fetch(key []byte) []byte {
	ck := toCDatum(key)
	result := C.csdb_fetch(db.ptr, ck)
	if result.dptr == nil {
		return nil
	}
	return C.GoBytes(unsafe.Pointer(result.dptr), result.dsize)
}

// Delete removes a key-value pair. Returns the C return code:
// 0 = success, -1 = not found or error.
func (db *CDB) Delete(key []byte) int {
	ck := toCDatum(key)
	return int(C.csdb_delete(db.ptr, ck))
}

// Close closes the database and frees resources.
func (db *CDB) Close() {
	if db.ptr != nil {
		C.csdb_close(db.ptr)
		db.ptr = nil
	}
}

func toCDatum(data []byte) C.csdb_datum {
	var d C.csdb_datum
	if len(data) > 0 {
		d.dptr = (*C.char)(unsafe.Pointer(&data[0]))
	}
	d.dsize = C.int(len(data))
	return d
}
