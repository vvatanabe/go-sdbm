/*
 * gen_golden.c - Generate golden SDBM database files for Go compatibility tests.
 *
 * This is a self-contained program that implements the core SDBM routines inline
 * to avoid include-guard issues with the original K&R C sources.
 *
 * Generates:
 *   1. ascii_small: 100 entries (key001..key100, val001..val100)
 *   2. many_splits: 10000 entries (key00001..key10000, val00001..val10000)
 *
 * Build: cc -o gen_golden gen_golden.c
 * Usage: ./gen_golden
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>

/* --- Constants (from sdbm.h / tune.h) --- */

#define DBLKSIZ 4096
#define PBLKSIZ 1024
#define PAIRMAX 1008
#define SPLTMAX 10
#define BYTESIZ 8

#define DBM_RDONLY 0x1
#define DBM_IOERR  0x2
#define DBM_INSERT  0
#define DBM_REPLACE 1

/* --- Types (from sdbm.h) --- */

typedef struct {
    char *dptr;
    int dsize;
} datum;

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
    char pagbuf[PBLKSIZ];
    long dirbno;
    char dirbuf[DBLKSIZ];
} DBM;

static datum nullitem = {NULL, 0};

#define bad(x)      ((x).dptr == NULL || (x).dsize <= 0)
#define exhash(item) dbm_hash((item).dptr, (item).dsize)
#define ioerr(db)   ((db)->flags |= DBM_IOERR)

#define OFF_PAG(off) (long)(off) * PBLKSIZ
#define OFF_DIR(off) (long)(off) * DBLKSIZ

/* --- Masks (from sdbm.c) --- */

static long masks[] = {
    000000000000, 000000000001, 000000000003, 000000000007,
    000000000017, 000000000037, 000000000077, 000000000177,
    000000000377, 000000000777, 000000001777, 000000003777,
    000000007777, 000000017777, 000000037777, 000000077777,
    000000177777, 000000377777, 000000777777, 000001777777,
    000003777777, 000007777777, 000017777777, 000037777777,
    000077777777, 000177777777, 000377777777, 000777777777,
    001777777777, 003777777777, 007777777777, 017777777777
};

/* --- hash.c --- */

static long dbm_hash(char *str, int len) {
    unsigned long n = 0;
    while (len--)
        n = *str++ + 65599 * n;
    return (long)n;
}

/* --- pair.c --- */

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

static int fitpair(char *pag, int need) {
    int n;
    int off;
    int avail;
    short *ino = (short *) pag;

    off = ((n = ino[0]) > 0) ? ino[n] : PBLKSIZ;
    avail = off - (n + 1) * (int)sizeof(short);
    need += 2 * (int)sizeof(short);

    return need <= avail;
}

static void putpair(char *pag, datum key, datum val) {
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

static int duppair(char *pag, datum key) {
    short *ino = (short *) pag;
    return ino[0] > 0 && seepair(pag, ino[0], key.dptr, key.dsize) > 0;
}

static int delpair(char *pag, datum key) {
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

static int chkpage(char *pag) {
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

static void splpage(char *pag, char *newbuf, long sbit) {
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

/* --- sdbm core routines --- */

static int getdbit(DBM *db, long dbit) {
    long c = dbit / BYTESIZ;
    long dirb = c / DBLKSIZ;

    if (dirb != db->dirbno) {
        if (lseek(db->dirf, OFF_DIR(dirb), SEEK_SET) < 0
            || read(db->dirf, db->dirbuf, DBLKSIZ) < 0)
            return 0;
        db->dirbno = dirb;
    }
    return db->dirbuf[c % DBLKSIZ] & (1 << dbit % BYTESIZ);
}

static int setdbit(DBM *db, long dbit) {
    long c = dbit / BYTESIZ;
    long dirb = c / DBLKSIZ;

    if (dirb != db->dirbno) {
        if (lseek(db->dirf, OFF_DIR(dirb), SEEK_SET) < 0
            || read(db->dirf, db->dirbuf, DBLKSIZ) < 0)
            return 0;
        db->dirbno = dirb;
    }

    db->dirbuf[c % DBLKSIZ] |= (1 << dbit % BYTESIZ);

    if (dbit >= db->maxbno)
        db->maxbno += DBLKSIZ * BYTESIZ;

    if (lseek(db->dirf, OFF_DIR(dirb), SEEK_SET) < 0
        || write(db->dirf, db->dirbuf, DBLKSIZ) < 0)
        return 0;

    return 1;
}

static int getpage(DBM *db, long hash) {
    int hbit = 0;
    long dbit = 0;
    long pagb;

    while (dbit < db->maxbno && getdbit(db, dbit))
        dbit = 2 * dbit + ((hash & (1 << hbit++)) ? 2 : 1);

    db->curbit = dbit;
    db->hmask = masks[hbit];

    pagb = hash & db->hmask;

    if (pagb != db->pagbno) {
        if (lseek(db->pagf, OFF_PAG(pagb), SEEK_SET) < 0
            || read(db->pagf, db->pagbuf, PBLKSIZ) < 0)
            return 0;
        if (!chkpage(db->pagbuf))
            return 0;
        db->pagbno = pagb;
    }
    return 1;
}

static int makroom(DBM *db, long hash, int need) {
    long newp;
    char twin[PBLKSIZ];
    char *pag = db->pagbuf;
    char *newbuf = twin;
    int smax = SPLTMAX;

    do {
        (void) splpage(pag, newbuf, db->hmask + 1);
        newp = (hash & db->hmask) | (db->hmask + 1);

        if (hash & (db->hmask + 1)) {
            if (lseek(db->pagf, OFF_PAG(db->pagbno), SEEK_SET) < 0
                || write(db->pagf, db->pagbuf, PBLKSIZ) < 0)
                return 0;
            db->pagbno = newp;
            (void) memcpy(pag, newbuf, PBLKSIZ);
        }
        else if (lseek(db->pagf, OFF_PAG(newp), SEEK_SET) < 0
                 || write(db->pagf, newbuf, PBLKSIZ) < 0)
            return 0;

        if (!setdbit(db, db->curbit))
            return 0;

        if (fitpair(pag, need))
            return 1;

        db->curbit = 2 * db->curbit +
            ((hash & (db->hmask + 1)) ? 2 : 1);
        db->hmask |= db->hmask + 1;

        if (lseek(db->pagf, OFF_PAG(db->pagbno), SEEK_SET) < 0
            || write(db->pagf, db->pagbuf, PBLKSIZ) < 0)
            return 0;

    } while (--smax);

    return 0;
}

/* --- DB open/store/close --- */

static DBM *my_dbm_open(const char *file, int flags, int mode) {
    DBM *db;
    struct stat dstat;
    char dirname[1024];
    char pagname[1024];

    snprintf(dirname, sizeof(dirname), "%s.dir", file);
    snprintf(pagname, sizeof(pagname), "%s.pag", file);

    db = (DBM *) malloc(sizeof(DBM));
    if (db == NULL)
        return NULL;

    db->flags = 0;
    db->hmask = 0;
    db->blkptr = 0;
    db->keyptr = 0;

    if (flags & O_WRONLY)
        flags = (flags & ~O_WRONLY) | O_RDWR;
    else if ((flags & 03) == O_RDONLY)
        db->flags = DBM_RDONLY;

    db->pagf = open(pagname, flags, mode);
    if (db->pagf < 0) {
        free(db);
        return NULL;
    }
    db->dirf = open(dirname, flags, mode);
    if (db->dirf < 0) {
        close(db->pagf);
        free(db);
        return NULL;
    }

    if (fstat(db->dirf, &dstat) != 0) {
        close(db->dirf);
        close(db->pagf);
        free(db);
        return NULL;
    }

    db->dirbno = (!dstat.st_size) ? 0 : -1;
    db->pagbno = -1;
    db->maxbno = dstat.st_size * BYTESIZ;

    (void) memset(db->pagbuf, 0, PBLKSIZ);
    (void) memset(db->dirbuf, 0, DBLKSIZ);

    return db;
}

static int my_dbm_store(DBM *db, datum key, datum val, int flags) {
    int need;
    long hash;

    if (db == NULL || bad(key))
        return -1;
    if (db->flags & DBM_RDONLY)
        return -1;

    need = key.dsize + val.dsize;
    if (need < 0 || need > PAIRMAX)
        return -1;

    if (getpage(db, (hash = exhash(key)))) {
        if (flags == DBM_REPLACE)
            (void) delpair(db->pagbuf, key);
        else if (duppair(db->pagbuf, key))
            return 1;

        if (!fitpair(db->pagbuf, need))
            if (!makroom(db, hash, need))
                return ioerr(db), -1;

        (void) putpair(db->pagbuf, key, val);

        if (lseek(db->pagf, OFF_PAG(db->pagbno), SEEK_SET) < 0
            || write(db->pagf, db->pagbuf, PBLKSIZ) < 0)
            return ioerr(db), -1;

        return 0;
    }

    return ioerr(db), -1;
}

static void my_dbm_close(DBM *db) {
    if (db == NULL)
        return;
    close(db->dirf);
    close(db->pagf);
    free(db);
}

/* --- Main --- */

static int generate_db(const char *name, const char *keyfmt, const char *valfmt, int count) {
    DBM *db;
    datum key, val;
    char keybuf[64], valbuf[64];
    int i;
    char path[1024];

    snprintf(path, sizeof(path), "%s.dir", name);
    unlink(path);
    snprintf(path, sizeof(path), "%s.pag", name);
    unlink(path);

    db = my_dbm_open(name, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (db == NULL) {
        fprintf(stderr, "Failed to open %s\n", name);
        return -1;
    }

    for (i = 1; i <= count; i++) {
        snprintf(keybuf, sizeof(keybuf), keyfmt, i);
        snprintf(valbuf, sizeof(valbuf), valfmt, i);

        key.dptr = keybuf;
        key.dsize = (int)strlen(keybuf);
        val.dptr = valbuf;
        val.dsize = (int)strlen(valbuf);

        if (my_dbm_store(db, key, val, DBM_INSERT) != 0) {
            fprintf(stderr, "Failed to store entry %d in %s\n", i, name);
            my_dbm_close(db);
            return -1;
        }
    }

    my_dbm_close(db);
    printf("Generated %s: %d entries\n", name, count);
    return 0;
}

int main(void) {
    int ret = 0;

    printf("Generating golden SDBM database files...\n");

    if (generate_db("ascii_small", "key%03d", "val%03d", 100) != 0)
        ret = 1;

    if (generate_db("many_splits", "key%05d", "val%05d", 10000) != 0)
        ret = 1;

    if (ret == 0)
        printf("All golden files generated successfully.\n");
    else
        printf("Some golden files failed to generate.\n");

    return ret;
}
