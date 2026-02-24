package sdbm

import (
	"errors"
	"fmt"
	"io"
	"os"
)

var debug bool

const (
	// BITSIZ represents the number of bits per byte.
	BITSIZ = 8
	// DBLKSIZ defines the block size (in bytes) for a .dir file.
	DBLKSIZ = 4096
	// PBLKSIZ defines the block size (in bytes) for a .pag file.
	PBLKSIZ = 1024
	// PAIRMAX defines the maximum size (in bytes) of a key-value pair.
	PAIRMAX = 1008
	// SPLTMAX defines the maximum number of page splits allowed during insertion.
	SPLTMAX = 10
	// SHORTSIZE defines the size (in bytes) of a short integer.
	SHORTSIZE = 2

	// DIRFEXT is the file extension for the directory (.dir) file.
	DIRFEXT = ".dir" // dir file extensions
	// PAGFEXT is the file extension for the page (.pag) file.
	PAGFEXT = ".pag" // pag file extensions
)

// StoreFlags represents flags for store operation in SDBM.
type StoreFlags int

const (
	// StoreREPLACE indicates that if the key already exists, the value should be replaced.
	StoreREPLACE StoreFlags = iota + 1
	// StoreSEEDUPS indicates that duplicates should be avoided during insertion.
	StoreSEEDUPS
)

var (
	// ErrInvalidArgument indicates that an invalid argument was provided.
	ErrInvalidArgument = errors.New("invalid argument")
	// ErrInvalidPage indicates that the page being accessed is invalid.
	ErrInvalidPage = errors.New("invalid page")
	// ErrDBMRDOnly indicates that a write operation was attempted on a read-only database.
	ErrDBMRDOnly = errors.New("dbm read only")
)

// IOError records an error along with the operation and file path that caused it.
type IOError struct {
	Op   string // The operation that caused the error (e.g., "open", "read", "write").
	Path string // The path of the file where the error occurred.
	Err  error  // The original error that was encountered.
}

// Error returns a string representation of the IOError.
// It formats the error message as "operation file_path: error_message".
func (e *IOError) Error() string {
	return e.Op + " " + e.Path + ": " + e.Err.Error()
}

// Unwrap returns the underlying error wrapped by IOError.
// This allows access to the original error using errors.Unwrap and similar functions.
func (e *IOError) Unwrap() error {
	return e.Err
}

func bad(x Datum) bool {
	return x == nil
}

func exHash(item Datum) int64 {
	return Hash(item)
}

func wrapIOErr(op, path string, err error) error {
	return &IOError{Op: op, Path: path, Err: err}
}

func offPag(off int64) int64 {
	return off * PBLKSIZ
}

func offDir(off int64) int64 {
	return off * DBLKSIZ
}

func seekWrite(f *os.File, offset int64, whence int, buf []byte) error {
	if _, err := f.Seek(offset, whence); err != nil {
		return wrapIOErr("seek", f.Name(), err)
	}
	if _, err := f.Write(buf); err != nil {
		return wrapIOErr("write", f.Name(), err)
	}
	return nil
}

func seekRead(f *os.File, offset int64, whence int, buf []byte) error {
	if _, err := f.Seek(offset, whence); err != nil {
		return wrapIOErr("seek", f.Name(), err)
	}
	if _, err := f.Read(buf); err != nil {
		if !errors.Is(err, io.EOF) {
			return wrapIOErr("read", f.Name(), err)
		}
	}
	return nil
}

var masks = []int64{
	000000000000, 000000000001, 000000000003, 000000000007,
	000000000017, 000000000037, 000000000077, 000000000177,
	000000000377, 000000000777, 000000001777, 000000003777,
	000000007777, 000000017777, 000000037777, 000000077777,
	000000177777, 000000377777, 000000777777, 000001777777,
	000003777777, 000007777777, 000017777777, 000037777777,
	000077777777, 000177777777, 000377777777, 000777777777,
	001777777777, 003777777777, 007777777777, 017777777777,
}

// Datum represents a data item, typically used as a key or value in the SDBM database.
// It is a byte slice that can hold arbitrary data.
type Datum []byte

// Size returns the length of the data item in bytes.
func (d Datum) Size() int {
	return len(d)
}

// String converts the Datum to a string.
// This is useful when the Datum contains string data and needs to be represented as a string.
func (d Datum) String() string {
	return string(d)
}

// Nullitem is a special value representing an empty or null Datum.
var Nullitem = Datum(nil)

// DBM represents a simple database manager for SDBM files.
// It manages the directory (.dir) and page (.pag) files that store the key-value pairs.
type DBM struct {
	dirf   *os.File      // directory file
	pagf   *os.File      // page file
	rdonly bool          // read only flag
	maxbno int64         // size of dirfile in bits
	curbit int64         // current bit number
	hmask  int64         // current hash mask
	blkptr int64         // current block for next key
	keyptr int           // current key for next key
	pagbno int64         // current page in pag
	pag    *Page         // page file block buffer
	dirbno int64         // current block in dirbuf
	dirbuf [DBLKSIZ]byte // directory file block buffer
}

// Open initializes and opens an SDBM database from the specified file.
// It accepts the file name, flags (such as read/write permissions), and file mode.
// It returns a DBM pointer and an error if opening the database fails.
func Open(file string, flags int, mode os.FileMode) (*DBM, error) {
	if file == "" {
		return nil, ErrInvalidArgument
	}

	dirname := file + DIRFEXT
	pagname := file + PAGFEXT

	db, err := Prep(dirname, pagname, flags, mode)
	if err != nil {
		return nil, err
	}

	return db, nil
}

// Prep prepares the DBM structure by opening the directory (.dir) and page (.pag) files.
// It adjusts the flags to handle read/write modes and sets the internal read-only flag if necessary.
// It returns a pointer to the initialized DBM structure and an error if any step fails.
func Prep(dirname, pagname string, flags int, mode os.FileMode) (*DBM, error) {
	db := &DBM{}
	// adjust user flags so that WRONLY becomes RDWR,
	// as required by this package. Also set our internal
	// flag for RDONLY if needed.
	if flags&os.O_WRONLY != 0 {
		flags = (flags &^ os.O_WRONLY) | os.O_RDWR
	} else if flags == os.O_RDONLY {
		db.rdonly = true
	}

	// open the files in sequence, and stat the dirfile.
	// If we fail anywhere, undo everything, return NULL.
	var err error
	db.dirf, err = os.OpenFile(dirname, flags, mode)
	if err != nil {
		return nil, err
	}
	db.pagf, err = os.OpenFile(pagname, flags, mode)
	if err != nil {
		_ = db.dirf.Close()
		return nil, err
	}

	fileInfo, err := db.dirf.Stat()
	if err != nil {
		_ = db.dirf.Close()
		_ = db.pagf.Close()
		return nil, err
	}

	// need the dirfile size to establish max bit number.
	//
	// zero size: either a fresh database, or one with a single,
	// unsplit data page: dirpage is all zeros.
	if fileInfo.Size() == 0 {
		db.dirbno = 0
	} else {
		db.dirbno = -1
	}
	db.pagbno = -1
	db.maxbno = fileInfo.Size() * BITSIZ

	db.pag = &Page{}

	return db, nil
}

// Close closes the DBM database by closing both the directory (.dir) and page (.pag) files.
// It returns an error if there is an issue closing either of the files.
func (db *DBM) Close() error {
	errDir := db.dirf.Close()
	errPag := db.pagf.Close()

	if errDir != nil {
		return wrapIOErr("close", db.dirf.Name(), errDir)
	}
	if errPag != nil {
		return wrapIOErr("close", db.pagf.Name(), errPag)
	}
	return nil
}

// Fetch retrieves the value associated with the given key from the database.
// It returns the value and an error if the key is invalid or if there is a problem accessing the page.
func (db *DBM) Fetch(key Datum) (Datum, error) {
	if bad(key) {
		return Nullitem, ErrInvalidArgument
	}

	hash := exHash(key)
	if err := db.getPage(hash); err != nil {
		return Nullitem, err
	}

	return db.pag.GetPair(key), nil
}

// Delete removes the key-value pair associated with the given key from the database.
// It returns a boolean indicating success or failure, and an error if the key is invalid,
// the database is read-only, or there is a problem accessing the page.
func (db *DBM) Delete(key Datum) (bool, error) {
	if bad(key) {
		return false, ErrInvalidArgument
	}
	if db.rdonly {
		return false, ErrDBMRDOnly
	}

	hash := exHash(key)
	if err := db.getPage(hash); err != nil {
		return false, err
	}
	if !db.pag.DelPair(key) {
		return false, nil
	}

	// update the page file
	if err := seekWrite(db.pagf, offPag(db.pagbno), io.SeekStart, db.pag.buf[:]); err != nil {
		return false, err
	}

	return true, nil
}

// Store inserts or updates a key-value pair in the database.
// If the key already exists and StoreREPLACE is specified, the value is replaced.
// If StoreSEEDUPS is specified, duplicates are not allowed.
// It returns a boolean indicating success and an error if the operation fails or if the database is read-only.
func (db *DBM) Store(key, val Datum, flags StoreFlags) (bool, error) {
	if bad(key) {
		return false, ErrInvalidArgument
	}

	if db.rdonly {
		return false, ErrDBMRDOnly
	}

	need := key.Size() + val.Size()

	// is the pair too big (or too small) for this database ??
	if need < 0 || need > PAIRMAX {
		return false, ErrInvalidArgument
	}

	hash := exHash(key)
	if err := db.getPage(hash); err != nil {
		return false, err
	}

	// if we need to replace, delete the key/data pair
	// first. If it is not there, ignore.
	if flags == StoreREPLACE {
		_ = db.pag.DelPair(key)
	} else if flags == StoreSEEDUPS && db.pag.DupPair(key) {
		// success
		return true, nil
	}

	// if we do not have enough room, we have to split.
	if !db.pag.FitPair(need) {
		if err := db.makeRoom(hash, need); err != nil {
			return false, err
		}
	}

	// we have enough room or split is successful. insert the key,
	// and update the page file.
	db.pag.PutPair(key, val)

	if err := seekWrite(db.pagf, offPag(db.pagbno), io.SeekStart, db.pag.buf[:]); err != nil {
		return false, err
	}

	// success
	return true, nil
}

// makeRoom - make room by splitting the overfull page
// this routine will attempt to make room for SPLTMAX times before
// giving up.
func (db *DBM) makeRoom(hash int64, need int) error {
	var newp int64
	var twin [PBLKSIZ]byte
	pag := db.pag.buf[:]
	newPag := &Page{
		buf: twin,
	}
	smax := SPLTMAX

	for smax--; smax > 0; smax-- {
		// split the current page
		db.pag.SplPage(newPag, db.hmask+1)

		//  address of the new page
		newp = (hash & db.hmask) | (db.hmask + 1)

		// write delay, read avoidence/cache shuffle:
		// select the page for incoming pair: if key is to go to the new page,
		// write out the previous one, and copy the new one over, thus making
		// it the current page. If not, simply write the new page, and we are
		// still looking at the page of interest. current page is not updated
		// here, as dbm_store will do so, after it inserts the incoming pair.
		if hash&(db.hmask+1) != 0 {
			if err := seekWrite(db.pagf, offPag(db.pagbno), io.SeekStart, db.pag.buf[:]); err != nil {
				return err
			}
			db.pagbno = newp
			copy(pag, newPag.buf[:])
		} else {
			if err := seekWrite(db.pagf, offPag(newp), io.SeekStart, newPag.buf[:]); err != nil {
				return err
			}
		}

		if err := db.setDBit(db.curbit); err != nil {
			return err
		}

		// see if we have enough room now
		if db.pag.FitPair(need) {
			return nil
		}

		// try again... update curbit and hmask as getpage would have
		// done. because of our update of the current page, we do not
		// need to read in anything. BUT we have to write the current
		// [deferred] page out, as the window of failure is too great.
		if (hash & (db.hmask + 1)) != 0 {
			db.curbit = 2*db.curbit + 2
		} else {
			db.curbit = 2*db.curbit + 1
		}
		db.hmask |= db.hmask + 1

		if err := seekWrite(db.pagf, offPag(db.pagbno), io.SeekStart, db.pag.buf[:]); err != nil {
			return err
		}
	}

	// if we are here, this is real bad news. After SPLTMAX splits,
	// we still cannot fit the key. say goodnight.
	if debug {
		fmt.Println("sdbm: cannot insert after SPLTMAX attempts.")
	}

	return nil
}

// FirstKey retrieves the first key in the database.
// This function initializes the reading of the first page (page 0) and sets the current pointers (pagbno, blkptr, keyptr) to 0.
// If an error occurs while reading the page, it returns an error.
// Note: These routines may fail if deletions are not accounted for, due to an ndbm bug.
func (db *DBM) FirstKey() (Datum, error) {
	// start at page 0
	if err := seekRead(db.pagf, offPag(0), io.SeekStart, db.pag.buf[:]); err != nil {
		return Nullitem, err
	}
	db.pagbno = 0
	db.blkptr = 0
	db.keyptr = 0

	return db.getNext()
}

// NextKey retrieves the next key in the database after FirstKey or after the last key retrieved by a previous call to NextKey.
// If the current page has more keys, it returns the next one; otherwise, it moves to the next page to continue searching for keys.
// Note: These routines may fail if deletions are not accounted for, due to an ndbm bug.
func (db *DBM) NextKey() (Datum, error) {
	return db.getNext()
}

// all important binary trie traversal.
func (db *DBM) getPage(hash int64) error {
	var dbit, hbit int64
	for dbit < db.maxbno && db.getDBit(dbit) {
		if hash&(1<<hbit) != 0 {
			dbit = 2*dbit + 2
		} else {
			dbit = 2*dbit + 1
		}
		hbit++
	}
	if debug {
		fmt.Printf("dbit: %d...\n", dbit)
	}

	db.curbit = dbit
	db.hmask = masks[hbit]

	pagb := hash & db.hmask

	// see if the block we need is already in memory.
	// note: this lookaside cache has about 10% hit rate.
	if pagb != db.pagbno {
		// note: here, we assume a "hole" is read as 0s.
		// if not, must zero pag first.
		if err := seekRead(db.pagf, offPag(pagb), io.SeekStart, db.pag.buf[:]); err != nil {
			return err
		}
		if !db.pag.ChkPage() {
			return ErrInvalidPage
		}
		db.pagbno = pagb

		if debug {
			fmt.Printf("pag read: %d\n", pagb)
		}
	}

	return nil
}

func (db *DBM) getDBit(dbit int64) bool {
	c := dbit / BITSIZ
	dirb := c / DBLKSIZ

	if dirb != db.dirbno {
		if err := seekRead(db.dirf, offDir(dirb), io.SeekStart, db.dirbuf[:]); err != nil {
			return false
		}
		db.dirbno = dirb

		if debug {
			fmt.Printf("dir read: %d\n", dirb)
		}
	}

	return int(db.dirbuf[c%DBLKSIZ]&(1<<(dbit%BITSIZ))) != 0
}

func (db *DBM) setDBit(dbit int64) error {
	c := dbit / BITSIZ
	dirb := c / DBLKSIZ

	if dirb != db.dirbno {
		if err := seekRead(db.dirf, offDir(dirb), io.SeekStart, db.dirbuf[:]); err != nil {
			return err
		}
		db.dirbno = dirb

		if debug {
			fmt.Printf("dir read: %d\n", dirb)
		}
	}

	db.dirbuf[c%DBLKSIZ] |= 1 << (dbit % BITSIZ)

	if dbit >= db.maxbno {
		db.maxbno += DBLKSIZ * BITSIZ
	}

	if err := seekWrite(db.dirf, offDir(dirb), io.SeekStart, db.dirbuf[:]); err != nil {
		return err
	}

	return nil
}

// getNext - get the next key in the page, and if done with
// the page, try the next page in sequence.
func (db *DBM) getNext() (Datum, error) {
	var key Datum

	for {
		db.keyptr++
		key = db.pag.GetNKey(db.keyptr)
		if key != nil {
			return key, nil
		}

		// we either run out, or there is nothing on this page...
		// try the next one... If we lost our position on the
		// file, we will have to seek.
		db.keyptr = 0
		if db.pagbno != db.blkptr {
			db.blkptr++
			if _, err := db.pagf.Seek(offPag(db.blkptr), io.SeekStart); err != nil {
				return Nullitem, wrapIOErr("seek", db.pagf.Name(), err)
			}
		}

		db.pagbno = db.blkptr
		if _, err := db.pagf.Read(db.pag.buf[:]); err != nil {
			if errors.Is(err, io.EOF) {
				return Nullitem, nil
			}
			return Nullitem, wrapIOErr("read", db.pagf.Name(), err)
		}

		if !db.pag.ChkPage() {
			return Nullitem, ErrInvalidPage
		}
	}
}
