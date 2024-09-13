package sdbm_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"

	"github.com/vvatanabe/go-sdbm"
)

const DBMFile = "sdbm_test"

type Pair struct {
	Key sdbm.Datum
	Val sdbm.Datum
}

func setup(t *testing.T, initialData ...Pair) (string, *sdbm.DBM) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, DBMFile)
	db, err := sdbm.Open(path, os.O_RDWR|os.O_CREATE, os.FileMode(0644))
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	for _, pair := range initialData {
		ok, err := db.Store(pair.Key, pair.Val, 0)
		if err != nil {
			t.Fatalf("failed to store key=%s, val=%s: %v", pair.Key, pair.Val, err)
		}
		if !ok {
			t.Fatalf("failed to store key=%s, val=%s", pair.Key, pair.Val)
		}
	}
	return dir, db
}

func teardown(t *testing.T, dbm *sdbm.DBM) {
	t.Helper()
	err := dbm.Close()
	if err != nil {
		t.Errorf("failed to close db: %v", err)
	}
}

func generatePairs(key, val string, n int) []Pair {
	data := make([]Pair, n)
	for i := 0; i < n; i++ {
		num := strconv.Itoa(i + 1)
		data[i] = Pair{
			Key: sdbm.Datum(key + num),
			Val: sdbm.Datum(val + num),
		}
	}
	return data
}

func TestDBM_Datum(t *testing.T) {
	datum := sdbm.Datum("key")
	if datum.Size() != 3 {
		t.Errorf("datum.Size() got = %v, want %v", datum.Size(), 3)
	}
	if datum.String() != "key" {
		t.Errorf("datum.String() got = %v, want %v", datum.String(), "key")
	}
}

func TestDBM_IOError(t *testing.T) {
	err := errors.New("dummy")
	ioerr := sdbm.IOError{
		Op:   "read",
		Path: "/foo/bar/baz.pag",
		Err:  err,
	}
	if !errors.Is(ioerr.Unwrap(), err) {
		t.Errorf("IOError.Unwrap() got = %v, want %v", ioerr.Unwrap(), err)
	}
	want := "read /foo/bar/baz.pag: dummy"
	if ioerr.Error() != want {
		t.Errorf("IOError.Error() got = %v, want %v", ioerr.Error(), want)
	}
}

func TestOpen_EmptyDBM(t *testing.T) {
	_, dbm := setup(t)
	defer teardown(t, dbm)

	got, err := dbm.Fetch(sdbm.Datum("key"))
	if err != nil {
		t.Errorf("Fetch() error = %v", err)
		return
	}
	if !reflect.DeepEqual(got, sdbm.Nullitem) {
		t.Errorf("Fetch() got = %v, want %v", got, sdbm.Nullitem)
	}

	ok, err := dbm.Delete(sdbm.Datum("key"))
	if err != nil {
		t.Errorf("Delete() error = %v", err)
		return
	}
	if ok {
		t.Errorf("Delete() got = %v, want %v", ok, false)
	}

	firstKey, err := dbm.FirstKey()
	if err != nil {
		t.Errorf("FirstKey() error = %v", err)
		return
	}
	if !reflect.DeepEqual(firstKey, sdbm.Nullitem) {
		t.Errorf("FirstKey() got = %v, want %v", got, sdbm.Nullitem)
	}

	nextKey, err := dbm.NextKey()
	if err != nil {
		t.Errorf("NextKey() error = %v", err)
		return
	}
	if !reflect.DeepEqual(nextKey, sdbm.Nullitem) {
		t.Errorf("NextKey() got = %v, want %v", got, sdbm.Nullitem)
	}

	ok, err = dbm.Store(sdbm.Datum("key"), sdbm.Datum("val"), sdbm.StoreSEEDUPS)
	if err != nil {
		t.Errorf("Store() error = %v", err)
		return
	}
	if !ok {
		t.Errorf("Delete() got = %v, want %v", ok, true)
	}
}

func TestOpen_InvalidArgument(t *testing.T) {
	_, err := sdbm.Open("", 0, 0)
	if !errors.Is(err, sdbm.ErrInvalidArgument) {
		t.Errorf("Open() error = %v, wantErr %v", err, sdbm.ErrInvalidArgument)
	}

	_, err = sdbm.Open(filepath.Join(t.TempDir(), "foo"), 0, 0)
	if err == nil {
		t.Error("Open() want error")
	}
}

func TestDBM_Fetch(t *testing.T) {
	_, dbm := setup(t, generatePairs("key", "val", 10)...)
	defer teardown(t, dbm)

	type args struct {
		key sdbm.Datum
	}
	tests := []struct {
		name    string
		args    args
		want    sdbm.Datum
		wantErr bool
	}{
		{
			name: "key is nil",
			args: args{
				key: nil,
			},
			want:    sdbm.Nullitem,
			wantErr: true,
		},
		{
			name: "key0 is not found",
			args: args{
				key: sdbm.Datum("key0"),
			},
			want:    sdbm.Nullitem,
			wantErr: false,
		},
		{
			name: "key1 is found",
			args: args{
				key: sdbm.Datum("key1"),
			},
			want:    sdbm.Datum("val1"),
			wantErr: false,
		},
		{
			name: "key10 is found",
			args: args{
				key: sdbm.Datum("key10"),
			},
			want:    sdbm.Datum("val10"),
			wantErr: false,
		},
		{
			name: "key11 is not found",
			args: args{
				key: sdbm.Datum("key11"),
			},
			want:    sdbm.Nullitem,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := dbm.Fetch(tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("Fetch() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Fetch() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDBM_Store(t *testing.T) {
	_, dbm := setup(t, generatePairs("key", "val", 10)...)
	defer teardown(t, dbm)

	type args struct {
		key   sdbm.Datum
		val   sdbm.Datum
		flags sdbm.StoreFlags
	}
	tests := []struct {
		name    string
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "key is nil",
			args: args{
				key:   nil,
				val:   nil,
				flags: 0,
			},
			want:    false,
			wantErr: true,
		},
		{
			name: "pair is too big",
			args: args{
				key:   bytes.Repeat([]byte("a"), sdbm.PAIRMAX),
				val:   sdbm.Datum("v"),
				flags: 0,
			},
			want:    false,
			wantErr: true,
		},
		{
			name: "key is duplicated and flags is 0",
			args: args{
				key:   sdbm.Datum("key1"),
				val:   sdbm.Datum("val1"),
				flags: 0,
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "key is not duplicated and flags is 0",
			args: args{
				key:   sdbm.Datum("key11"),
				val:   sdbm.Datum("val11"),
				flags: 0,
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "key is duplicated and flags is REPLACE",
			args: args{
				key:   sdbm.Datum("key2"),
				val:   sdbm.Datum("replaced2"),
				flags: sdbm.StoreREPLACE,
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "key is duplicated and flags is SEEDUPS",
			args: args{
				key:   sdbm.Datum("key3"),
				val:   sdbm.Datum("replaced3"),
				flags: sdbm.StoreSEEDUPS,
			},
			want:    true,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := dbm.Store(tt.args.key, tt.args.val, tt.args.flags)
			if (err != nil) != tt.wantErr {
				t.Errorf("Store() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Store() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDBM_Delete(t *testing.T) {
	_, dbm := setup(t, generatePairs("key", "val", 10)...)
	defer teardown(t, dbm)

	type args struct {
		key sdbm.Datum
	}
	tests := []struct {
		name    string
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "key is nil",
			args: args{
				key: nil,
			},
			want:    false,
			wantErr: true,
		},
		{
			name: "key0 is not found",
			args: args{
				key: sdbm.Datum("key0"),
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "key1 is found",
			args: args{
				key: sdbm.Datum("key1"),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "key10 is found",
			args: args{
				key: sdbm.Datum("key10"),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "key11 is not found",
			args: args{
				key: sdbm.Datum("key11"),
			},
			want:    false,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := dbm.Delete(tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("Delete() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Delete() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDBM_RDOnlyDBM(t *testing.T) {
	dir, dbm := setup(t, generatePairs("key", "val", 10)...)
	defer teardown(t, dbm)

	dbm2, err := sdbm.Open(filepath.Join(dir, DBMFile), os.O_RDONLY, os.FileMode(0644))
	if err != nil {
		t.Errorf("failed to open db: %v", err)
	}
	defer func() {
		_ = dbm2.Close()
	}()

	_, err1 := dbm2.Delete(sdbm.Datum("key1"))
	if !errors.Is(err1, sdbm.ErrDBMRDOnly) {
		t.Errorf("Delete() error = %v", err1)
	}

	_, err2 := dbm2.Store(sdbm.Datum("key1"), sdbm.Datum("val1"), 0)
	if !errors.Is(err2, sdbm.ErrDBMRDOnly) {
		t.Errorf("Store() error = %v", err2)
	}

}

func TestDBM_WRONLYDBM(t *testing.T) {
	dir, dbm := setup(t, generatePairs("key", "val", 100)...)
	defer teardown(t, dbm)

	dbm2, err := sdbm.Open(filepath.Join(dir, DBMFile), os.O_WRONLY, os.FileMode(0644))
	if err != nil {
		t.Errorf("failed to open db: %v", err)
	}
	defer func() {
		_ = dbm2.Close()
	}()

	key := sdbm.Datum("key1")
	val := sdbm.Datum("val1")

	datum, err := dbm2.Fetch(key)
	if err != nil {
		t.Errorf("Fetch() error = %v", err)
	}
	if !reflect.DeepEqual(datum, val) {
		t.Errorf("Fetch() got = %v, want %v", datum, val)
	}
}

func TestDBM_FirstKey(t *testing.T) {
	pairs := generatePairs("key", "val", 10)
	_, dbm := setup(t, pairs...)
	defer teardown(t, dbm)

	want := sdbm.Datum("key1")
	for i := 0; i < 20; i++ {
		key, err := dbm.FirstKey()
		if err != nil {
			t.Errorf("FirstKey() error = %v", err)
		}
		if !reflect.DeepEqual(key, want) {
			t.Errorf("FirstKey() got = %v, want %v", key, want)
		}
	}
}

func TestDBM_NextKey(t *testing.T) {
	pairs := generatePairs("key", "val", 10)
	_, dbm := setup(t, pairs...)
	defer teardown(t, dbm)

	for _, pair := range pairs {
		key, err := dbm.NextKey()
		if err != nil {
			t.Errorf("NextKey() error = %v", err)
		}
		if !reflect.DeepEqual(key, pair.Key) {
			t.Errorf("NextKey() got = %v, want %v", key, pair.Key)
		}
	}
}

func TestDBM_ManyPairs_Fetch(t *testing.T) {
	size := 100000
	pairs := generatePairs("key", "val", size)
	_, dbm := setup(t, pairs...)
	defer teardown(t, dbm)

	for i := 1; i <= size; i++ {
		key := sdbm.Datum("key" + strconv.Itoa(i))
		val, err := dbm.Fetch(key)
		if err != nil {
			t.Errorf("Fetch() error = %v", err)
		}
		if val.Size() == 0 {
			t.Errorf("Fetch() got = %v", val)
		}
		if !reflect.DeepEqual(val, pairs[i-1].Val) {
			t.Errorf("Fetch() got = %v, want %v", val, pairs[i].Val)
		}
	}
}

func TestDBM_ManyPairs_Delete(t *testing.T) {
	size := 100000
	pairs := generatePairs("key", "val", size)
	_, dbm := setup(t, pairs...)
	defer teardown(t, dbm)

	for i := 1; i <= size; i++ {
		key := sdbm.Datum("key" + strconv.Itoa(i))
		ok, err := dbm.Delete(key)
		if err != nil {
			t.Errorf("Delete() error = %v", err)
		}
		if !ok {
			t.Errorf("Delete() got = %v, want %v", ok, true)
		}

		val, err := dbm.Fetch(key)
		if err != nil {
			t.Errorf("Fetch() error = %v", err)
		}
		if !reflect.DeepEqual(val, sdbm.Nullitem) {
			t.Errorf("Fetch() got = %v, want %v", val, "sdbm.Nullitem")
		}
	}
}
