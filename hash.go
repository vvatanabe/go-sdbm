package sdbm

// Hash polynomial conversion ignoring overflows
// [this seems to work remarkably well, in fact better
// than the ndbm hash function. Replace at your own risk]
// use: 65599	nice.
// 65587   even better.
func Hash(data []byte) int64 {
	var hash uint64 = 0
	for i := 0; i < len(data); i++ {
		hash = uint64(data[i]) + 65599*hash
	}
	return int64(hash)
}
