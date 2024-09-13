# go-sdbm

 [![Go](https://github.com/vvatanabe/go-sdbm/actions/workflows/go.yml/badge.svg)](https://github.com/vvatanabe/go-sdbm/actions/workflows/go.yml) [![Go Report Card](https://goreportcard.com/badge/github.com/vvatanabe/go-sdbm)](https://goreportcard.com/report/github.com/vvatanabe/go-sdbm) [![Go Reference](https://pkg.go.dev/badge/github.com/vvatanabe/go-sdbm.svg)](https://pkg.go.dev/github.com/vvatanabe/go-sdbm)

go-sdbm is a pure Go implementation of the classic SDBM key-value store.

## About SDBM

SDBM (Substitute DBM) is a public domain clone of the ndbm library, originally implemented in C by Ozan Yigit. It provides a simple and efficient way to store and retrieve key-value pairs using dynamic hashing, based on P.-A. Larson’s 1978 algorithm known as “Dynamic Hashing”. SDBM is recognized for being practical, easy to understand, and compatible with ndbm, while offering improvements such as reduced file sizes and faster database creation.

## Motivation

Reimplementing a simple and classic key-value store like SDBM in the Go programming language offers the following objectives and learning opportunities:

- **Data Structures and Algorithms**: Gain a deep understanding of the internal workings of classic key-value stores (KVS). By learning concepts such as dynamic hashing methods, page management, and bitmaps, you can master efficient data storage and retrieval techniques.
- **Hash Functions and Hashing Techniques**: Learn about selecting and implementing effective hash functions and collision resolution methods. This is crucial for deepening your foundational understanding of databases and cache systems.
- **File I/O and Data Persistence in Go**: Acquire knowledge on how to directly manipulate the file system and persist data. This enhances your understanding of Go's `os` package and binary data reading and writing.

Through this project, you can learn how to implement a simple and efficient KVS in Go, thereby deepening your understanding of the fundamental principles of system programming and databases.

## Installation

To install the `sdbm` package, run:

```sh
go get github.com/vvatanabe/go-sdbm
```

## Documentation

Detailed documentation is available on [pkg.go.dev](https://pkg.go.dev/github.com/vvatanabe/go-sdbm).

## Usage

Below is an example demonstrating basic usage of the `sdbm` package:

```go
package main

import (
	"fmt"
	"os"

	"github.com/vvatanabe/go-sdbm"
)

func main() {
	// Open the database file (creates it if it doesn't exist)
	db, err := sdbm.Open("mydatabase", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Create key and value data
	key := sdbm.Datum("mykey")
	value := sdbm.Datum("myvalue")

	// Store the key-value pair in the database
	success, err := db.Store(key, value, sdbm.StoreREPLACE)
	if err != nil {
		panic(err)
	}
	if success {
		fmt.Println("Key-value pair stored successfully.")
	}

	// Fetch the value associated with the key
	fetchedValue, err := db.Fetch(key)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Fetched Value: %s\n", fetchedValue)

	// Delete the key-value pair from the database
	deleted, err := db.Delete(key)
	if err != nil {
		panic(err)
	}
	if deleted {
		fmt.Println("Key-value pair deleted successfully.")
	}

	// First keys in the database
	key, err = db.FirstKey()
	if err != nil {
		panic(err)
	}
	fmt.Printf("FirstKey: %s\n", key)

	// Next keys in the database
	key, err = db.NextKey()
	if err != nil {
		panic(err)
	}
	fmt.Printf("NextKey: %s\n", key)
}
```

## Acknowledgments

- Original SDBM implementation by [ozan (oz) s. yigit](https://github.com/plan9).
- Inspiration from the C implementation of [SDBM](http://www.cse.yorku.ca/~oz/sdbm.bun).

## Authors

- [vvatanabe](https://github.com/vvatanabe)

## License

This project is licensed under the [CC0 1.0 Universal](LICENSE).
