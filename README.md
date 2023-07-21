# SimDB
Simple document Key-Value database written in go, but SQLite backend.

## Getting Started

```go
package main

import (
    "fmt"
    "log"

    "github.com/diiyw/simdb"
)

func main() {
   path, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	db, err := Open(path + "/testdata/data/test.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
    err = db.Document("users",1000).Put("name", "John Doe")
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println(db.Document("users",1000).Get("name") == "John Doe")
}

```