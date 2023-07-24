package simdb

import (
	"fmt"
	"os"
	"testing"
	"time"
)

var db *DB

func init() {
	path, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	db, err = Open(path+"/testdata/data/test.db", WithAutoIncrement(1000))
	if err != nil {
		panic(err)
	}
}

func TestDocument(t *testing.T) {
	defer db.Close()
	n := time.Now()
	doc1, _ := db.Document("test", 1000)
	fmt.Println("Fisrt get", time.Since(n).Microseconds(), "us", doc1)
	n = time.Now()
	doc2, err := db.Document("test", 1000)
	fmt.Println("Scendary get", time.Since(n).Nanoseconds(), "ns", doc2)
	if err != nil {
		t.Fatal(err)
	}
}

func TestDocumentPuts(t *testing.T) {
	defer db.Close()
	doc, err := db.Document("test", 1000)
	if err != nil {
		t.Fatal(err)
	}
	err = doc.Puts([]string{"name", "age"}, []any{"test", 18})
	if err != nil {
		t.Fatal(err.Error())
	}
}

func TestDocumentGetKeys(t *testing.T) {
	defer db.Close()
	doc, err := db.Document("test", 1000)
	if err != nil {
		t.Fatal(err)
	}
	r, err := doc.Gets([]string{"name", "age"}...)
	if err != nil {
		t.Fatal(err.Error())
	}

	if r == nil {
		t.Fatal("age error")
	}
	fmt.Println(r)
}

func BenchmarkGet(b *testing.B) {
	for i := 0; i < b.N; i++ {
		doc, err := db.Document("test", 1266)
		if err != nil {
			b.Fatal(err)
		}
		r, err := doc.Gets([]string{"name", "age"}...)
		if err != nil {
			b.Fatal(err.Error())
		}

		if r == nil {
			b.Fatal("age error")
		}
	}
}
