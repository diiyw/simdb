package binDB

import (
	"os"
	"testing"
)

func TestDB(t *testing.T) {
	_ = os.RemoveAll("./testdata/testDB")
	db, err := Open("./testdata/testDB", nil)
	if err != nil {
		t.Error(err)
	}
	defer func() {
		_ = db.Close()
	}()
	if err := db.Put("key", "value"); err != nil {
		t.Error("put error")
	}
	var result string
	err = db.Get("key", &result)
	if err != nil {
		t.Error(err)
	}
	if result != "value" {
		t.Error("get error")
	}
	if err := db.Put("key1", "value1"); err != nil {
		t.Error("put error")
	}
	err = db.Get("key1", &result)
	if err != nil {
		t.Error(err)
	}
	if result != "value1" {
		t.Error("get error")
	}
}

func BenchmarkPut(b *testing.B) {
	_ = os.RemoveAll("./testdata/testDB")
	db, err := Open("./testdata/testDB", nil)
	if err != nil {
		b.Error(err)
	}
	defer func() {
		_ = db.Close()
	}()
	for i := 0; i < b.N; i++ {
		if err := db.Put("key", "value"); err != nil {
			b.Error("put error")
		}
	}
}

func BenchmarkGet(b *testing.B) {
	db, err := Open("./testdata/testDB", nil)
	if err != nil {
		b.Error(err)
	}
	defer func() {
		_ = db.Close()
	}()
	var v string
	for i := 0; i < b.N; i++ {
		if err := db.Get("key", &v); err != nil {
			b.Error("get error", err)
		}
	}
}

func TestDB_Put(t *testing.T) {
	db := getDB(&Option{
		BlockSize: 1024 * 1024,
	})
	defer func() {
		_ = db.Close()
	}()
	for i := 0; i < 1000000; i++ {
		if err := db.Put("key", "value"); err != nil {
			t.Error(err)
		}
	}
}

func getDB(option *Option) *DB {
	_ = os.RemoveAll("./testdata/testDB")
	db, err := Open("./testdata/testDB", option)
	if err != nil {
		panic(err)
	}
	return db
}
