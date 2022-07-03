package simdb

import (
	"os"
	"strconv"
	"testing"
)

var db *DB

func init() {
	db = getDB(&Option{
		BlockSize: 1024 * 1024 * 10, // 100MB
	})
}

func TestDB(t *testing.T) {
	defer func() {
		_ = db.Close()
	}()
	var kv = map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}
	for k, v := range kv {
		if err := db.Put(k, v); err != nil {
			t.Error("put error")
		}
		var result string
		err := db.Get(k, &result)
		if err != nil {
			t.Error(err)
		}
		if result != v {
			t.Error("get error")
		}
	}
}

func TestDB_PutSameKey(t *testing.T) {
	defer func() {
		_ = db.Close()
	}()
	for i := 0; i < 1000000; i++ {
		if err := db.Put("key", "value"); err != nil {
			t.Error(err)
		}
	}
}

func TestDB_PutDiffKeyAndGet(t *testing.T) {
	defer func() {
		_ = db.Close()
	}()
	for i := 0; i < 1000000; i++ {
		if err := db.Put(strconv.Itoa(i), i); err != nil {
			t.Error(err)
		}
	}
	for i := 0; i < 1000000; i++ {
		var v int
		if err := db.Get(strconv.Itoa(i), &v); err != nil || v != i {
			t.Fatal(err)
		}
	}
}

func BenchmarkSameKeyPut(b *testing.B) {
	defer func() {
		_ = db.Close()
	}()
	for i := 0; i < b.N; i++ {
		if err := db.Put("key", i); err != nil {
			b.Error("put error")
		}
	}
}

func BenchmarkSameKeyGet(b *testing.B) {
	defer func() {
		_ = db.Close()
	}()
	var v string
	for i := 0; i < b.N; i++ {
		if err := db.Put("key", i); err != nil {
			b.Error("put error")
		}
	}
	for i := 0; i < b.N; i++ {
		if err := db.Get("key", &v); err != nil {
			b.Error("get error", err)
		}
	}
}

func BenchmarkDiffKeyPut(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if err := db.Put(strconv.Itoa(i), i); err != nil {
			b.Error("put error")
		}
	}
	_ = db.Close()
}

func BenchmarkDiffKeyGet(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if err := db.Put(strconv.Itoa(i), i); err != nil {
			b.Error("put error")
		}
	}
	var v int
	for i := 0; i < b.N; i++ {
		if err := db.Get(strconv.Itoa(i), &v); err != nil || v != i {
			b.Error("get error", err)
		}
	}
	_ = db.Close()
}

func getDB(option *Option) *DB {
	_ = os.RemoveAll("./testdata/testDB")
	db, err := Open("./testdata/testDB", option)
	if err != nil {
		panic(err)
	}
	return db
}
