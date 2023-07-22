package simdb

import (
	"database/sql"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	_ "modernc.org/sqlite"
)

type DB struct {
	sqlite      *sql.DB
	collections map[string]*Collection
}

type Collection struct {
	mu       sync.RWMutex
	selected bool
	Fields   []string
	Values   []any
}

// Open 打开数据库
func Open(dbFile string, opts ...Option) (*DB, error) {
	dir := filepath.Dir(dbFile)
	d := &DB{collections: make(map[string]*Collection, 0)}
	if _, err := os.Stat(dbFile); os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0755)
		if err != nil {
			return nil, err
		}
	}
	db, err := sql.Open("sqlite", dbFile)
	if err != nil {
		return nil, err
	}
	// 获取所有表格
	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='table'")
	if err != nil {
		db.Close()
		return nil, err
	}
	for rows.Next() {
		var name string
		rows.Scan(&name)
		d.collections[name] = &Collection{Fields: make([]string, 0), Values: make([]any, 0)}
	}
	d.sqlite = db
	for _, opt := range opts {
		err = opt(d)
		if err != nil {
			return nil, err
		}
	}
	return d, nil
}

// Document 获取文档
func (d *DB) Document(collection string, docId int64) (*Document, error) {
	if d.collections[collection] == nil {
		// 创建表
		_, err := d.sqlite.Exec("CREATE TABLE IF NOT EXISTS " + collection + " (_ID INTEGER PRIMARY KEY AUTOINCREMENT)")
		if err != nil {
			return nil, err
		}
		// 修改自增值
		_, err = d.sqlite.Exec("UPDATE sqlite_sequence SET seq = 1000 WHERE name = '" + collection + "'")
		if err != nil {
			return nil, err
		}
		// 插入默认记录
		_, err = d.sqlite.Exec("INSERT INTO " + collection + " (_ID) VALUES (1000)")
		if err != nil {
			return nil, err
		}
	}
	if !d.collections[collection].selected {
		// 查询记录
		r, err := d.sqlite.Query("SELECT * FROM " + collection + " WHERE _ID = " + strconv.FormatInt(docId, 10))
		if err != nil {
			return nil, err
		}
		columns, err := r.Columns()
		if err != nil {
			r.Close()
			return nil, err
		}
		values := make([]interface{}, len(columns))
		for i := range values {
			values[i] = new(any)
		}
		c := &Collection{Fields: columns, Values: values}
		if r.Next() {
			err = r.Scan(c.Values...)
			if err != nil {
				r.Close()
				return nil, err
			}
		}
		r.Close()
		c.selected = true
		d.collections[collection] = c
	}
	return &Document{ID: docId, Name: collection, sqlite: d.sqlite, Collection: d.collections[collection]}, nil
}

// Close 关闭数据库
func (d *DB) Close() error {
	return d.sqlite.Close()
}
