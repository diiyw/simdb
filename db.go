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
	mu            sync.RWMutex
	sqlite        *sql.DB
	autoIncrement string
	collections   map[string]*Collection
}

type Collection struct {
	mu        sync.RWMutex
	Fields    []string
	Documents map[int64]*Document
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
		d.collections[name] = &Collection{Documents: make(map[int64]*Document, 0)}
		// 获取所有字段
		r, err := db.Query("SELECT * FROM " + name + " LIMIT 0")
		if err != nil {
			db.Close()
			return nil, err
		}
		cols, err := r.Columns()
		if err != nil {
			db.Close()
			return nil, err
		}
		d.collections[name].Fields = cols
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
	d.mu.Lock()
	defer d.mu.Unlock()
	c := d.collections[collection]
	if c == nil {
		// 创建表
		_, err := d.sqlite.Exec("CREATE TABLE IF NOT EXISTS " + collection + " (_ID INTEGER PRIMARY KEY AUTOINCREMENT)")
		if err != nil {
			return nil, err
		}
		// 修改自增值
		_, err = d.sqlite.Exec("UPDATE sqlite_sequence SET seq = " + d.autoIncrement + " WHERE name = '" + collection + "'")
		if err != nil {
			return nil, err
		}
		c = &Collection{Documents: make(map[int64]*Document, 0), Fields: []string{"_ID"}}
		d.collections[collection] = c
	}
	if c.Documents[docId] == nil {
		// 查询数据
		rows, err := d.sqlite.Query("SELECT * FROM " + collection + " WHERE _ID = " + strconv.FormatInt(docId, 10))
		if err != nil {
			return nil, err
		}
		doc := &Document{
			ID:         docId,
			Name:       collection,
			sqlite:     d.sqlite,
			Collection: c,
			Values:     make([]any, len(c.Fields)),
		}
		if !rows.Next() {
			// 没有记录，插入默认记录
			_, err := d.sqlite.Exec("INSERT INTO " + collection + " (_ID) VALUES (" + strconv.FormatInt(docId, 10) + ")")
			if err != nil {
				return nil, err
			}
			a := new(any)
			*a = docId
			doc.Values[0] = a
			c.Documents[docId] = doc
		} else {
			for i := 0; i < len(c.Fields); i++ {
				doc.Values[i] = new(any)
			}
			err = rows.Scan(doc.Values...)
			if err != nil {
				return nil, err
			}
		}
		c.Documents[docId] = doc
	}
	return c.Documents[docId], nil
}

// Close 关闭数据库
func (d *DB) Close() error {
	return d.sqlite.Close()
}
