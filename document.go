package simdb

import (
	"database/sql"
	"strconv"
	"strings"

	_ "modernc.org/sqlite"
)

type Document struct {
	sqlite      *sql.DB
	Name        string
	ID          int64
	collection  *Collection
	preparation *preparation
}

type preparation struct {
	diff   map[string]int
	keys   []string
	values []any
}

// Prepares 准备文档
func (d *Document) Prepares(key []string, value []any) *Document {
	d.collection.mu.Lock()
	defer d.collection.mu.Unlock()
	// 求Fields和Key的差集
	var diff = make(map[string]int)
	for i := 0; i < len(key); i++ {
		found := false
		for j := 0; j < len(d.collection.Fields); j++ {
			if key[i] == d.collection.Fields[j] {
				found = true
			}
		}
		if !found {
			diff[key[i]] = len(d.collection.Values) + 1
		}
	}
	if d.preparation == nil {
		d.preparation = &preparation{diff: diff, keys: key, values: value}
	} else {
		for field, index := range diff {
			d.preparation.diff[field] = index
		}
		d.preparation.keys = append(d.preparation.keys, key...)
		d.preparation.values = append(d.preparation.values, value...)
	}
	return d
}

// Prepare 准备文档
func (d *Document) Prepare(key string, value any) *Document {
	found := false
	for j := 0; j < len(d.collection.Fields); j++ {
		if key == d.collection.Fields[j] {
			found = true
		}
	}
	if d.preparation == nil {
		d.preparation = &preparation{diff: make(map[string]int), keys: make([]string, 0), values: make([]any, 0)}
	}
	if !found {
		d.preparation.diff[key] = len(d.preparation.values) + 1
	}
	d.preparation.keys = append(d.preparation.keys, key)
	d.preparation.values = append(d.preparation.values, value)
	return d
}

// Save 保存文档
func (d *Document) Save() error {
	d.collection.mu.Lock()
	defer d.collection.mu.Unlock()
	if d.preparation == nil {
		return nil
	}
	// 直接保存
	if len(d.preparation.diff) != 0 {
		// 添加新增的字段
		for field, index := range d.preparation.diff {
			switch d.preparation.values[index].(type) {
			case int:
				_, err := d.sqlite.Exec("ALTER TABLE " + d.Name + " ADD COLUMN " + field + " INTEGER")
				if err != nil {
					return err
				}
			default:
				_, err := d.sqlite.Exec("ALTER TABLE " + d.Name + " ADD COLUMN " + field + " TEXT")
				if err != nil {
					return err
				}
			}
		}
	}
	// 更新数据库
	var set strings.Builder
	for i := 0; i < len(d.preparation.keys); i++ {
		set.WriteString(d.preparation.keys[i])
		set.WriteString(" = ?,")
	}
	var sqlStmt = "UPDATE " + d.Name + " SET " + strings.TrimRight(set.String(), ",") + " WHERE _ID = " + strconv.FormatInt(d.ID, 10)
	_, err := d.sqlite.Exec(sqlStmt, d.preparation.values...)
	if err != nil {
		return err
	}
	// 更新内存中的值
	for i := 0; i < len(d.preparation.keys); i++ {
		*d.collection.Values[d.preparation.diff[d.preparation.keys[i]]-1].(*any) = d.preparation.values[i]
	}
	d.preparation = nil
	return nil
}

// Puts 设置文档
func (d *Document) Puts(key []string, value []any) error {
	d.collection.mu.Lock()
	defer d.collection.mu.Unlock()
	// 求Fields和Key的差集
	var diff = make(map[string]any)
	for i := 0; i < len(key); i++ {
		found := false
		for j := 0; j < len(d.collection.Fields); j++ {
			if key[i] == d.collection.Fields[j] {
				found = true
			}
		}
		if !found {
			diff[key[i]] = value[i]
		}
	}
	// 不存在的字段，添加到数据库
	for field, v := range diff {
		switch v.(type) {
		case int:
			_, err := d.sqlite.Exec("ALTER TABLE " + d.Name + " ADD COLUMN " + field + " INTEGER")
			if err != nil {
				return err
			}
		default:
			_, err := d.sqlite.Exec("ALTER TABLE " + d.Name + " ADD COLUMN " + field + " TEXT")
			if err != nil {
				return err
			}
		}
		// 添加到Fields
		d.collection.Fields = append(d.collection.Fields, field)
		d.collection.Values = append(d.collection.Values, new(any))
	}
	// 更新数据库
	var set strings.Builder
	for i := 0; i < len(key); i++ {
		set.WriteString(key[i])
		set.WriteString(" = ?,")
	}
	var sqlStmt = "UPDATE " + d.Name + " SET " + strings.TrimRight(set.String(), ",") + " WHERE _ID = " + strconv.FormatInt(d.ID, 10)
	_, err := d.sqlite.Exec(sqlStmt, value...)
	if err != nil {
		return err
	}
	// 更新内存中的值
	for i := 0; i < len(key); i++ {
		*d.collection.Values[i].(*any) = value[i]
	}
	return nil
}

// Gets 获取文档键值
func (d *Document) Gets(key ...string) ([]any, error) {
	d.collection.mu.Lock()
	defer d.collection.mu.Unlock()
	// 内存中查询
	var values = make([]any, 0)
	for i := 0; i < len(key); i++ {
		for j := 0; j < len(d.collection.Fields); j++ {
			if key[i] == d.collection.Fields[j] {
				values = append(values, *d.collection.Values[j].(*any))
			}
		}
	}
	return values, nil
}

// Put 设置文档
func (d *Document) Put(key string, value any) error {
	d.collection.mu.Lock()
	defer d.collection.mu.Unlock()
	found := false
	for j := 0; j < len(d.collection.Fields); j++ {
		if key == d.collection.Fields[j] {
			found = true
		}
	}
	if !found {
		// 不存在的字段，添加到数据库
		switch value.(type) {
		case int:
			_, err := d.sqlite.Exec("ALTER TABLE " + d.Name + " ADD COLUMN " + key + " INTEGER")
			if err != nil {
				return err
			}
		default:
			_, err := d.sqlite.Exec("ALTER TABLE " + d.Name + " ADD COLUMN " + key + " TEXT")
			if err != nil {
				return err
			}
		}
	}
	// 更新数据库
	var sqlStmt = "UPDATE " + d.Name + " SET " + key + " = ? WHERE _ID = " + strconv.FormatInt(d.ID, 10)
	_, err := d.sqlite.Exec(sqlStmt, value)
	if err != nil {
		return err
	}
	// 添加到Fields
	d.collection.Fields = append(d.collection.Fields, key)
	// 设置内存中的值
	var a = new(any)
	*a = value
	d.collection.Values = append(d.collection.Values, a)
	return nil
}

// Get 获取文档键值
func (d *Document) Get(key string) (any, error) {
	// 内存中查询
	for j := 0; j < len(d.collection.Fields); j++ {
		if key == d.collection.Fields[j] {
			return *d.collection.Values[j].(*any), nil
		}
	}
	return nil, nil
}

// Delete 删除文档
func (d *Document) Delete() error {
	d.collection.selected = false
	d.collection.mu.Lock()
	defer d.collection.mu.Unlock()
	if _, err := d.sqlite.Exec("DELETE FROM " + d.Name + " WHERE _ID = " + strconv.FormatInt(d.ID, 10)); err != nil {
		return err
	}
	return nil
}

// DeleteKey 删除文档Key
func (d *Document) DeleteKey(key string) error {
	d.collection.mu.Lock()
	defer d.collection.mu.Unlock()
	// 删除字段
	_, err := d.sqlite.Exec("ALTER TABLE " + d.Name + " DROP COLUMN " + key)
	if err != nil {
		return err
	}
	// 删除内存中的key和value
	for i := 0; i < len(d.collection.Fields); i++ {
		if key == d.collection.Fields[i] {
			d.collection.Fields = append(d.collection.Fields[:i], d.collection.Fields[i+1:]...)
			d.collection.Values = append(d.collection.Values[:i], d.collection.Values[i+1:]...)
		}
	}
	return nil
}
