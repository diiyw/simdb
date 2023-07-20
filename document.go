package simdb

import (
	"database/sql"
	"strconv"
	"strings"

	_ "modernc.org/sqlite"
)

type Document struct {
	sqlite     *sql.DB
	Name       string
	ID         int64
	Collection *Collection
}

// Puts 设置文档
func (d *Document) Puts(key []string, value []any) error {
	// 求Fields和Key的差集
	var diff = make(map[string]any)
	for i := 0; i < len(key); i++ {
		found := false
		for j := 0; j < len(d.Collection.Fields); j++ {
			if key[i] == d.Collection.Fields[j] {
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
		d.Collection.Fields = append(d.Collection.Fields, field)
		d.Collection.Values = append(d.Collection.Values, new(any))
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
		*d.Collection.Values[i].(*any) = value[i]
	}
	return nil
}

// Gets 获取文档键值
func (d *Document) Gets(key ...string) ([]any, error) {
	// 内存中查询
	var values = make([]any, 0)
	for i := 0; i < len(key); i++ {
		for j := 0; j < len(d.Collection.Fields); j++ {
			if key[i] == d.Collection.Fields[j] {
				values = append(values, *d.Collection.Values[j].(*any))
			}
		}
	}
	return values, nil
}

// Put 设置文档
func (d *Document) Put(key string, value any) error {
	found := false
	for j := 0; j < len(d.Collection.Fields); j++ {
		if key == d.Collection.Fields[j] {
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
		// 添加到Fields
		d.Collection.Fields = append(d.Collection.Fields, key)
		// 设置内存中的值
		var a = new(any)
		*a = value
		d.Collection.Values = append(d.Collection.Values, a)
	}
	// 更新数据库
	var sqlStmt = "UPDATE " + d.Name + " SET " + key + " = ? WHERE _ID = " + strconv.FormatInt(d.ID, 10)
	_, err := d.sqlite.Exec(sqlStmt, value)
	if err != nil {
		return err
	}
	return nil
}

// Get 获取文档键值
func (d *Document) Get(key string) (any, error) {
	// 内存中查询
	for j := 0; j < len(d.Collection.Fields); j++ {
		if key == d.Collection.Fields[j] {
			return *d.Collection.Values[j].(*any), nil
		}
	}
	return nil, nil
}

// Delete 删除文档
func (d *Document) Delete(key string) error {
	// 删除字段
	_, err := d.sqlite.Exec("ALTER TABLE " + d.Name + " DROP COLUMN " + key)
	if err != nil {
		return err
	}
	// 删除内存中的key和value
	for i := 0; i < len(d.Collection.Fields); i++ {
		if key == d.Collection.Fields[i] {
			d.Collection.Fields = append(d.Collection.Fields[:i], d.Collection.Fields[i+1:]...)
			d.Collection.Values = append(d.Collection.Values[:i], d.Collection.Values[i+1:]...)
		}
	}
	return nil
}
