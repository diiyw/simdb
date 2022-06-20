package binDB

import (
	"errors"
	"github.com/kelindar/binary"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
)

type DB struct {
	dir     string
	keys    map[string]*Key
	data    *os.File
	delKeys []*Key
	index   uint64 // current data file
	offset  int64  // the offset of the data file
	opt     *Option
}

func Open(dir string, opt *Option) (*DB, error) {
	if !fileIsExist(dir) {
		err := os.MkdirAll(dir, 0755)
		if err != nil {
			return nil, err
		}
	}
	var db = &DB{
		dir: dir,
	}
	if opt == nil {
		db.opt = DefaultOption
	} else {
		db.opt = opt
	}
	if err := db.loadKeys(); err != nil {
		return nil, err
	}
	if err := db.loadData(); err != nil {
		return nil, err
	}
	return db, nil
}

func (db *DB) loadKeys() error {
	keyFile, err := os.OpenFile(db.dir+"/keys", os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	defer func() {
		_ = keyFile.Close()
	}()
	keyData, err := ioutil.ReadAll(keyFile)
	if err != nil {
		return err
	}
	db.keys, err = InitKeyData(keyData)
	if err != nil {
		return err
	}
	return nil
}

func (db *DB) loadData() error {
	indexFile, err := os.OpenFile(db.dir+"/index", os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	index, err := ioutil.ReadAll(indexFile)
	if len(index) == 0 {
		// create a new index file if not exists
		db.index = 0
		if err := os.WriteFile(db.dir+"/index", []byte("0"), 0755); err != nil {
			return err
		}
	} else {
		db.index, _ = strconv.ParseUint(binary.ToString(&index), 10, 32)
	}
	filename := db.dir + "/" + strconv.FormatUint(db.index, 10) + ".dat"
	db.data, err = os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	db.offset, err = db.data.Seek(0, io.SeekEnd)
	return err
}

func (db *DB) Put(key string, v any) error {
	data, err := binary.Marshal(v)
	if err != nil {
		return err
	}
	if err = db.check(key); err != nil {
		return err
	}
	n, err := db.data.Write(data)
	if err != nil {
		return err
	}
	db.keys[key] = &Key{
		Index:  db.index,
		Offset: db.offset,
		Size:   int64(n),
	}
	db.offset += int64(n)
	return nil
}

func (db *DB) Get(key string, v any) error {
	if db.keys[key] == nil {
		return errors.New("keys not found")
	}
	data := make([]byte, db.keys[key].Size)
	_, err := db.data.ReadAt(data, db.keys[key].Offset)
	if err != nil {
		return err
	}
	if err := binary.Unmarshal(data, v); err != nil {
		return err
	}
	return nil
}

func (db *DB) check(key string) (err error) {
	if db.offset >= db.opt.BlockSize {
		db.index++
		filename := db.dir + "/" + strconv.FormatUint(db.index, 10) + ".dat"
		prev := db.data
		db.data, err = os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0755)
		if err != nil {
			return err
		}
		db.offset = 0
		if db.keys[key] != nil {
			oldKey := db.keys[key]
			if oldKey.Index != db.index {
				go func() {
					// 不是当前分块，需要同步
					if err := db.async(oldKey, prev); err != nil {
						log.Println("async:", err)
					}
				}()
				return
			}
			// 当前并不立即同步，而是等到下一次分块时同步
			db.delKeys = append(db.delKeys, db.keys[key])
		}
	}
	return nil
}

func (db *DB) async(key *Key, fi *os.File) error {
	defer func() {
		_ = fi.Close()
	}()
	stat, err := fi.Stat()
	if err != nil {
		return err
	}
	// left data
	left := make([]byte, key.Offset)
	_, err = fi.ReadAt(left, 0)
	if err != nil {
		return err
	}
	var right []byte
	splitOffset := key.Offset + key.Size
	if splitOffset < stat.Size() {
		ret, _ := fi.Seek(splitOffset, io.SeekStart)
		// 读取到结尾所有数据
		right = make([]byte, stat.Size()-ret)
		_, err = fi.Read(right)
		if err != nil {
			return err
		}
	}
	// 清空文件
	ret, _ := fi.Seek(0, io.SeekStart)
	if err = fi.Truncate(ret); err != nil {
		return err
	}
	// 重新写入数据
	_, err = fi.Write(left)
	if err != nil {
		return err
	}
	if len(right) > 0 {
		_, err = fi.Write(right)
		if err != nil {
			return err
		}
	}
	return nil
}

// Close closes the database.
func (db *DB) Close() error {
	keyData, err := binary.Marshal(db.keys)
	if err != nil {
		return err
	}
	if err := os.WriteFile(db.dir+"/keys", keyData, 0755); err != nil {
		return err
	}
	delData, err := binary.Marshal(db.delKeys)
	if err != nil {
		return err
	}
	if err := os.WriteFile(db.dir+"/del", delData, 0755); err != nil {
		return err
	}
	return db.data.Close()
}

func fileIsExist(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil
}
