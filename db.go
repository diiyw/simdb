package simdb

import (
	"bytes"
	"errors"
	"github.com/kelindar/binary"
	"io"
	"log"
	"os"
	"strconv"
)

type DB struct {
	dir      string
	keys     KeyMap
	dataFile *os.File
	cache    bytes.Buffer
	index    int // current dataFile file index
	opt      *Option
	files    []*os.File
}

func Open(dir string, opt *Option) (*DB, error) {
	if !fileIsExist(dir) {
		err := os.MkdirAll(dir, 0755)
		if err != nil {
			return nil, err
		}
	}
	var db = &DB{
		dir:   dir,
		files: make([]*os.File, 0, 64),
	}
	if opt == nil {
		db.opt = DefaultOption
	} else {
		db.opt = opt
	}
	db.cache.Grow(db.opt.BlockSize)
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
	fiInfo, err := keyFile.Stat()
	if err != nil {
		return err
	}
	keyData := make([]byte, fiInfo.Size())
	_, err = keyFile.Read(keyData)
	if err != nil {
		return err
	}
	if err != nil {
		return err
	}
	db.keys, err = NewKeyMap(keyData)
	if err != nil {
		return err
	}
	return nil
}

func (db *DB) loadData() error {
	index, err := os.OpenFile(db.dir+"/index", os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	defer func() {
		_ = index.Close()
	}()
	indexData, err := io.ReadAll(index)
	if err != nil {
		return err
	}
	if len(indexData) == 0 {
		db.index = 0
	} else {
		if err = binary.Unmarshal(indexData, &db.index); err != nil {
			return err
		}
	}
	filename := db.dir + "/" + strconv.Itoa(db.index) + ".dat"
	db.dataFile, err = os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	_, err = io.Copy(&db.cache, db.dataFile)
	if err != nil {
		return err
	}
	if db.index != 0 {
		for i := 0; i < int(db.index); i++ {
			filename = db.dir + "/" + strconv.FormatUint(uint64(i), 10) + ".dat"
			fi, err := os.OpenFile(filename, os.O_RDWR, 0755)
			if err != nil {
				return err
			}
			db.files = append(db.files, fi)
		}
	}
	return err
}

func (db *DB) Put(key string, v any) error {
	before := db.cache.Len()
	if err := binary.MarshalTo(v, &db.cache); err != nil {
		return err
	}
	size := db.cache.Len() - before
	newKey := &Key{
		Index:  db.index,
		Offset: before,
		Size:   size,
	}
	if db.keys[key] != nil {
		oldKey := db.keys[key]
		if oldKey.Index != db.index {
			go func() {
				// 不是当前分块，需要同步
				if err := db.async(oldKey); err != nil {
					log.Println("async:", err)
				}
			}()
		} else {
			// 同步
			if err := db.sync(oldKey, newKey); err != nil {
				return err
			}
		}
	}
	var err error
	if db.cache.Len()+size >= db.opt.BlockSize {
		if err := db.fSync(true); err != nil {
			return err
		}
		db.index++
		db.files = append(db.files, db.dataFile)
		filename := db.dir + "/" + strconv.Itoa(db.index) + ".dat"
		db.dataFile, err = os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0755)
		if err != nil {
			return err
		}
	}
	db.keys[key] = newKey
	return nil
}

func (db *DB) Get(key string, v any) error {
	if db.keys[key] == nil {
		return errors.New("keys not found")
	}
	return db.readAt(db.keys[key], v)
}

func (db *DB) readAt(key *Key, v any) error {
	data := make([]byte, key.Size)
	if key.Index != db.index {
		fi := db.files[key.Index]
		_, err := fi.ReadAt(data, int64(key.Offset))
		if err != nil {
			return err
		}
	} else {
		data = db.cache.Bytes()[key.Offset : key.Offset+key.Size]
	}
	if err := binary.Unmarshal(data, v); err != nil {
		return err
	}
	return nil
}

func (db *DB) fSync(clean bool) error {
	if err := db.dataFile.Truncate(0); err != nil {
		return err
	}
	if _, err := db.cache.WriteTo(db.dataFile); err != nil {
		return err
	}
	if clean {
		db.cache.Reset()
	}
	return nil
}

func (db *DB) sync(key *Key, newKey *Key) error {
	data := db.cache.Bytes()
	db.cache.Truncate(0)
	n, err := db.cache.Write(data[:key.Offset])
	if err != nil {
		return err
	}
	newKey.Offset = n
	n, err = db.cache.Write(data[key.Offset : key.Offset+key.Size])
	if err != nil {
		return err
	}
	return nil
}

func (db *DB) async(key *Key) error {
	fi := db.files[key.Index]
	stat, err := fi.Stat()
	if err != nil {
		return err
	}
	// left dataFile
	left := make([]byte, key.Offset)
	_, err = fi.ReadAt(left, 0)
	if err != nil {
		return err
	}
	var right []byte
	splitOffset := int64(key.Offset + key.Size)
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
	if err := db.Save(); err != nil {
		for _, fi := range db.files {
			_ = fi.Close()
		}
		return db.dataFile.Close()
	}
	return nil
}

// Save saves the database.
func (db *DB) Save() error {
	keyData, err := db.keys.Marshal()
	if err != nil {
		return err
	}
	if err := os.WriteFile(db.dir+"/keys", keyData, 0755); err != nil {
		return err
	}
	indexData, err := binary.Marshal(db.index)
	if err != nil {
		return err
	}
	if err := os.WriteFile(db.dir+"/index", indexData, 0755); err != nil {
		return err
	}
	if err := db.fSync(false); err != nil {
		return err
	}
	return nil
}

func fileIsExist(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil
}
