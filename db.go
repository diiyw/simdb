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
	dir      string
	keys     map[string]*Key
	dataFile *os.File
	cache    []byte
	index    uint64 // current dataFile file index
	offset   int64  // the offset of the dataFile file
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
		files: make([]*os.File, 0, 1024),
	}
	if opt == nil {
		db.opt = DefaultOption
	} else {
		db.opt = opt
	}
	db.cache = make([]byte, 0, db.opt.BlockSize)
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
	filename := db.dir + "/" + strconv.FormatUint(db.index, 10) + ".dat"
	db.dataFile, err = os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	db.cache, err = ioutil.ReadAll(db.dataFile)
	if err != nil {
		return err
	}
	db.offset, err = db.dataFile.Seek(0, io.SeekEnd)
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
	data, err := binary.Marshal(v)
	if err != nil {
		return err
	}
	if err = db.check(key); err != nil {
		return err
	}
	size := int64(len(data))
	db.cache = append(db.cache, data...)
	db.keys[key] = &Key{
		Index:  db.index,
		Offset: db.offset,
		Size:   size,
	}
	db.offset += size
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
		_, err := fi.ReadAt(data, key.Offset)
		if err != nil {
			return err
		}
	} else {
		data = db.cache[key.Offset : key.Offset+key.Size]
	}
	if err := binary.Unmarshal(data, v); err != nil {
		return err
	}
	return nil
}

func (db *DB) check(key string) (err error) {
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
			db.sync(oldKey)
		}
	}
	if db.offset >= db.opt.BlockSize {
		if err := db.FSync(); err != nil {
			return err
		}
		db.index++
		db.files = append(db.files, db.dataFile)
		filename := db.dir + "/" + strconv.FormatUint(db.index, 10) + ".dat"
		db.dataFile, err = os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0755)
		if err != nil {
			return err
		}
		db.offset = 0
	}
	return nil
}

func (db *DB) FSync() error {
	if _, err := db.dataFile.Write(db.cache); err != nil {
		return err
	}
	db.cache = make([]byte, 0, db.opt.BlockSize)
	return nil
}

func (db *DB) sync(key *Key) {
	db.cache = append(db.cache[:key.Offset], db.cache[key.Offset+key.Size:]...)
	db.offset -= key.Size
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
	indexData, err := binary.Marshal(db.index)
	if err != nil {
		return err
	}
	if err := os.WriteFile(db.dir+"/index", indexData, 0755); err != nil {
		return err
	}
	if err := db.FSync(); err != nil {
		return err
	}
	for _, fi := range db.files {
		_ = fi.Close()
	}
	return db.dataFile.Close()
}

func fileIsExist(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil
}
