package simdb

import (
	"errors"
	"github.com/kelindar/binary"
	"io"
	"os"
	"strconv"
)

type DB struct {
	dir      string
	keys     *Keys
	dataFile string
	buf      []byte
	index    int // current dataFile file index
	opt      *Option
	off      int
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
	db.off = int(fiInfo.Size())
	if db.off == 0 {
		db.keys = &Keys{
			M: make(map[string]*Key, 1024),
			D: make([]Key, 0, 1024),
		}
		return nil
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
	db.dataFile = db.dir + "/" + strconv.Itoa(db.index) + ".dat"
	dataFile, err := os.OpenFile(db.dataFile, os.O_RDONLY|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	dataStat, err := dataFile.Stat()
	if err != nil {
		return err
	}
	defer func() {
		_ = dataFile.Close()
	}()
	// 如果文件大小为0，没有数据不处理
	if dataStat.Size() == 0 {
		db.buf = make([]byte, db.opt.BlockSize, db.opt.BlockSize)
		return nil
	}
	db.buf = make([]byte, dataStat.Size(), dataStat.Size())
	_, err = dataFile.Read(db.buf)
	if err != nil {
		return err
	}
	for i := 0; i < db.index; i++ {
		filename := db.dir + "/" + strconv.FormatUint(uint64(i), 10) + ".dat"
		fi, err := os.OpenFile(filename, os.O_RDONLY, 0755)
		if err != nil {
			return err
		}
		db.files = append(db.files, fi)
	}
	return err
}

func (db *DB) Put(key string, v any) error {
	output, err := binary.Marshal(v)
	if err != nil {
		return err
	}
	size := len(output)
	if db.off+size >= db.opt.BlockSize {
		if err := db.fSync(true); err != nil {
			return err
		}
		db.index++
		filename := db.dir + "/" + strconv.Itoa(db.index) + ".dat"
		dataFile, err := os.OpenFile(filename, os.O_RDONLY|os.O_CREATE, 0755)
		if err != nil {
			return err
		}
		db.off = 0
		db.files = append(db.files, dataFile)
	}
	if oldKey := db.keys.Get(key); oldKey != nil {
		if oldKey.Index == db.index {
			// 立即操作
			if oldKey.Size == size {
				// 如果大小相同，则直接覆盖
				copy(db.buf[oldKey.Offset:], output)
				return nil
			}
		}
		db.keys.Del(oldKey)
	}
	newKey := &Key{
		Index:  db.index,
		Offset: db.off,
		Size:   size,
	}
	copy(db.buf[db.off:], output)
	db.off += size
	db.keys.Set(key, newKey)
	return nil
}

func (db *DB) Get(key string, v any) error {
	keyInfo := db.keys.Get(key)
	if keyInfo == nil {
		return errors.New("keys not found")
	}
	return db.readAt(keyInfo, v)
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
		data = db.buf[key.Offset : key.Offset+key.Size]
	}
	if err := binary.Unmarshal(data, v); err != nil {
		return err
	}
	return nil
}

func (db *DB) fSync(clean bool) error {
	if err := os.WriteFile(db.dataFile, db.buf[:db.off], 0755); err != nil {
		return err
	}
	if clean {
		db.buf = db.buf[:0]
	}
	return nil
}

// Close closes the database.
func (db *DB) Close() error {
	if err := db.Save(); err != nil {
		for _, fi := range db.files {
			_ = fi.Close()
		}
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
