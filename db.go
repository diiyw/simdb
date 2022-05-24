package binDB

import "os"
import "github.com/kelindar/binary"

type DB struct {
	file *os.File
}

func Open(name string, options Option) (*DB, error) {
	return nil, nil
}

func (db *DB) Add(v any) error {
	binary.Marshal(v)
}

func (db *DB) Remove(id int) error {
	
}
