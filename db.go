package binDB

import "os"

type DB struct {
	file *os.File
}

func Open(name string, options Option) (*DB, error) {
	return nil, nil
}
