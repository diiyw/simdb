package binDB

import "os"

type DB struct {
	file *os.File
}

func Open(name string) (*DB, error) {
	return nil, nil
}
