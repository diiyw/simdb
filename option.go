package simdb

import "strconv"

type Option func(*DB) error

// WithAutoIncrement 自增ID
func WithAutoIncrement(base int64) Option {
	return func(db *DB) error {
		db.autoIncrement = strconv.FormatInt(base, 10)
		return nil
	}
}
