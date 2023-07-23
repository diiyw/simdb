package simdb

type Option func(*DB) error

// WithAutoIncrement 自增ID
func WithAutoIncrement(base string) Option {
	return func(db *DB) error {
		db.autoIncrement = base
		return nil
	}
}
