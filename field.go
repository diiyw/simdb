package simdb

type Fields interface {
	GetKeys() []string
	GetValues() []any
}
