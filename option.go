package simdb

type Option struct {
	BlockSize int
}

var DefaultOption = &Option{
	BlockSize: 1024 * 1024 * 100, // 100MB
}
