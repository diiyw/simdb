package binDB

type Option struct {
	BlockSize int64
}

var DefaultOption = &Option{
	BlockSize: 1024 * 1024 * 100, // 100MB
}
