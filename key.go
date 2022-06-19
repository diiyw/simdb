package binDB

import (
	"github.com/kelindar/binary"
)

type Key struct {
	Index  uint64
	Offset int64
	Size   int64
}

func InitKeyData(data []byte) (map[string]*Key, error) {
	var keyMap = map[string]*Key{}
	if err := binary.Unmarshal(data, &keyMap); err != nil && err.Error() != "EOF" {
		return nil, err
	}
	return keyMap, nil
}
