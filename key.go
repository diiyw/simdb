package simdb

import (
	"bytes"
	"github.com/kelindar/binary"
)

type Key struct {
	Index  int
	Offset int
	Size   int
}

type KeyMap map[string]*Key

func NewKeyMap(data []byte) (KeyMap, error) {
	var keyMap = map[string]*Key{}
	if err := binary.Unmarshal(data, &keyMap); err != nil && err.Error() != "EOF" {
		return nil, err
	}
	return keyMap, nil
}

func (k KeyMap) Marshal() ([]byte, error) {
	buf := bytes.Buffer{}
	buf.Grow(len(k) * 24)
	err := binary.MarshalTo(k, &buf)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
