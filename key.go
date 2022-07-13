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

type Keys struct {
	D []Key
	M map[string]*Key
}

func NewKeyMap(data []byte) (*Keys, error) {
	var keyMap = &Keys{}
	if err := binary.Unmarshal(data, &keyMap); err != nil && err.Error() != "EOF" {
		return nil, err
	}
	return keyMap, nil
}

func (k *Keys) Marshal() ([]byte, error) {
	buf := bytes.Buffer{}
	buf.Grow(len(k.M)*12 + len(k.D)*12)
	err := binary.MarshalTo(k, &buf)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (k *Keys) Set(key string, ky *Key) {
	k.M[key] = ky
}

func (k *Keys) Get(key string) *Key {
	return k.M[key]
}

func (k *Keys) Del(key *Key) {
	k.D = append(k.D, *key)
}
