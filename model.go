package binDB

//Model 基础的数据页 4*4=16 byte
type Model interface {
	Serialize() []byte        // 序列化
	Unserialize([]byte) error // 反序列化
}

func Insert(v Model) error {
	return nil
}

func Delete(v Model) error {
	return nil
}

func Update(v Model) error {
	return nil
}

func Select(v Model) error {
	return nil
}
