package storage

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

var (
	ErrInvalidEntry = errors.New("storage/entry: invalid entry")
	ErrInvalidCrc   = errors.New("storage/entry: invalid crc")
)

const (
	//KeySize, ValueSize, ExtraSize, crc32 均为 uint32 类型，各占 4 字节
	//Type 和 Mark 占 2 + 2
	//4 + 4 + 4 + 4 + 2 + 2 = 20
	entryHeaderSize = 20
)

const (
	String uint16 = iota
)

// Entry bitcask思想中，数据操作日志的最小单位，每一个对象，都是对kv键值对对操作日志
// 存储了数据的k、v， 数据类型，操作op类型
// 封装了序列化、反序列化能力
type Entry struct {
	Meta  *Meta
	Type  uint16 //数据类型
	Mark  uint16 //数据op类型
	crc32 uint32 //校验和
}

type Meta struct {
	Key       []byte
	Value     []byte
	Extra     []byte //操作Entry所需的额外信息
	KeySize   uint32
	ValueSize uint32
	ExtraSize uint32
}

func NewEntry(key, value, extra []byte, t, mark uint16) *Entry {
	return &Entry{
		Meta: &Meta{
			Key:       key,
			Value:     value,
			Extra:     extra,
			KeySize:   uint32(len(key)),
			ValueSize: uint32(len(value)),
			ExtraSize: uint32(len(extra)),
		},
		Type: t,
		Mark: mark,
	}
}

func (e *Entry) Size() uint32 {
	return entryHeaderSize + e.Meta.KeySize + e.Meta.ValueSize + e.Meta.ExtraSize
}

// 对e对象进行序列化，返回字节数组
func (e *Entry) Encode() ([]byte, error) {
	if e == nil || e.Meta.KeySize == 0 {
		return nil, ErrInvalidEntry
	}

	ks, vs := e.Meta.KeySize, e.Meta.ValueSize
	es := e.Meta.ExtraSize
	buf := make([]byte, e.Size())

	// uint32类型对象，均为4字节
	binary.BigEndian.PutUint32(buf[4:8], ks)
	binary.BigEndian.PutUint32(buf[8:12], vs)
	binary.BigEndian.PutUint32(buf[12:16], es)
	binary.BigEndian.PutUint16(buf[16:18], e.Type)
	binary.BigEndian.PutUint16(buf[18:20], e.Mark)
	copy(buf[entryHeaderSize:entryHeaderSize+ks], e.Meta.Key)
	copy(buf[entryHeaderSize+ks:entryHeaderSize+ks+vs], e.Meta.Value)
	if es > 0 {
		copy(buf[entryHeaderSize+ks+vs:entryHeaderSize+ks+vs+es], e.Meta.Extra)
	}

	crc := crc32.ChecksumIEEE(e.Meta.Value)
	binary.BigEndian.PutUint32(buf[0:4], crc)

	return buf, nil
}

// Decode 解码字节数组，返回Entry
func Decode(buf []byte) (*Entry, error) {
	ks := binary.BigEndian.Uint32(buf[4:8])
	vs := binary.BigEndian.Uint32(buf[8:12])
	es := binary.BigEndian.Uint32(buf[12:16])
	t := binary.BigEndian.Uint16(buf[16:18])
	mark := binary.BigEndian.Uint16(buf[18:20])
	crc := binary.BigEndian.Uint32(buf[0:4])
	// 疑惑？ 为什么这里只取了header信息，而不加载key、value

	return &Entry{
		Meta: &Meta{
			KeySize:   ks,
			ValueSize: vs,
			ExtraSize: es,
		},
		Type:  t,
		Mark:  mark,
		crc32: crc,
	}, nil
}
