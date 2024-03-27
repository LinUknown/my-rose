package storage

import (
	"hash/crc32"
	"log"
	"os"
	"testing"
)

func TestNewEntry(t *testing.T) {
	key, val := []byte("test_key"), []byte("test_val")
	extra := []byte("test_extra")
	e := NewEntry(key, val, extra, String, 0)
	t.Logf("%+v", e)
}

func TestEncode(t *testing.T) {
	key, val := []byte("test_key"), []byte("test_val")
	extra := []byte("test_extra")
	e := NewEntry(key, val, extra, String, 0)
	e.Meta.KeySize = uint32(len(e.Meta.Key))
	e.Meta.ValueSize = uint32(len(e.Meta.Value))

	encVal, err := e.Encode()
	if err != nil {
		log.Fatal(err)
	}
	if encVal != nil {
		file, _ := os.OpenFile("/tmp/myRose/test/test.dat", os.O_CREATE|os.O_WRONLY, 0644)
		file.Write(encVal)
	}
}

func TestDecode(t *testing.T) {
	file, err := os.OpenFile("/tmp/myRose/test/test.dat", os.O_RDONLY, os.ModePerm)
	if err != nil {
		log.Fatal(err)
	}
	buf := make([]byte, entryHeaderSize)
	offset := int64(0)

	_, err = file.ReadAt(buf, offset)
	if err != nil {
		log.Fatal(err)
	}

	e, _ := Decode(buf)

	offset += entryHeaderSize
	if e.Meta.KeySize > 0 {
		key := make([]byte, e.Meta.KeySize)
		file.ReadAt(key, offset)
		e.Meta.Key = key
	}

	offset += int64(e.Meta.KeySize)
	if e.Meta.ValueSize > 0 {
		value := make([]byte, e.Meta.ValueSize)
		file.ReadAt(value, offset)
		e.Meta.Value = value
	}

	checkCrc := crc32.ChecksumIEEE(e.Meta.Value)
	if checkCrc != e.crc32 {
		log.Fatal("crc check fail")
	}

	t.Logf("key:%s, value=%s", string(e.Meta.Key), string(e.Meta.Value))

}
