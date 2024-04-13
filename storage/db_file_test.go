package storage

import (
	"hash/crc32"
	"log"
	"testing"
)

func TestNewDBFile(t *testing.T) {
	dirFilePath := "/tmp/myRose/test/"
	df, err := NewDBFile(dirFilePath, 1, FileIO)
	if err != nil {
		log.Fatal(err)
	}

	key, val := []byte("test_key"), []byte("test_val")
	extra := []byte("test_extra")
	e := NewEntry(key, val, extra, String, 0)
	e.Meta.KeySize = uint32(len(e.Meta.Key))
	e.Meta.ValueSize = uint32(len(e.Meta.Value))

	err = df.Write(e)
	if err != nil {
		log.Fatal(err)
	}

	e2, err := df.Read(0)
	if err != nil {
		log.Fatal(err)
	}
	checkCrc := crc32.ChecksumIEEE(e.Meta.Value)

	if checkCrc != e2.crc32 {
		log.Fatal("value fail fail")
	}
	t.Logf("key:%s, value=%s", string(e.Meta.Key), string(e.Meta.Value))
	t.Logf("key:%s, value=%s", string(e2.Meta.Key), string(e2.Meta.Value))
	defer func() {
		err = df.Close(true)
	}()

	if err != nil {
		t.Error("写入数据错误 : ", err)
	}
}

func TestDBFile_Write(t *testing.T) {
	entry2 := &Entry{
		Meta: &Meta{
			Key:   []byte("test_key_002"),
			Value: []byte("test_val_002"),
		},
	}

	entry2.Meta.KeySize = uint32(len(entry2.Meta.Key))
	entry2.Meta.ValueSize = uint32(len(entry2.Meta.Value))

	entry3 := &Entry{
		Meta: &Meta{
			Key:   []byte("test_key_003"),
			Value: []byte("test_val_003"),
		},
	}

	entry3.Meta.KeySize = uint32(len(entry3.Meta.Key))
	entry3.Meta.ValueSize = uint32(len(entry3.Meta.Value))

	dirFilePath := "/tmp/myRose/test/"
	df, err := NewDBFile(dirFilePath, 2, FileIO)
	if err != nil {
		log.Fatal(err)
	}

	df.Write(entry2)
	df.Write(entry3)

	defer func() {
		err = df.Close(true)
	}()

	if err != nil {
		t.Error("写入数据错误 : ", err)
	}
}

func TestDBFile_Read(t *testing.T) {
	dirFilePath := "/tmp/myRose/test/"
	df, err := NewDBFile(dirFilePath, 2, FileIO)
	if err != nil {
		log.Fatal(err)
	}

	e2, err := df.Read(0)
	if err != nil {
		log.Fatal(err)
	}

	e3, err := df.Read(int64(e2.Size()))
	if err != nil {
		log.Fatal(err)
	}
	t.Logf("key:%s, value=%s", string(e3.Meta.Key), string(e3.Meta.Value))
	t.Logf("key:%s, value=%s", string(e2.Meta.Key), string(e2.Meta.Value))

}

func TestBuild(t *testing.T) {
	dirFilePath := "/tmp/myRose/test/"
	mp, topFileId, err := Build(dirFilePath)
	if err != nil {
		log.Fatal(err)
	}
	for k, v := range mp {
		t.Log(k)

		firstEntry, err := v.Read(0)
		if err != nil {
			log.Fatal(err)
		}
		t.Log(firstEntry.Meta.Value)
	}

	t.Log(topFileId)

}
