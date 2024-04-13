package myRose

import (
	"myRose/index/hash"
	"myRose/storage"
	"testing"
)

func TestMyRose_HSet(t *testing.T) {
	dirFilePath := "/tmp/myRose/test/"

	allActiveFileMap, topFileId, err := storage.Build(dirFilePath)
	if err != nil {
		t.Error(err)
		return
	}
	topActiveFile, err := storage.NewDBFile(dirFilePath, topFileId, storage.FileIO)
	if err != nil {
		t.Error(err)
		return
	}

	// 下一步把这两个数据结构持久化，就能实现完整的数据库的持久化了
	hashIndex := hash.BuildHashIndex()
	meta := &storage.DBMeta{
		ActiveWriteOff: 0,
	}

	db := &MyRose{
		activeFile:   topActiveFile,
		activeFileId: topFileId,
		archFileAll:  allActiveFileMap,
		hashIndex:    hashIndex,
		meta:         meta,
	}

	res := db.HGet([]byte("my_hash"), []byte("a"))
	t.Log(string(res))
	set, err := db.HSet([]byte("my_hash"), []byte("a"), []byte("hash_data_001"))
	if err != nil {
		return
	}
	t.Log(set)
	res = db.HGet([]byte("my_hash"), []byte("a"))
	t.Log(string(res))

}
