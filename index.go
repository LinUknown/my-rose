package myRose

import (
	"io"
	"myRose/storage"
	"sort"
)

const (
	String uint16 = iota
	Hash
)

const (
	HashHSet uint16 = iota
	HashHDel
)

func (db *MyRose) loadIdxFromFiles() error {

	dbFile := make(ArchivedFiles)
	var fileIds []int
	for k, v := range db.archFileAll {
		dbFile[k] = v
		fileIds = append(fileIds, int(k))
	}

	dbFile[db.activeFileId] = db.activeFile
	//fileIds = append(fileIds, int(db.activeFileId))

	sort.Ints(fileIds)

	// 遍历所有的ActiveFile文件， 取出里面的entry， 将entry回放到hashIndex中
	for _, fileId := range fileIds {
		curActiveFile := dbFile[uint32(fileId)]
		var offset int64 = 0
		for {
			if entry, err := curActiveFile.Read(offset); err == nil {
				db.buildIndex(entry)
				offset += int64(entry.Size())
			} else {
				if err == io.EOF {
					break
				}
				return err
			}
		}
	}
	return nil
}

func (db *MyRose) buildHashIndex(entry *storage.Entry) {
	// todo: 这里有问题， entry.Type不对
	switch entry.Mark {
	case HashHSet:
		db.hashIndex.HSet(string(entry.Meta.Key), string(entry.Meta.Extra), entry.Meta.Value)
	case HashHDel:
		db.hashIndex.HDel(string(entry.Meta.Key), string(entry.Meta.Extra))
	}
}
