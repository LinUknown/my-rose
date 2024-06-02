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

// loadIdxFromFiles 从文件中加载索引
func (db *MyRose) loadIdxFromFiles() error {

	// 取出所有文件
	dbFile := make(ArchivedFiles)
	var fileIds []int
	for k, v := range db.archFileAll {
		dbFile[k] = v
		fileIds = append(fileIds, int(k))
	}

	// 为什么这里要单独处理activeFile呢？难道美方在archFileAll里面？
	dbFile[db.activeFileId] = db.activeFile
	//fileIds = append(fileIds, int(db.activeFileId))

	sort.Ints(fileIds)

	// 遍历所有的ActiveFile文件， 取出里面的entry， 将entry回放到hashIndex中
	for _, fileId := range fileIds {
		curActiveFile := dbFile[uint32(fileId)]
		var offset int64 = 0
		for {
			if entry, err := curActiveFile.Read(offset); err == nil {
				db.buildIndexByEntry(entry)
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

func (db *MyRose) buildHashIndexByEntry(entry *storage.Entry) {
	// 构建entry type为hash的数据结构索引
	// 根据entry的Mark，回放具体的数据操作，更新hashIndex
	switch entry.Mark {
	case HashHSet:
		db.hashIndex.HSet(string(entry.Meta.Key), string(entry.Meta.Extra), entry.Meta.Value)
	case HashHDel:
		db.hashIndex.HDel(string(entry.Meta.Key), string(entry.Meta.Extra))
	}
}
