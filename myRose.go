package myRose

import (
	"myRose/index/hash"
	"myRose/storage"
)

type (
	MyRose struct {
		activeFile   *storage.DBFile // 当前的活跃文件
		activeFileId uint32          // 当前活跃文件id
		archFileAll  ArchivedFiles   // 活跃用户的总集合
		hashIndex    *hash.Hash      // hash索引
		//config
		meta *storage.DBMeta //数据库配置额外信息

	}

	// ArchivedFiles 已封存的文件定义
	ArchivedFiles map[uint32]*storage.DBFile
)

// entry节点写入日志文件中
func (db *MyRose) store(entry *storage.Entry) error {

	// 暂时写死
	var blockSize = int64(1024)
	var sync = true
	fileDirPath := "/tmp/myRose/test/"

	if db.activeFile.Offset+int64(entry.Size()) > blockSize {
		// 写满的文件刷到磁盘中
		err := db.activeFile.Close(sync)
		if err != nil {
			return err
		}

		// 加入到已封存的文件集合中
		db.archFileAll[db.activeFileId] = db.activeFile

		// 构建下一个文件
		acfiveFileId := db.activeFileId + 1
		dbFile, err := storage.NewDBFile(fileDirPath, acfiveFileId, storage.FileIO)
		if err != nil {
			return err
		}
		db.activeFile = dbFile
		db.activeFileId = acfiveFileId
		db.activeFile.Offset = 0
	}

	// entry写入activeFile
	err := db.activeFile.Write(entry)
	if err != nil {
		return err
	}

	// 将entry写入activeFile后，会修改activeFile的OffSet值
	db.meta.ActiveWriteOff = db.activeFile.Offset

	if sync {
		err = db.activeFile.Sync()
		if err != nil {
			return err
		}
	}
	return nil
}

// todo: build my rose db from file
