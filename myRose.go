package myRose

import (
	"myRose/index/hash"
	"myRose/storage"
	"os"
)

type (
	MyRose struct {
		activeFile   *storage.DBFile // 当前的活跃文件
		activeFileId uint32          // 当前活跃文件id
		archFileAll  ArchivedFiles   // 活跃用户的总集合
		hashIndex    *hash.Hash      // hash索引
		meta         *storage.DBMeta //数据库配置额外信息
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

func (db *MyRose) buildIndexByEntry(entry *storage.Entry) {
	// 构建索引的总入口
	// 基于entry，构建索引
	// 根据entry的类型，调用不同的构建索引方法
	// 目前只支持hash类型
	switch entry.Type {
	case storage.Hash:
		db.buildHashIndexByEntry(entry)
	}
}

func Open() (*MyRose, error) {
	dirFilePath := "/tmp/myRose/test/"
	if _, err := os.Stat(dirFilePath); os.IsNotExist(err) {
		_, err := os.Create(dirFilePath)
		if err != nil {
			return nil, err
		}
	}

	// 从数据目录中，构建数据文件和活跃文件id
	allActiveFileMap, topFileId, err := storage.Build(dirFilePath)
	if err != nil {
		return nil, err
	}
	// 取出最近的活跃文件
	topActiveFile := allActiveFileMap[topFileId]

	// todo: 不知道为什么这里要单独处理，我觉得直接取map就行
	//topActiveFile, err := storage.NewDBFile(dirFilePath, topFileId, storage.FileIO)
	//if err != nil {
	//	return nil, err
	//}

	// 从数据目录中，加载meta.json文件
	err, meta := storage.LoadMeta(dirFilePath + "meta.json")
	if err != nil {
		return nil, err
	}

	// 构建db对象
	db := &MyRose{
		activeFile:   topActiveFile,
		activeFileId: topFileId,
		archFileAll:  allActiveFileMap,
		hashIndex:    hash.New(),
		meta:         meta,
	}

	// 从文件中加载索引
	err = db.loadIdxFromFiles()
	if err != nil {
		return nil, err
	}
	return db, nil
}

func (db *MyRose) Close() error {
	if err := db.saveMeta(); err != nil {
		return err
	}

	if err := db.activeFile.Close(true); err != nil {
		return err
	}

	return nil
}

func (db *MyRose) saveMeta() error {
	dirFilePath := "/tmp/myRose/test/meta.json"
	err := db.meta.Store(dirFilePath)
	if err != nil {
		return err
	}
	return nil
}
