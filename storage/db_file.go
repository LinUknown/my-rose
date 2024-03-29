package storage

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
)

type FileRWMethod uint8

const (
	FileIO FileRWMethod = iota
)

var (
	ErrEmptyEntry = errors.New("storage/db_file: entry or the Key of entry is empty")
)

const (
	// FilePerm 默认的创建文件权限
	FilePerm = 0644

	// DBFileFormatName 默认数据文件名称格式化
	DBFileFormatName = "%09d.data"

	PathSeparator = string(os.PathSeparator)
)

// DBFIle 表示一个日志集合，对于每一次操作日志，都会写到一个日志集合
// 写满了之后，封存到文件中，重新创建一个新的DBFile用于写入
type DBFIle struct {
	Id     uint32
	path   string
	File   *os.File
	Offset int64
	method FileRWMethod
}

// Read 从文件的offset开始，中读取一个entry,
func (df *DBFIle) Read(offSet int64) (*Entry, error) {

	nextOffset, entry, err := df.BuildEntryHead(offSet)
	if err != nil {
		return nil, err
	}
	nextOffset, err = df.BuildEntryKey(entry, nextOffset)
	if err != nil {
		return nil, err
	}
	nextOffset, err = df.BuildEntryValue(entry, nextOffset)
	if err != nil {
		return nil, err
	}
	nextOffset, err = df.BuildEntryExtra(entry, nextOffset)
	if err != nil {
		return nil, err
	}

	return entry, nil
}

func (df *DBFIle) BuildEntryKey(entry *Entry, offset int64) (int64, error) {
	buf, err := df.readBuf(offset, int64(entry.Meta.KeySize))
	if err != nil {
		return 0, err
	}
	entry.Meta.Key = buf
	return offset + int64(entry.Meta.KeySize), nil
}

func (df *DBFIle) BuildEntryValue(entry *Entry, offset int64) (int64, error) {
	buf, err := df.readBuf(offset, int64(entry.Meta.ValueSize))
	if err != nil {
		return 0, err
	}
	entry.Meta.Value = buf

	checkCrc := crc32.ChecksumIEEE(entry.Meta.Value)
	if checkCrc != entry.crc32 {
		return 0, ErrInvalidCrc
	}
	return offset + int64(entry.Meta.ValueSize), nil
}

func (df *DBFIle) BuildEntryExtra(entry *Entry, offset int64) (int64, error) {
	buf, err := df.readBuf(offset, int64(entry.Meta.ExtraSize))
	if err != nil {
		return 0, err
	}
	entry.Meta.Extra = buf
	return offset + int64(entry.Meta.ExtraSize), nil
}

func (df *DBFIle) BuildEntryHead(beginOffset int64) (int64, *Entry, error) {
	// 下面两步，应该封装成一个方法。 作用是取出一个entry的头部
	buf, err := df.readBuf(beginOffset, entryHeaderSize)
	if err != nil {
		return 0, nil, err
	}

	entry, err := Decode(buf)
	if err != nil {
		return 0, nil, err
	}

	return beginOffset + entryHeaderSize, entry, nil
}

func (df *DBFIle) Write(entry *Entry) error {
	if entry == nil || entry.Meta.KeySize == 0 {
		return ErrEmptyEntry
	}

	buf, err := entry.Encode()
	if err != nil {
		return err
	}

	_, err = df.File.WriteAt(buf, df.Offset)
	if err != nil {
		return err
	}

	df.Offset += int64(entry.Size())
	return nil
}

// Build 从文件夹中，构建DBFile的字典，key是递增的file_id
// 函数会返回当前最大的file_id
// 上层通过最大的file_id, 遍历这个map，得到所有的日志集合
func Build(dirPath string) (map[uint32]*DBFIle, uint32, error) {
	// read dir, get all dbfile
	// get all db file ids
	// sort, build for a map

	dir, err := ioutil.ReadDir(dirPath)
	if err != nil {
		return nil, 0, err
	}

	var fileIds []int
	for _, fileItem := range dir {
		if strings.HasSuffix(fileItem.Name(), "data") {
			splitNames := strings.Split(fileItem.Name(), ".")
			id, _ := strconv.Atoi(splitNames[0])
			fileIds = append(fileIds, id)
		}
	}

	sort.Ints(fileIds)
	activeFileId := uint32(0)
	archFiles := make(map[uint32]*DBFIle)
	if len(fileIds) == 0 {
		return archFiles, activeFileId, nil
	}

	activeFileId = uint32(fileIds[len(fileIds)-1])
	for i := 0; i < len(fileIds); i++ {
		fileId := fileIds[i]
		curDBFile, err := NewDBFile(dirPath, uint32(fileId), FileIO)
		if err != nil {
			return nil, 0, err
		}
		archFiles[uint32(fileId)] = curDBFile

	}
	return archFiles, activeFileId, nil

}

// 根据文件夹地址和文件id， 打开文件具柄， 封装成dbfile对象
func NewDBFile(fileDirPath string, fileId uint32, method FileRWMethod) (*DBFIle, error) {
	filePath := fileDirPath + PathSeparator + fmt.Sprintf(DBFileFormatName, fileId)

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, FilePerm)
	if err != nil {
		return nil, err
	}

	df := &DBFIle{
		Id:     fileId,
		path:   filePath,
		File:   file,
		Offset: 0,
		method: method,
	}
	return df, nil
}

// 对文件系统里，文件的读取操作
func (df *DBFIle) readBuf(offset int64, n int64) ([]byte, error) {

	buf := make([]byte, n)
	_, err := df.File.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (df *DBFIle) Close(sync bool) error {
	if sync {
		err := df.sync()
		if err != nil {
			return err
		}
	}

	err := df.File.Close()
	return err
}

func (df *DBFIle) sync() error {
	err := df.File.Sync()
	return err
}
