package storage

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

type DBMeta struct {
	ActiveWriteOff int64 `json:"active_write_off"`
}

// LoadMeta 从文件中加载DBMeta
// @param path 文件路径
// @return error 错误信息
// @return *DBMeta DBMeta对象
func LoadMeta(path string) (e error, m *DBMeta) {
	m = &DBMeta{}

	file, err := os.OpenFile(path, os.O_RDONLY, 0600)
	if err != nil {
		return
	}
	defer file.Close()

	b, _ := ioutil.ReadAll(file)
	_ = json.Unmarshal(b, m)
	return
}

func (m *DBMeta) Store(path string) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	data, _ := json.Marshal(m)
	_, err = file.Write(data)
	return err
}
