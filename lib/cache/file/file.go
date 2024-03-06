package file

import (
	"crypto/md5"
	"encoding/hex"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/unknwon/com"
)

// file缓存上下文
type Cacher struct {
	rootPath string
}

// 创建file缓存上下文
func NewCacher(path string) *Cacher {
	return &Cacher{
		rootPath: path,
	}
}

//内部方法：获取文件路径
func (c *Cacher) filepath(key string) string {
	m := md5.Sum([]byte(key))
	hash := hex.EncodeToString(m[:])
	return filepath.Join(c.rootPath, string(hash[0]), string(hash[1]), hash)
}

//内部方法：读取文件内容
func (c *Cacher) read(key string) ([]byte, error) {
	filename := c.filepath(key)

	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// 写入缓存
func (c *Cacher) Put(key string, val []byte) error {
	filename := c.filepath(key)
	os.MkdirAll(filepath.Dir(filename), os.ModePerm)

	return ioutil.WriteFile(filename, val, os.ModePerm)
}

// 获取缓存
func (c *Cacher) Get(key string) []byte {
	val, err := c.read(key)
	if err != nil {
		return nil
	}

	return val
}

// 删除缓存
func (c *Cacher) Delete(key string) error {
	return os.Remove(c.filepath(key))
}

// 判断缓存是否存在
func (c *Cacher) IsExist(key string) bool {
	return com.IsExist(c.filepath(key))
}

// 清除所有缓存
func (c *Cacher) Flush() error {
	return os.RemoveAll(c.rootPath)
}
