package db

import (
	"fmt"
	"os"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/zssky/log"

	"github.com/mysql-binlog/common/inter"
)

// InitDb init level Db
func InitDb(ap *inter.AbsolutePath) *leveldb.DB {
	ldb, err := leveldb.OpenFile(ap.TmpSourcePath(), nil)
	if err != nil {
		log.Debug("error for open level Db ", err.Error())
		os.Exit(-1)
	}
	return ldb
}

// fileExist : check file exists
func fileExist(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil || os.IsExist(err)
}

// CopyDb : copy db to another new path
func CopyDb(src, dst string) (*leveldb.DB, string, error) {
	if !fileExist(src) {
		// file not exist then just return
		return nil, "", nil
	}
	i := 1
	nf := ""
	for {
		// 固定占位6个字符
		nf = fmt.Sprintf("%s_%06d", dst, i)
		if fileExist(nf) {
			i++
			continue
		}
		break
	}
	// rename file name
	err := os.Rename(src, nf)
	if err != nil {
		return nil, nf, err
	}

	// open the new db using the old src
	ndb, err := leveldb.OpenFile(src, nil)
	if err != nil {
		return nil, nf, err
	}

	return ndb, nf, nil
}
