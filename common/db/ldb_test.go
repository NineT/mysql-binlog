package db

import (
	"bytes"
	"fmt"
	"github.com/mysql-binlog/common/inter"
	"github.com/zssky/log"
	"os"
	"testing"
)

func TestInitDb(t *testing.T) {
	if err := os.RemoveAll("/tmp/pengan"); err != nil {

	}

	ldb := InitDb(&inter.AbsolutePath{
		TmpSrc: "/tmp/pengan/",
	})

	defer ldb.Close()

	// init data
	for i := 0; i < 10; i ++ {
		if err := ldb.Put([]byte(fmt.Sprintf("pengan%d%d", i, i)), []byte(fmt.Sprintf("pengan%d%d", i, i)), nil); err != nil {
			log.Fatal(err)
		}
	}

	// first snapshot
	fs, err := ldb.GetSnapshot()
	if err != nil {
		log.Fatal(err)
	}
	defer fs.Release()

	iter := fs.NewIterator(nil, nil)
	for iter.Next() {
		fmt.Println("first snapshot ", " key ", string(iter.Key()), ", value ", string(iter.Value()))
	}
	defer iter.Release()


	// init data
	for i := 10; i < 20; i ++ {
		if err := ldb.Put([]byte(fmt.Sprintf("pengan%d%d", i, i)), []byte(fmt.Sprintf("pengan%d%d", i, i)), nil); err != nil {
			log.Fatal(err)
		}
	}

	//second snapshot
	ss, err := ldb.GetSnapshot()
	if err != nil {
		log.Fatal(err)
	}
	defer ss.Release()

	siter := ss.NewIterator(nil, nil)
	for siter.Next() {
		k := siter.Key()
		cv := siter.Value()

		bv, _ := fs.Get(k, nil)
		if bv != nil && bytes.Equal(bv, cv) {
			log.Warn("key exist in previous snapshot then continue not for current, before value ", string(bv), ", current value ", string(cv))
			continue
		}
		fmt.Println("second snapshot ", "key ", string(k), ", value ", string(cv))
	}
	defer siter.Release()
}
