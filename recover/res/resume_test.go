package res

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/mysql-binlog/siddontang/go-mysql/replication"
	"github.com/zssky/log"

	"github.com/mysql-binlog/common/db"
	"github.com/mysql-binlog/common/inter"
)

func TestRecover_Execute(t *testing.T) {
	f, err := os.OpenFile("/tmp/out_cus.log", os.O_CREATE|os.O_TRUNC|os.O_RDWR, inter.FileMode)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	// use log file should close
	log.SetOutput(f)

	wg := &sync.WaitGroup{}
	wg.Add(1)
	r := &Recover{
		TaskCounter: wg,
		SqlExecutor: &db.MetaConf{},
		buffer:      bytes.NewBuffer(nil),
		logFileChan: make(chan *inter.FileName, 64),
		parser:      replication.NewBinlogParser(),
	}

	r.setDelimiter()

	go r.execute()

	r.logFileChan <- &inter.FileName{Path: "/home/pengan", Name: "hour_1551142220_1551145820.log", Suffix: inter.LogSuffix}
	close(r.logFileChan)

	wg.Wait()
}

func TestRecover_Clear(t *testing.T) {
	s := &db.MetaConf{
		Host:     "127.0.0.1",
		IP:       "127.0.0.1",
		Port:     3306,
		Db:       "test",
		User:     "root",
		Password: "secret",
	}

	s.RefreshConnection()

	if err := s.Execute([]byte("select 1")); err != nil {
		log.Fatal(err)
	}

	if err := s.Execute([]byte(fmt.Sprintf("DELIMITER %s", inter.Delimiter))); err != nil {
		log.Fatal(err)
	}

	if err := s.Execute([]byte(fmt.Sprintf("DELIMITER %s\nROLLBACK %s", inter.Delimiter, inter.Delimiter))); err != nil {
		log.Fatal(err)
	}
}

func TestRecover_StartPubRecover(t *testing.T) {
	c := make(chan string, 64)

	wg := &sync.WaitGroup{}

	wg.Add(1)

	go func() {
		i := 0
		for {
			select {
			case s, hasMore := <-c:
				if !hasMore {
					wg.Done()
					return
				}

				fmt.Println("index ", i, ", ", hasMore, s)
				i ++
			}
		}
	}()

	for i := 0; i < 20; i++ {
		s := fmt.Sprintf("%d - %d", i, i)
		c <- s
	}

	close(c)

	wg.Wait()
}
