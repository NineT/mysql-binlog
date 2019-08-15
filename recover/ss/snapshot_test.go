package ss

import (
	"context"
	"fmt"
	"github.com/mysql-binlog/common/inter"
	"github.com/mysql-binlog/siddontang/go-mysql/mysql"
	"github.com/zssky/log"
	"sync"
	"testing"
	"time"
)

func TestNewSnapshot(t *testing.T) {
	s, err := NewSnapshot("/export/backup", 59923, inter.ParseTime("2019-08-06"))
	if err != nil {
		log.Fatal(err)
	}

	log.Info(s.ID())
}

func TestNewCluster(t *testing.T) {

	og, err := mysql.ParseMysqlGTIDSet("11c0d44e-51c5-11e8-91a9-ba69a456fe1c:1-734072,2d53d4de-ebb6-11e7-92c5-3aec959bea56:1-102,3be20e7d-ebb6-11e7-928e-261f8ccca485:1-269,efe65103-f695-11e7-8638-96b32dd55a68:1-126905")
	if err != nil {
		log.Errorf("parse mysql error{%v}", err)
		log.Fatal(err)
	}

	// 11c0d44e-51c5-11e8-91a9-ba69a456fe1c:1-734072,2d53d4de-ebb6-11e7-92c5-3aec959bea56:1-102,3be20e7d-ebb6-11e7-928e-261f8ccca485:1-269,efe65103-f695-11e7-8638-96b32dd55a68:1-126905
	if err := og.Update("efe65103-f695-11e7-8638-96b32dd55a68:1-126905"); err != nil {
		log.Fatal(err)
	}
	log.Infof("timestamp %v", false)
}

func TestSnapshot_Auth(t *testing.T) {
	errs := make(chan error, 64)

	wg := &sync.WaitGroup{}
	wg.Add(10)

	ctx, cancel := context.WithCancel(context.Background())

	for i := 0; i < 10; i ++ {
		go func(k int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					log.Infof("context %d canceled", k)
					return
				default:
					time.Sleep(time.Second)
					log.Infof("routine %d running", k)
					if k == 3 {
						errs <- fmt.Errorf("time out")
					}
				}
			}
		}(i)
	}

	go func() {
		for {
			select {
			case <-errs:
				log.Info("calling cancel")
				cancel()
				log.Info("all canceled")
			}
		}
	}()

	wg.Wait()
	log.Info("all is closed")
}
