package bpct

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/zssky/log"
)

func TestNewInstance(t *testing.T) {
	i, err := NewInstance("root", "secret", 3306)
	if err != nil {
		log.Fatal(err)
	}

	defer i.Close()

	if err := i.Flush(); err != nil {
		log.Error(err)
	}
}

func TestInstance_Check(t *testing.T) {
	i, err := NewInstance("root", "secret", 3306)
	if err != nil {
		log.Fatal(err)
	}
	defer i.Close()

	if err := i.Check(); err != nil {
		log.Fatal(err)
	}
}

func TestInstance_Commit(t *testing.T) {
	ctx := context.Background()

	i, err := NewInstance("root", "secret", 3306)
	if err != nil {
		log.Fatal(err)
	}
	defer i.Close()

	// one table one connection
	for _, t := range []string{"test.test01", "test.test02", "test.test03", "test.test04", "test.test05", "test.aaa"} {
		log.Infof("routine for table {%s}", t)

		conn, err := i.db.Conn(ctx)
		if err != nil {
			log.Fatal(err)
		}

		go func(table string, conn *sql.Conn) {
			for {
				tx, err := conn.BeginTx(ctx, nil)
				if err != nil {
					log.Fatal(err)
				}

				j := rand.Int()
				for k := 0; k < 100; k ++ {
					s := fmt.Sprintf("insert into %s select '%d', 'name%d', 'address%d'", table, j*10+k, j*10+k, j*10+k)
					if _, err := tx.Exec(s); err != nil {
						log.Errorf("execute sql{%s} error {%v}", s, err)
						log.Fatal(err)
					}
				}

				if err := tx.Commit(); err != nil {
					log.Fatal(err)
				}
			}
		}(t, conn)
	}

	errs := make(chan error, 61)
	for e := range errs {
		log.Info(e)
	}
}

func TestInstance_Execute(t *testing.T) {

	for i := 0; i<  10; i++ {
		ins, err := NewInstance("root", "secret", 3306)
		if err != nil {
			log.Fatal(err)
		}
		go func(inst *Instance) {

			defer inst.Close()

			for {
				if err := inst.Begin(); err != nil {

				}

				if err := inst.Execute([]byte("set names='utf-8'")); err != nil {
				}

				if err := inst.Commit(); err != nil {

				}
			}
		}(ins)
	}

	time.Sleep(1000000 * time.Hour)
}

func TestInstance_GtidMode(t *testing.T) {
	i, err := NewInstance("root", "secret", 3306)
	if err != nil {
		log.Fatal(err)
	}
	defer i.Close()

	log.Infof("gtid mode %v", i.GtidMode())
}

func TestInstance_MaxPackageSize(t *testing.T) {
	i, err := NewInstance("root", "secret", 3306)
	if err != nil {
		log.Fatal(err)
	}
	defer i.Close()

	log.Infof("gtid mode %v", i.maxPackageSize())
}