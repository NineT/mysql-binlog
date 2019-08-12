package bpct

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"testing"

	"github.com/zssky/log"
)

func TestNewInstance(t *testing.T) {
	i, err := NewInstance("root", "secret", 3306)
	if err != nil {
		log.Fatal(err)
	}

	defer i.Close()
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
