package inter

import (
	"fmt"
	"testing"

	"github.com/zssky/log"
)

func TestExists(t *testing.T) {
	f := `/export/backup/1/test.test02/1561346137.log`
	fmt.Println(Exists(f))
}

func TestTail(t *testing.T) {
	f := `/export/backup/1/test.aaa/list`
	b, err := Tail(f)
	if err != nil {
		log.Fatal(err)
	}

	log.Info(string(b))
}
