package inter

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/zssky/log"

	"github.com/mysql-binlog/common/meta"
)

func TestExists(t *testing.T) {
	f := `/export/backup/1/test.test02/1561346137.log`
	fmt.Println(Exists(f))
}

func TestTail(t *testing.T) {
	f := `./so.index`
	b, err := LastLine(f)
	if err != nil {
		log.Fatal(err)
	}

	o := &meta.Offset{}
	if err := json.Unmarshal(b, o); err != nil {
		log.Fatal(err)
	}

	log.Infof("%v", o)
}
