package inter

import (
	"encoding/json"
	"fmt"
	"sort"
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
	if err := json.Unmarshal([]byte(b), o); err != nil {
		log.Fatal(err)
	}

	log.Infof("%v", o)
}

func TestInt64s_Len(t *testing.T) {
	a := Int64s{10, 20, 30, 40, 50}
	sort.Sort(a)

	log.Infof("%v", a)
}
