package pb

import (
	"fmt"
	"testing"

	"github.com/mysql-binlog/siddontang/go-mysql/replication"
)

func TestPbToByteArray(t *testing.T) {
	is := make([][]interface{}, 2)
	is[0] = make([]interface{}, 2)

	is[0][0] = "string-type-then"
	is[0][1] = 8989

	is[1][0] = int64(28408)
	is[1][1] = []byte("byte-type")

}

func TestProtoEnum2Num(t *testing.T) {
	evtp := EVENT_TYPE_WRITE_ROWS_EVENTv1
	fmt.Println(int32(evtp))
	fmt.Println(replication.EventType(int32(evtp)))
}
