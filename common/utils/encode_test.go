package utils

import (
	"encoding/base64"
	"fmt"
	"testing"

	"github.com/mysql-binlog/siddontang/go-mysql/mysql"
	"github.com/mysql-binlog/siddontang/go-mysql/replication"
)

func TestBase64EncodeFormatDescEvent(t *testing.T) {
	str := `PBmWWg8VAAAAZwAAAGsAAAAAAAQANS41LjU0LWxvZwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
AAAAAAAAAAAAAAAAAAAAAAAAEzgNAAgAEgAEBAQEEgAAVAAEGggAAAAICAgCAA==`

	bt, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		fmt.Println(err)
	}

	h := &replication.EventHeader{}
	h.Decode(bt[:replication.EventHeaderSize])
	fmt.Println(h)

	fde := &replication.FormatDescriptionEvent{}
	if err := fde.Decode(bt[replication.EventHeaderSize:]); err != nil {
		fmt.Println(err)
	}
}

func TestBase64TableMapEvent(t *testing.T) {
	str := `+BmWWhMVAAAASAAAAP4CAAAAAPoBAAAAAAEACHRvd2VyX3YyAAhzbmFwc2hvdAAJAw8PDw8PDwcH
DP0C/QL9Av0C/QL9AhAA`

	bt, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		fmt.Println(err)
	}

	h := &replication.EventHeader{}
	h.Decode(bt[:replication.EventHeaderSize])
	fmt.Println(h)

	tme := &replication.TableMapEvent{}
	tme.Decode(bt[replication.EventHeaderSize+6:])
	tme.TableID = mysql.FixedLengthInt(bt[replication.EventHeaderSize : replication.EventHeaderSize+6])
	fmt.Println(string(tme.Schema), ", ", string(tme.Table), ", table id ", tme.TableID)
}

func TestBase64RowsEvent(t *testing.T) {
	str := `+BmWWhkVAAAAUAEAAIYGAAAAAPoBAAAAAAEACf//AP6/NAMAEwAxOTIuMTY4LjgxLjEwNzozMzU4
AAABADBWADI5NzM3NDg5LTAxOWUtMTFlOC04NDBhLWZhMTYzZTIyZjdlZjoxLTIsCjJhNDFkMjNm
LTAxOWUtMTFlOC04NGEyLWZhMTYzZTUzMmRkNjoxLTk5NzEzAQAwDgAxOTIuMTY4LjgxLjEwN/gZ
llr4GZZaAP7ANAMAEwAxOTIuMTY4LjgxLjEwODozMzU4EABteXNxbC1iaW4uMDAwMDAxCAA3Njcw
NTk5OVYAMjk3Mzc0ODktMDE5ZS0xMWU4LTg0MGEtZmExNjNlMjJmN2VmOjEtMiwKMmE0MWQyM2Yt
MDE5ZS0xMWU4LTg0YTItZmExNjNlNTMyZGQ2OjEtOTk3MTMBADAOADE5Mi4xNjguODEuMTA4+BmW
WvgZllo=`

	bt, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		fmt.Println(err)
	}

	h := &replication.EventHeader{}
	h.Decode(bt[:replication.EventHeaderSize])
	fmt.Println(h)

	tableID := mysql.FixedLengthInt(bt[replication.EventHeaderSize : replication.EventHeaderSize+6])
	fmt.Println(tableID)

	fmt.Println(bt[replication.EventHeaderSize:])
}
