package mode

import (
	"github.com/mysql-binlog/siddontang/go-mysql/replication"

	"github.com/mysql-binlog/common/final"
)

// IBinlogDump binlog dump interface
type IBinlogDump interface {
	Handle(f func(ev *replication.BinlogEvent) bool, a *final.After)
}
