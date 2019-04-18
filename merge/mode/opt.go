package mode

import (
	"github.com/mysql-binlog/common/final"
	"github.com/mysql-binlog/siddontang/go-mysql/replication"
)

// IBinlogDump binlog dump interface
type IBinlogDump interface {
	Handle(f func(ev *replication.BinlogEvent) bool,  a *final.After)
}
