package inter

import (
	"log"
	"strings"

	"github.com/mysql-binlog/siddontang/go-mysql/mysql"
)

// BinlogPosition binlog position
type BinlogPosition struct {
	Domain string // Domain 域名
	IP     string // IP 域名对应的ip地址

	GTIDSet    mysql.GTIDSet // GTID 对象
	BinlogFile string        // BinlogFile 文件
	BinlogPos  uint32        // BinlogPos binlog位置
	Timestamp  int64         // Timestamp 结束时间
	Status     string        // Status 状态
}


func (p *BinlogPosition) Copy() *BinlogPosition {
	b := &BinlogPosition{
		Domain:     p.Domain,
		IP:         p.IP,
		BinlogFile: p.BinlogFile,
		BinlogPos:  p.BinlogPos,
		Timestamp:  p.Timestamp,
	}

	if p.GTIDSet != nil {
		g, err := mysql.ParseGTIDSet(mysql.MySQLFlavor, p.GTIDSet.String())
		if err != nil {
			log.Fatal("copy binlog position failure")
		}
		b.GTIDSet = g
	}

	return b
}

// Less true: means p <= p1; false: meas p > p1
func (p *BinlogPosition) Less(p1 *BinlogPosition) bool {
	// compare timestamp: but timestamp may be the same using offset
	if p.Timestamp < p1.Timestamp || strings.Compare(p.BinlogFile, p1.BinlogFile) < 0 {
		return true
	}

	if p.Timestamp > p1.Timestamp || strings.Compare(p.BinlogFile, p1.BinlogFile) > 0 {
		return false
	}

	// time is equals and binlog file is equal using offset or gtid instead
	if p.BinlogPos < p1.BinlogPos {
		return true
	}

	return false
}
