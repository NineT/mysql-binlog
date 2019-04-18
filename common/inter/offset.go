package inter

import (
	"database/sql"
	"log"
	"strings"

	"github.com/mysql-binlog/siddontang/go-mysql/mysql"
)

// MySQLConfig configuration connect to mysql
type MySQLConfig struct {
	Host       string  // Host MySQL host 信息
	IP         string  // IP host對應的ip信息
	Port       int     // Port 端口信息
	Db         string  // Db 数据库名
	User       string  // User 用户名
	Password   string  // Password 链接MySQL 密码
	Conn       *sql.DB // Conn MySQL 链接
	Tx         *sql.Tx // transaction
}

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
