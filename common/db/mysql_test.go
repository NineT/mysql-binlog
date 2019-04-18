package db

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/mysql-binlog/common/final"
	// mysql
	_ "github.com/go-sql-driver/mysql"
)

func TestMySQLQuery(t *testing.T) {
	m := &MetaConf{
		IP: "127.0.0.1",
		Port: 3306,
		Db: "mysql",
		User: "root",
		Password: "secret",
	}

	m.RefreshConnection()
	defer m.Close()

	fmt.Println(m.queryUniqueIndexColumns("test.test1"))
}

func TestMySQLConfig_UpdatePosition(t *testing.T) {
	conf := &MetaConf{
		Host:     "192.168.200.158",
		Port:     3306,
		User:     "root",
		Password: "secret",
		Db:       "binlog_backup",
	}
	url := fmt.Sprintf("%s:%s@tcp(%s:%d)/", conf.User, conf.Password, conf.Host, conf.Port)
	mysqlConn, err := sql.Open("mysql", url)
	if err != nil {
		final.Terminate(err, nil)
	}
	conf.Conn = mysqlConn

	update := fmt.Sprintf(
		"REPLACE INTO %s.position SET domain = ?, ip = ?, gtid_sets = ?, binlog_file = ?, binlog_pos = ?",
		conf.Db)

	conf.RefreshConnection()
	defer conf.Close()

	rst, err := conf.Conn.Exec(update, "dump158.db.com", "192.168.200.158", "", "mysql-bin.000008", 0)
	if err != nil {
		final.Terminate(err, nil)
	}

	fmt.Println(rst)
}

func TestMySQLConfig_GetPosition(t *testing.T) {
	conf := &MetaConf{
		Host:     "192.168.200.158",
		Port:     3306,
		User:     "root",
		Password: "secret",
		Db:       "binlog_backup",
	}

	fmt.Println(conf.GetPosition("dump158.db.com"))
}

func TestCopy(t *testing.T) {
	size := 19
	bts := make([]byte, 30)

	head := []byte("67890tyuionm,yuionm")
	body := []byte("bbbbbbbbbbb")

	copy(bts, head)
	copy(bts[size:], body)

	fmt.Println(string(bts))

}

func TestMySQLConfig_GetBinlogPath(t *testing.T) {
	stopTime, _ := time.Parse("2006-01-02 15:04:05", "2999-12-30 23:59:59")
	fmt.Println(stopTime.Unix())
}
