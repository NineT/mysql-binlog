package db

import (
	"database/sql"
	"fmt"
	"github.com/mysql-binlog/common/meta"
	"strconv"
	"strings"

	// mysql
	_ "github.com/go-sql-driver/mysql"
	"github.com/zssky/log"

	"github.com/mysql-binlog/common/final"
	"github.com/mysql-binlog/common/inter"
	"github.com/mysql-binlog/siddontang/go-mysql/mysql"
)

// MetaConf 元数据信息
type MetaConf inter.MySQLConfig

// TableMeta table meta data
type TableMeta struct {
	Columns  []string
	KeyIndex []int
}

// BinlogOffset MySQL index offset
type BinlogOffset struct {
	File    string `json:"file"`  // file name
	IP      string `json:"ip"`    // ip on MySQL node
	Start   int64  `json:"start"` // start time
	End     int64  `json:"end"`   // end time
	GTID    string `json:"gtid"`  // GTID range offset
	GTIDSet mysql.GTIDSet         // GTIDSet object
}

const indexName = 2
const columnName = 4

// unique index from table
const unique = "0"

// primary index from table index
const primary = "primary"

// initConnection init MySQL connection
func (c *MetaConf) initConnection() {
	// SET MAX ALLOWED PACKAGE SIZE == 0 THEN =>  https://github.com/go-sql-driver/mysql/driver.go:142
	/***
		if mc.cfg.MaxAllowedPacket > 0 {
		mc.maxAllowedPacket = mc.cfg.MaxAllowedPacket
	} else {
		// Get max allowed packet size
		maxap, err := mc.getSystemVar("max_allowed_packet")
		if err != nil {
			mc.Close()
			return nil, err
		}
		mc.maxAllowedPacket = stringToInt(maxap) - 1
	}
	if mc.maxAllowedPacket < maxPacketSize {
		mc.maxWriteSize = mc.maxAllowedPacket
	}
	 */
	url := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?maxAllowedPacket=0", c.User, c.Password, c.Host, c.Port, c.Db)
	mysqlConn, err := sql.Open("mysql", url)
	if err != nil {
		final.Terminate(err, nil)
	}
	c.Conn = mysqlConn
}

// RefreshConnection  刷新链接 {没有则创建 异常关闭后创建}
func (c *MetaConf) RefreshConnection() {
	if c.Conn == nil {
		c.initConnection()
		return
	}

	if err := c.Conn.Close(); err != nil {
		log.Error("close connection pool error")
	}

	c.Tx = nil

	c.initConnection()
}

// Close close mysql connection
func (c *MetaConf) Close() {
	if c.Conn != nil {
		c.Conn.Close()
	}
}

// queryUniqueIndexColumns 获取唯一索引列名称
func (c *MetaConf) queryUniqueIndexColumns(table string) []string {
	c.RefreshConnection() // 刷新链接 保证链接正常

	rst, err := c.Conn.Query(fmt.Sprintf("SHOW INDEX FROM %s", table))
	if err != nil {
		log.Info(err)
		return nil
	}
	defer rst.Close()

	columns, _ := rst.Columns()
	length := len(columns)

	indexes := make(map[string][]string)
	for rst.Next() {
		value := make([]string, length)
		valPointer := make([]interface{}, length)
		for i := 0; i < length; i++ {
			valPointer[i] = &value[i]
		}

		rst.Scan(valPointer...)

		switch value[1] {
		case unique: // non_unique = 0
			indexName := inter.CharStd(value[indexName])
			indexColumn := inter.CharStd(value[columnName])

			cols := append(indexes[indexName], indexColumn)
			indexes[indexName] = cols
		default:
			continue
		}
	}

	if len(indexes) == 0 {
		log.Info("no unique index in table")
		return nil
	}

	if len(indexes) == 1 {
		log.Info("only one unique index on table ", table)
		for key, value := range indexes {
			log.Debug("key ", key, ", value ", value)
			return value
		}
	}

	if _, ok := indexes[primary]; ok {
		log.Debug("primary index column is ", indexes[primary])
		return indexes[primary]
	}

	// take the minimum size of columns and to prevent random choose using order by name
	k := ""
	s := length // take column number as the max size
	for key := range indexes {
		// column index size
		cs := len(indexes[key])
		if cs < s {
			k = key
			s = len(indexes[key])
		}

		// equals size then using key order
		if cs == s && strings.Compare(key, k) < 0 {
			k = key
			s = len(indexes[key])
		}
	}

	log.Debug(indexes[k])
	return indexes[k]
}

// queryUniqueIndexColumns 获取唯一索引列列序号 参数表全称: 库名.表名
func (c *MetaConf) queryUniqueIndexNum(table string) ([]int, []string) {
	// 无唯一索引列 则返回nil
	cols := c.queryUniqueIndexColumns(table)
	if cols == nil {
		return nil, nil
	}

	// 获取表结构
	rst, err := c.Conn.Query(fmt.Sprintf("SELECT * FROM %s WHERE 1 = 0", table))
	if err != nil {
		final.Terminate(err, nil)
	}
	defer rst.Close()

	totalCols, _ := rst.Columns()
	var keyIndex []int
	for index, col := range totalCols {
		upper := strings.ToUpper(col)
		for _, indexCol := range cols {
			if strings.Compare(upper, indexCol) == 0 {
				keyIndex = append(keyIndex, index)
			}
		}
	}

	return keyIndex, totalCols
}

// NewTableMeta get table meta according to full table name
func (c *MetaConf) NewTableMeta(table string) *TableMeta {
	keyIndex, columns := c.queryUniqueIndexNum(table)
	return &TableMeta{KeyIndex: keyIndex, Columns: columns}
}

// GetPosition 根据域名 获取binlog backup的位置信息
func (c *MetaConf) GetPosition(host string) (*inter.BinlogPosition, error) {
	query := fmt.Sprintf(
		"SELECT domain, ip, gtid_sets, binlog_file, binlog_pos FROM %s.position "+
			"WHERE domain = ? AND status = 'success' ORDER BY last_time DESC LIMIT 1", c.Db)

	c.RefreshConnection()
	defer c.Close()

	rst, err := c.Conn.Query(query, host)
	if err != nil {
		return nil, err
	}
	defer rst.Close()

	pos := &inter.BinlogPosition{}

	gtid := ""
	// 反射获取结果集
	for rst.Next() {
		rst.Scan(&pos.Domain, &pos.IP, &gtid, &pos.BinlogFile, &pos.BinlogPos)
	}

	pos.GTIDSet, err = mysql.ParseMysqlGTIDSet(gtid)
	if err != nil {
		log.Error("解析GTID 异常 ", err)
	}

	return pos, nil
}

// UpdatePosition 更新binlog 的offset @paras{domain, ip, gtids, binlog_file, binlog_pos, last_time, status}
func (c *MetaConf) UpdatePosition(pos *inter.BinlogPosition) {
	update := fmt.Sprintf(
		"REPLACE INTO %s.position SET domain = ?, ip = ?, gtid_sets = ?, "+
			"binlog_file = ?, binlog_pos = ?, last_time = ?, status = ?",
		c.Db)

	log.Info("update meta ", update)

	c.RefreshConnection()
	defer c.Close()

	_, err := c.Conn.Exec(update, pos.Domain, pos.IP,
		pos.GTIDSet.String(), pos.BinlogFile, pos.BinlogPos,
		pos.Timestamp, pos.Status)
	if err != nil {
		log.Error(err)
	}
}

// oneRstQuery 查询仅仅返回一行一列结果
func (c *MetaConf) oneRstQuery(sql string) string {
	rst, err := c.Conn.Query(sql)
	if err != nil {
		final.Terminate(err, nil)
	}
	defer rst.Close()

	var value string
	for rst.Next() {
		rst.Scan(&value)
	}

	return value
}

// GetBinlogPath 根据MySQL版本查询本机 binlog 文件位置
func (c *MetaConf) GetBinlogPath() string {
	c.RefreshConnection()
	defer c.Close()

	version := c.oneRstQuery("SELECT version()")

	// 前两个版本号
	switch version[0:strings.LastIndex(version, ".")] {
	case "5.5":
		return c.oneRstQuery("SELECT @@global.datadir")
	default: // 5.5以上版本
		prefix := c.oneRstQuery("SELECT @@global.log_bin_basename")
		return prefix[0:strings.LastIndex(prefix, "/")]
	}
}

// GetMaxAllowedPackage get max allowed package for the max transaction size limitation
func (c *MetaConf) GetMaxAllowedPackage() int64 {
	c.RefreshConnection()

	rst := c.oneRstQuery("select @@max_allowed_packet")
	size, err := strconv.ParseInt(rst, 10, 64)
	if err != nil {
		final.Terminate(err, nil)
	}

	return size
}

// UpdateMeta update Meta in case of error occur
func (c *MetaConf) UpdateMeta(Pos *inter.BinlogPosition, status, msg string) {
	if len(c.Host) == 0 {
		log.Info("meta is not initiated yet")
		return
	}

	url := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", c.User, c.Password, c.Host, c.Port, c.Db)
	conn, err := sql.Open("mysql", url)
	if err != nil {
		log.Error("connect meta error ", err)
		return
	}

	update := `REPLACE INTO position SET domain = ?, ip = ?, gtid_sets = ?,
		binlog_file = ?, binlog_pos = ?, last_time = ?, status = ?, message = ?`

	log.Info("update meta ", update)

	if _, err = conn.Exec(update, Pos.Domain, Pos.IP,
		Pos, Pos.BinlogFile, Pos.BinlogPos,
		Pos.Timestamp, status, msg); err != nil {
		log.Error(err)
	}
}

// Copy Meta config
func (c *MetaConf) Copy() *MetaConf {
	return &MetaConf{
		Host:     c.Host,
		IP:       c.IP,
		Port:     c.Port,
		Db:       c.Db,
		User:     c.User,
		Password: c.Password,
	}
}

// Begin start begin
func (c *MetaConf) Begin() error {
	log.Debug("begin")
	tx, err := c.Conn.Begin()
	if err != nil {
		log.Error(err)
		return err
	}
	c.Tx = tx
	return nil
}

// Execute execute binlog statement byte type for string is too big exhaust much memory
func (c *MetaConf) Execute(bins []byte) error {
	log.Debug("execute binlog statement ", " exeucte size ", len(bins))

	if _, err := c.Tx.Exec(string(bins)); err != nil {
		log.Error(err)
		return err
	}
	return nil
}

// Commit commit transaction
func (c *MetaConf) Commit() error {
	log.Debug("commit")
	return c.Tx.Commit()
}

// hasGTID has GTIDSet open
func (c *MetaConf) HasGTID() (bool, error) {
	c.RefreshConnection()

	rst, err := c.Conn.Query("show variables like \"%gtid_mode%\"")
	if err != nil {
		log.Error(err)
		return false, err
	}
	defer rst.Close()

	var mode, val string

	for rst.Next() {
		rst.Scan(&mode, &val)
	}

	if strings.EqualFold(val, "ON") {
		return true, nil
	}

	return false, nil
}

// Master status
func (c *MetaConf) MasterStatus() (*meta.Offset, error) {
	c.RefreshConnection()

	rst, err := c.Conn.Query("show master status")
	if err != nil {
		log.Error(err)
		return nil, err
	}
	defer rst.Close()

	var f, pos, doDB, igDB, gtid string

	for rst.Next() {
		rst.Scan(&f, &pos, &doDB, &igDB, &gtid)
	}

	g, err := mysql.ParseMysqlGTIDSet(gtid)
	if err != nil {
		return nil, err
	}

	p, err := strconv.Atoi(pos)
	if err != nil {
		log.Errorf("show master status error for binlog position %v", err)
		return nil, err
	}

	off := &meta.Offset{
		IntGtid: []byte(g.String()),
		SinGtid: []byte(g.String()),
		BinFile: f,
		BinPos:  uint32(p),
	}

	return off, nil
}
