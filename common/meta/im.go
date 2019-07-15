package meta

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/mysql-binlog/siddontang/go-mysql/mysql"
	"github.com/zssky/log"
)

// DbMeta including init offset and instance information
type DbMeta struct {
	Off  *Offset   `json:"offset"`   // offset
	Inst *Instance `json:"instance"` // instance
}

// Offset binlog offset write to meta
type Offset struct {
	CID      int64  `json:"clusterid"` // cluster id
	ExedGtid string `json:"exedgtid"`  // executed gtid equals to gtid that merged the next eg. 2d784ad8-8f7a-4916-858e-d7069e5a24b2:1-30000
	TrxGtid  string `json:"trxgtid"`   // transaction gtid equals to gtid exists on gtid event  eg. 2d784ad8-8f7a-4916-858e-d7069e5a24b2:100
	Time     uint32 `json:"time"`      // timestamp
	BinFile  string `json:"file"`      // binlog File
	BinPos   uint32 `json:"pos"`       // binlog position
	Counter  int    `json:"-"`         // counter
	Header   bool   `json:"-"`         // header flag
}

// Instance information for mysql
type Instance struct {
	CID      int64  `json:"clusterid"` // cluster id
	Host     string `json:"host"`      // MySQL host
	Port     int    `json:"port"`      // MySQL port
	User     string `json:"user"`      // mysql dump user
	Password string `json:"password"`  // mysql dump password
}

const (
	// offset set key in etcd
	OffsetKey = "offset"
)

// LessEqual whether the o{mean the current offset} is <= another offset
func LessEqual(o1, o2 *Offset) (bool, error) {
	// if process is crashed
	if o1.Time < o2.Time {
		return true, nil
	}

	if o1.Time > o2.Time {
		return false, nil
	}

	g1, err := mysql.ParseMysqlGTIDSet(o1.ExedGtid)
	if err != nil {
		log.Warnf("gtid {%s} format error %v", o1.ExedGtid, err)
		return false, err
	}

	g2, err := mysql.ParseMysqlGTIDSet(o2.ExedGtid)
	if err != nil {
		log.Warnf("gtid {%s} format error %v", o2.ExedGtid, err)
		return false, err
	}

	if g1.Equal(g2) {
		return true, nil
	}

	if g2.Contain(g1) {
		return true, nil
	}

	return false, nil
}

// Meta data interface
type IMeta interface {
	// ReadOffset meta offset from meta storage
	ReadOffset(k interface{}) (*Offset, error)

	// ReadInstance for binlog dump {including host, port, user, password}
	ReadInstance(k interface{}) (*Instance, error)

	// SaveOffset node to storage
	SaveOffset(offset *Offset) error

	// SaveInstance node to storage
	SaveInstance(ins *Instance) error

	// DeleteOffset node from storage
	DeleteOffset(k interface{}) error

	// DeleteInstance node from storage
	DeleteInstance(k interface{}) error
}

// Master status
func (i *Instance) MasterStatus() (*Offset, error) {
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
	url := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?maxAllowedPacket=0", i.User, i.Password, i.Host, i.Port, "test")
	c, err := sql.Open("mysql", url)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	rst, err := c.Query("show master status")
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

	return &Offset{
		ExedGtid: g.String(),
		TrxGtid:  g.String(),
		BinFile:  f,
		BinPos:   uint32(p),
	}, nil
}

// HasGTID check
func (i *Instance) HasGTID() bool {
	url := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?maxAllowedPacket=0", i.User, i.Password, i.Host, i.Port, "mysql")
	c, err := sql.Open("mysql", url)
	if err != nil {
		return false
	}
	defer c.Close()

	rst, err := c.Query("show variables like \"%gtid_mode%\"")
	if err != nil {
		log.Error(err)
		return false
	}
	defer rst.Close()

	var mode, val string

	for rst.Next() {
		rst.Scan(&mode, &val)
	}

	if strings.EqualFold(val, "ON") {
		return true
	}

	return false
}
