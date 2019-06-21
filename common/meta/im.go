package meta

import (
	"github.com/mysql-binlog/siddontang/go-mysql/mysql"
	"github.com/mysql-binlog/siddontang/go/log"
)

// Offset binlog offset write to meta
type Offset struct {
	ClusterID int64  `json:"clusterid"` // cluster id
	IntGtid   []byte `json:"intgtid"`   // integrate gtid equals to gtid that merged the next eg. 2d784ad8-8f7a-4916-858e-d7069e5a24b2:1-30000
	SinGtid   []byte `json:"singtid"`   // single gtid equals to gtid exists on gtid event  eg. 2d784ad8-8f7a-4916-858e-d7069e5a24b2:100
	Time      uint32 `json:"time"`      // timestamp
	BinFile   string `json:"file"`      // binlog File
	BinPos    uint32 `json:"pos"`       // binlog position
	Counter   int    `json:"-"`         // counter
	Header    bool   `json:"-"`         // header flag
}

// LessEqual whether the o{mean the current offset} is <= another offset
func LessEqual(o1, o2 *Offset) (bool, error) {
	// if process is crashed
	if o1.Time < o2.Time {
		return true, nil
	}

	if o1.Time > o2.Time {
		return false, nil
	}

	g1, err := mysql.ParseMysqlGTIDSet(string(o1.IntGtid))
	if err != nil {
		log.Warnf("gtid {%s} format error %v", string(o1.IntGtid), err)
		return false, err
	}

	g2, err := mysql.ParseMysqlGTIDSet(string(o2.IntGtid))
	if err != nil {
		log.Warnf("gtid {%s} format error %v", string(o2.IntGtid), err)
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
	// Read meta offset from meta storage
	Read(k interface{}) (*Offset, error)

	// Save node to storage
	Save(offset *Offset) error

	// Delete node from storage
	Delete(k interface{}) error
}
