package res

import (
	"fmt"
	"github.com/zssky/log"

	"github.com/mysql-binlog/siddontang/go-mysql/mysql"
)

// LogPosition for
type LogPosition struct {
	executed  mysql.GTIDSet // executed gtid
	timestamp uint32        // executed binlog timestamp
}

// NewLogPosition for one table
func NewLogPosition(gtid string, t uint32) (*LogPosition, error) {
	mg, err := mysql.ParseMysqlGTIDSet(gtid)
	if err != nil {
		log.Errorf("parse executed gtid{%s} error{%v}", gtid, err)
		return nil, err
	}

	return &LogPosition{
		executed:  mg,
		timestamp: t,
	}, nil
}

// Update for newly coming gtid
func (l *LogPosition) UpdateGTID(gtid string) error {
	if err := l.executed.Update(gtid); err != nil {
		log.Errorf("merge gtid{%s} into gtid{%s} error{%v}", gtid, l.executed.String(), err)
		return err
	}

	return nil
}

// Update time
func (l *LogPosition) UpdateTime(t uint32) {
	l.timestamp = t
}

// String for log postion
func (l *LogPosition) String() string {
	return fmt.Sprintf("executed gtid set %s, executed timestamp %d", l.executed.String(), l.timestamp)
}
