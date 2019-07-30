package ss

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/zssky/log"

	"github.com/mysql-binlog/common/inter"
	"github.com/mysql-binlog/common/meta"

	"github.com/mysql-binlog/recover/utils"
)

/***
* snapshots
* @Attention: snapshot files format eg: snapshot_timestamp
* binlog to apply using different MySQL connection according to timestamp
* 1. find the right binlog files
* 2. locate gtid then using the right gtid for recover using snapshot
* 3. locate total gtid offset
*/

const (
	snapshotPrefix = "snapshot_"

	mysqlConf = "/export/servers/mysql/etc/"

	snapshotOffset = "so.index"
)

// Snapshot using snapshot and binlog files
type Snapshot struct {
	base      string // base path here default is /mysql_backup
	clusterID int64  // cluster id for snapshot
	src       string // src snapshot for the right timestamp
	timestamp int64  // timestamp to recover
}

// NewSnapshot for recover
func NewSnapshot(base string, clusterID, timestamp int64) (*Snapshot, error) {
	if strings.HasSuffix(base, "/") {
		base = strings.TrimSuffix(base, "/")
	}

	s := &Snapshot{
		base:      fmt.Sprintf("%s/%d", base, clusterID),
		clusterID: clusterID,
		timestamp: timestamp,
	}

	f, err := s.latestSnapshot()
	if err != nil {
		return nil, err
	}

	s.src = f
	return s, nil
}

// ID for snapshot
func (s *Snapshot) ID() string {
	return fmt.Sprintf("%s/%s%d", s.base, snapshotPrefix, s.timestamp)
}

// latestSnapshot to find
func (s *Snapshot) latestSnapshot() (string, error) {
	fs, err := ioutil.ReadDir(s.base)
	if err != nil {
		log.Errorf("read dir %s error {%v}", s.base, err)
		return "", err
	}

	mx := s.timestamp
	// range files
	for _, f := range fs {
		n := f.Name()
		if strings.HasPrefix(n, snapshotPrefix) {
			ts := strings.TrimPrefix(n, snapshotPrefix)
			t, err := strconv.ParseInt(ts, 10, 64)
			if err != nil {
				log.Errorf("parse int{%s} error {%v}", ts, err)
				return "", err
			}

			// using max timestamp distance
			dist := s.timestamp - t
			if dist > 0 && mx > dist {
				mx = dist
			}
		}
	}

	// error for max value not have changed then means no snapshot get error
	if mx == s.timestamp {
		err := fmt.Errorf("no snapshot get from directory{%s}", s.base)
		log.Error(err)
		return "", err
	}

	// return absolute file path with no error
	return fmt.Sprintf("%s/%s%d", s.base, snapshotPrefix, s.timestamp-mx), nil
}

// CopyData data from original data on cfs to another timestamp
func (s *Snapshot) CopyData() error {
	log.Debugf("copy data from source {%s} to dst{%s} ", s.src, "/export/")

	c := fmt.Sprintf("cp -R %s/* %s", s.src, "/export/")
	log.Debugf("execute shell command %s", c)

	if _, _, err := utils.ExeShell(c); err != nil {
		return err
	}
	return nil
}

// CopyConf change data directory and replace server-id
func (s *Snapshot) CopyConf() error {
	// newly path for current timestamp
	// copy my.cnf to the right path
	cp := fmt.Sprintf("cp %s/my.cnf %s", s.base, mysqlConf)
	log.Infof("execute shell command %s", cp)
	if _, _, err := utils.ExeShell(cp); err != nil {
		return err
	}

	log.Infof("modify config success")
	return nil
}

// Auth grant all auth to file to mysql:mysql
func (s *Snapshot) Auth() error {
	c := "chown -R mysql:mysql /export/"
	o, e, err := utils.ExeShell(c)
	if err != nil {
		return err
	}
	log.Infof("out %s, err %s", o, e)
	return nil
}

// StartMySQL if data is ready
func (s *Snapshot) StartMySQL() error {
	c := fmt.Sprintf("/export/servers/mysql/bin/mysqld_safe --defaults-file=/export/servers/mysql/etc/my.cnf --user=mysql &", )
	o, e, err := utils.ExeShell(c)
	if err != nil {
		return err
	}
	log.Infof("out %s, err %s", o, e)
	return nil
}

// Offset under snapshot
func (s *Snapshot) Offset() (*meta.Offset, error) {
	f := fmt.Sprintf("%s/%s", s.src, snapshotOffset)
	log.Infof("snapshot offset index file %s", f)

	bt, err := inter.LastLine(f)
	if err != nil {
		log.Errorf("read last line on file{%s} error{%v}", f, err)
		return nil, err
	}

	o := &meta.Offset{}
	if err := json.Unmarshal(bt, o); err != nil {
		log.Errorf("unmarshal data {%s} error{%v}", string(bt), err)
		return nil, err
	}

	return o, nil
}

// FlushOffset merged offset to directory
func (s *Snapshot) FlushOffset(o *meta.Offset) error {
	c := fmt.Sprintf("%s/%s%d/%s", s.base, snapshotPrefix, s.timestamp, snapshotOffset)

	log.Infof("flush newly offset to %s", snapshotOffset)

	f, err := os.OpenFile(c, os.O_CREATE|os.O_RDWR|os.O_TRUNC, os.FileMode(0666))
	if err != nil {
		log.Errorf("open file{%s} error{%v}", c, err)
		return err
	}
	defer f.Close()

	bt, err := json.Marshal(o)
	if err != nil {
		log.Errorf("marshal json{%v} error{%v}", o, err)
		return err
	}

	if _, err := f.Write(bt); err != nil {
		log.Errorf("write data{%s} to file{%s} error{%v}", string(bt), c, err)
		return err
	}

	return nil

}
