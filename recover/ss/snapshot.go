package ss

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
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

	mysqlConf = "/export/servers/mysql/etc/my.cnf"

	mysqlServer = "/export/servers/mysql/support-files"

	snapshotOffset = "so.index"
)

// Snapshot using snapshot and binlog files
type Snapshot struct {
	base      string `json:"base"`      // base path here default is /mysql_backup
	clusterID int64  `json:"clusterid"` // cluster id for snapshot
	timestamp int64  `json:"timestamp"` // timestamp to recover
}

// NewSnapshot for recover
func NewSnapshot(base string, clusterID, timestamp int64) *Snapshot {
	if strings.HasSuffix(base, "/") {
		base = strings.TrimSuffix(base, "/")
	}

	return &Snapshot{
		base:      fmt.Sprintf("%s/%d", base, clusterID),
		clusterID: clusterID,
		timestamp: timestamp,
	}
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

// Copy data from original data on cfs to another timestamp
func (s *Snapshot) Copy() error {
	// src file
	src, err := s.latestSnapshot()
	if err != nil {
		return err
	}

	dst := fmt.Sprintf("%s/%s%d", s.base, snapshotPrefix, s.timestamp)
	log.Debug("copy data from source {%s} to dst{%s} ", src, dst)

	c := fmt.Sprintf("cp -R %s %s", src, dst)
	log.Debugf("execute shell command %s", c)

	if _, _, err := utils.ExeShell(c); err != nil {
		return err
	}
	return nil
}

// ModifyConf change data directory and replace server-id
func (s *Snapshot) ModifyConf() error {
	// newly path for current timestamp
	path := fmt.Sprintf("%s/%s%d", s.base, snapshotPrefix, s.timestamp)

	path = strings.Replace(path, "/", "\\/", 100)

	// datadir replace command
	dc := fmt.Sprintf("sed -i's/datadir.*/datadir=%s/g' %s", path, mysqlConf)

	log.Infof("execute shell command %s", dc)
	if _, _, err := utils.ExeShell(dc); err != nil {
		return err
	}

	// server-id replace command
	sc := fmt.Sprintf("sed -i 's/server-id.*/server-id	= %d/g' %s", rand.Uint32(), mysqlConf)
	log.Infof("execute shell command %s", sc)
	if _, _, err := utils.ExeShell(sc); err != nil {
		return err
	}

	log.Infof("modify config success")
	return nil
}

// StartMySQL if data is ready
func (s *Snapshot) StartMySQL() error {
	c := fmt.Sprintf("%s/mysql.server start", mysqlServer)
	o, e, err := utils.ExeShell(c)
	if err != nil {
		return err
	}
	log.Infof("out %s, err %s", o, e)
	return nil
}

// Offset under snapshot
func (s *Snapshot) Offset() (*meta.Offset, error) {
	f := fmt.Sprintf("%s/%s", s.base, snapshotOffset)
	log.Infof("snapshot offset index file %s", f)
	bt, err := inter.LastLine(f)
	if err != nil {
		log.Errorf("read last line on file{%s} error{%v}", f, err)
		return nil, err
	}

	var m *meta.Offset
	if err := json.Unmarshal(bt, m); err != nil {
		log.Errorf("unmarshal data {%s} error{%v}", string(bt), err)
		return nil, err
	}

	return m, nil
}
