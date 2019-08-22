package ss

import (
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/zssky/log"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"

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

	mysqlServerPath = "/export/servers/mysql/support-files"

	offsetSuffix = ".index"
)

// Snapshot using snapshot and binlog files
type Snapshot struct {
	base      string // base path here default is /mysql_backup
	clusterID int64  // cluster id for snapshot
	src       string // 离时间戳 timestamp 最近的快照
	baseTime  int64  // 离时间戳 timestamp 最近的快照 生成时间
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

	f, ts, err := s.latestSnapshot()
	if err != nil {
		return nil, err
	}

	s.src = f
	s.baseTime = ts
	return s, nil
}

// ID for snapshot
func (s *Snapshot) ID() string {
	return fmt.Sprintf("%s/%s%d", s.base, snapshotPrefix, s.timestamp)
}

// latestSnapshot to find
func (s *Snapshot) latestSnapshot() (string, int64, error) {
	fs, err := ioutil.ReadDir(s.base)
	if err != nil {
		log.Errorf("read dir %s error {%v}", s.base, err)
		return "", 0, err
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
				return "", 0, err
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
		return "", 0, err
	}

	// return absolute file path with no error
	return fmt.Sprintf("%s/%s%d", s.base, snapshotPrefix, s.timestamp-mx), s.timestamp - mx, nil
}

// CopyData data from original data on cfs to another timestamp
func (s *Snapshot) CopyData() error {
	log.Debugf("copy data from source {%s} to dst{%s} ", s.src, "/export/")

	// remove index file
	c := fmt.Sprintf("rsync -chaz --exclude=*.index %s/* %s", s.src, "/export/")
	log.Debugf("execute shell command %s", c)

	if _, _, err := utils.ExeShell(c); err != nil {
		return err
	}
	return nil
}

// CopyBin change data directory and replace server-id
func (s *Snapshot) CopyBin() error {
	// newly path for current timestamp
	// copy my.cnf to the right path
	cp := fmt.Sprintf("rsync -chaz %s/servers /export/", s.base)
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

// removeRedo log
func (s *Snapshot) removeRedo() {
	// remove redo logs in case of error  find ./* -name "ib_logfile*" | xargs /bin/rm
	c := "find /export/data/mysql/* -name 'ib_logfile*' | xargs /bin/rm "
	log.Debugf("remove redo log %s", c)
	if _, _, err := utils.ExeShell(c); err != nil {
		log.Warnf("execute command %s error {%v}", c, err)
	}
}

// StartMySQL if data is ready
func (s *Snapshot) StartMySQL() error {
	if err := s.startMySQLd(); err != nil {
		log.Warnf("try again according to remove redo log")
		s.removeRedo() //
		return s.startMySQLd()
	}
	// remove redo log in case and restart again
	return nil
}

// startMySQLd
func (s *Snapshot) startMySQLd() error {
	// todo using mysqld_save must be no error out have wait the daemon process
	m := fmt.Sprintf("%s/mysql.server start", mysqlServerPath)
	if err := utils.ExecuteShellNoWait(m); err != nil {
		return err
	}

	timeout := 10
	for i := 0; i < timeout; i ++ {
		c := "netstat -anp | grep 3358 | grep LISTEN | grep mysqld | wc -l"
		ou, _, err := utils.ExeShell(c)
		if err != nil {
			return err
		}

		n, err := strconv.Atoi(strings.TrimSpace(ou))
		if err != nil {
			log.Errorf("convert out{%s} to number error{%v}", ou, err)
			return err
		}

		if n != 0 {
			return nil
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("MySQL start timeout %d seconds", 20)
}

// StopMySQL for copy data to cfs
func (s *Snapshot) StopMySQL(user, pass string) error {
	c := fmt.Sprintf("%s/mysql.server stop", mysqlServerPath)
	o, e, err := utils.ExeShell(c)
	if err != nil {
		return err
	}
	log.Infof("out %s, err %s", o, e)
	return nil
}

// Offset under snapshot
func (s *Snapshot) Offset() (*meta.Offset, error) {
	f := fmt.Sprintf("%s/%d%s", s.src, s.baseTime, offsetSuffix)
	log.Infof("snapshot offset index file %s", f)

	bt, err := inter.LastLine(f)
	if err != nil {
		log.Errorf("read last line on file{%s} error{%v}", f, err)
		return nil, err
	}

	o := &meta.Offset{}
	if err := json.Unmarshal([]byte(bt), o); err != nil {
		log.Errorf("unmarshal data {%s} error{%v}", string(bt), err)
		return nil, err
	}

	return o, nil
}

// Copy2Cfs copy data to cfs
func (s *Snapshot) Copy2Cfs() error {
	sp := fmt.Sprintf("%s/%s%d", s.base, snapshotPrefix, s.timestamp)

	c := fmt.Sprintf("mkdir -p %s && rsync -chaz /export/data %s", sp, sp)
	o, e, err := utils.ExeShell(c)
	if err != nil {
		return err
	}
	log.Infof("out %s, err %s", o, e)

	if err := s.sync(); err != nil {
		log.Infof("sync error {%v}", err)
	}

	return nil
}

// Sync data in buffer into disk
func (s *Snapshot) sync() error {
	o, e, err := utils.ExeShell("sync")
	if err != nil {
		return err
	}
	log.Infof("out %s, err %s", o, e)
	return nil
}

// FlushOffset merged offset to directory
func (s *Snapshot) FlushOffset(o *meta.Offset) error {
	c := fmt.Sprintf("%s/%s%d/%d%s", s.base, snapshotPrefix, s.timestamp, s.timestamp, offsetSuffix)

	log.Infof("flush newly offset{%v} to file{%s}", o, c)

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

	return f.Sync()
}

// RemoveData
func (s *Snapshot) RemoveData() error {
	c := "/bin/rm -rf /export/*.index /export/servers /export/data"
	o, e, err := utils.ExeShell(c)
	if err != nil {
		return err
	}
	log.Infof("out %s, err %s", o, e)
	return nil
}
