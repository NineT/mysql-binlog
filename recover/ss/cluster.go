package ss

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"regexp"
	"strings"

	"github.com/zssky/log"

	"github.com/mysql-binlog/common/inter"
	clog "github.com/mysql-binlog/common/log"
)

/**
* cluster for filter needing tables
*/

// Cluster for path and id
type Cluster struct {
	mode string // execute mode
	path string // files path
	id   int64  // cluster id
}

// NewCluster for cluster client
func NewCluster(base, mode string, id int64) *Cluster {
	if strings.HasSuffix(base, "/") {
		base = strings.TrimSuffix(base, "/")
	}

	return &Cluster{
		mode: mode,
		path: fmt.Sprintf("%s/%d", base, id),
		id:   id,
	}
}

// CheckTime is before the current max time: true => some table timestamp > parameter timestamp, error => handle error on process
func (c *Cluster) CheckTime(ts int64) (bool, error) {
	switch c.mode {
	case inter.Separated:
		return c.checkSeparated(ts)
	case inter.Integrated:
		return c.checkIntegrated(ts)
	}
	return false, fmt.Errorf("mode %s no in {%s, %s}", c.mode, inter.Separated, inter.Integrated)
}

// checkIntegrated
func (c *Cluster) checkIntegrated(ts int64) (bool, error) {
	dirs, err := ioutil.ReadDir(c.path)
	if err != nil {
		log.Errorf("read directory on path{%s} error{%v}", c.path, err)
		return false, err
	}

	// table exists
	exists := false
	for _, d := range dirs {
		if strings.HasSuffix(d.Name(), string(inter.LogSuffix)) {
			exists = true
		}
	}

	if !exists {
		// no log file exist
		return false, nil
	}

	// read table log list last line
	l := fmt.Sprintf("%s/list", c.path)
	n, err := inter.LastLine(l)
	if err != nil {
		log.Errorf("read list file {%s} last line error {%v}", l, err)
		return false, err
	}

	// timestamp
	t := strings.TrimSuffix(strings.TrimSpace(string(n)), string(inter.LogSuffix))

	// read last log name index offset
	i := fmt.Sprintf("%s/%s.index", c.path, t)
	o, err := inter.LastLine(i) // offset
	if err != nil {
		log.Errorf("read index file{%s} last line error{%v}", i, err)
		return false, err
	}

	// take newly offset
	off := &clog.IndexOffset{}
	if err := json.Unmarshal([]byte(o), off); err != nil {
		log.Errorf("unmarshal json{%s} error %v", string(o), err)
		return false, err
	}

	// local timestamp
	if int64(off.Local.Time) >= ts {
		log.Warnf("exist file {%s} timestamp {%d} >= parameter timestamp{%d}", i, off.Local.Time, ts)
		return true, nil
	}

	// no table timestamp > parameter timestamp, and return nil
	return false, nil
}

// CheckSeparated
func (c *Cluster) checkSeparated(ts int64) (bool, error) {
	dirs, err := ioutil.ReadDir(c.path)
	if err != nil {
		log.Errorf("read directory on path{%s} error{%v}", c.path, err)
		return false, err
	}

	// table exists
	exists := false
	for _, d := range dirs {
		part := strings.Split(d.Name(), ".")

		if len(part) != 2 {
			// part
			continue
		}

		exists = true
	}

	if !exists {
		// no tables exists then return
		return false, nil
	}

	for _, d := range dirs {
		part := strings.Split(d.Name(), ".")

		if len(part) != 2 {
			// part
			continue
		}

		// read table log list last line
		l := fmt.Sprintf("%s/%s/list", c.path, d.Name())
		n, err := inter.LastLine(l)
		if err != nil {
			log.Errorf("read list file {%s} last line error {%v}", l, err)
			return false, err
		}

		// timestamp
		t := strings.TrimSuffix(strings.TrimSpace(string(n)), string(inter.LogSuffix))

		// read last log name index offset
		i := fmt.Sprintf("%s/%s/%s.index", c.path, d.Name(), t)
		o, err := inter.LastLine(i) // offset
		if err != nil {
			log.Errorf("read index file{%s} last line error{%v}", i, err)
			return false, err
		}

		// take newly offset
		off := &clog.IndexOffset{}
		if err := json.Unmarshal([]byte(o), off); err != nil {
			log.Errorf("unmarshal json{%s} error %v", string(o), err)
			return false, err
		}

		// local timestamp
		if int64(off.Local.Time) >= ts {
			log.Warnf("exist file {%s} timestamp {%d} >= parameter timestamp{%d}", i, off.Local.Time, ts)
			return true, nil
		}
	}
	// no table timestamp > parameter timestamp, and return nil
	return false, nil
}

// GetClusterPath for outside using
func (c *Cluster) GetClusterPath() string {
	return c.path
}

// SelectTables according to db reg and table reg, if none then return error
func (c *Cluster) SelectTables(db, tb string) ([]string, error) {
	log.Infof("lower case of db{%s} and tb{%s}", db, tb)
	dbReg := regexp.MustCompile(strings.ToLower(db))
	tbReg := regexp.MustCompile(strings.ToLower(tb))

	dirs, err := ioutil.ReadDir(c.path)
	if err != nil {
		log.Errorf("read directory on path{%s} error{%v}", c.path, err)
		return nil, err
	}

	// tables
	var tbs []string
	for _, d := range dirs {
		part := strings.Split(d.Name(), ".")

		if len(part) != 2 {
			// part
			continue
		}

		db := part[0]
		tb := part[1]

		if len(db) == 0 || len(tb) == 0 {
			log.Warnf("not comply with db.table formation %s", d.Name())
			continue
		}

		if dbReg.Match([]byte(db)) && tbReg.Match([]byte(tb)) {
			tbs = append(tbs, d.Name())
		}
	}

	if len(tbs) == 0 {
		log.Warnf("empty tables found on path{%s}", c.path)
		// empty tables found for reg
		return []string{}, nil
	}

	return tbs, nil
}
