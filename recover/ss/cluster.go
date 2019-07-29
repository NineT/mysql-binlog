package ss

import (
	"fmt"
	"github.com/mysql-binlog/siddontang/go/log"
	"io/ioutil"
	"regexp"
	"strings"
)

/**
* cluster for filter needing tables
*/

// Cluster for path and id
type Cluster struct {
	path string // files path
	id   int64  // cluster id
}

// NewCluster for cluster client
func NewCluster(base string, id int64) *Cluster {
	if strings.HasSuffix(base, "/") {
		base = strings.TrimSuffix(base, "/")
	}

	return &Cluster{
		path: fmt.Sprintf("%s/%d", base, id),
		id:   id,
	}
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

	ns, err := ioutil.ReadDir(c.path)
	if err != nil {
		log.Errorf("read directory on path{%s} error{%v}", c.path, err)
		return nil, err
	}

	// tables
	var tbs []string
	for _, n := range ns {
		part := strings.Split(n.Name(), ".")

		if len(part) != 2 {
			// part
			continue
		}

		db := part[0]
		tb := part[1]

		if len(db) == 0 || len(tb) == 0 {
			log.Warnf("not comply with db.table formation %s", n.Name())
			continue
		}

		if dbReg.Match([]byte(db)) && tbReg.Match([]byte(tb)) {
			tbs = append(tbs, n.Name())
		}
	}

	if len(tbs) == 0 {
		log.Warnf("empty tables found on path{%s}", c.path)
		// empty tables found for reg
		return []string{}, nil
	}

	return tbs, nil
}
