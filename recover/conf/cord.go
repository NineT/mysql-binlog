package conf

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"

	"github.com/xwb1989/sqlparser"
	"github.com/zssky/log"
)

/**
* sql must coordinate with all tables to recover and if and only if can it execute once.
* eg. RENAME TABLE `b2b_trade100`.`b2b_order_main` TO `b2b_trade100`.`_b2b_order_main_old`, `b2b_trade100`.`_b2b_order_main_new` TO `b2b_trade100`.`b2b_order_main`
*/

const (
	cordName = "conf/cord.reg"

	cordLevelDB = "DB"

	cordLevelAll = "ALL"
)

// Cord for table level
type Cord struct {
	Reg   string         `json:"reg"`   // regular
	Level string         `json:"level"` // level including DB, All etc.
	reg   *regexp.Regexp `json:"-"`
}

// IsCoordinateSQL
func IsCoordinateSQL(sql []byte) (bool, string, error) {
	f, err := os.Open(cordName)
	if err != nil {
		log.Errorf("open file {%s} error{%v}", cordName, err)
		return false, "", err
	}
	defer f.Close()

	var cords []*Cord

	buff := bufio.NewReader(f)
	for {
		line, err := buff.ReadString('\n')
		if err != nil && err != io.EOF {
			log.Errorf("read line {%s} error {%v}", line, err)
			return false, "", err
		}

		line = strings.TrimSpace(line)
		if line != "" {
			cord := &Cord{}
			if err := json.Unmarshal([]byte(line), cord); err != nil {
				log.Errorf("unmarshal data {%s}error {%v}", line, err)
				return false, "", err
			}

			cord.reg = regexp.MustCompile(cord.Reg)
			cord.Level = strings.ToUpper(cord.Level)
			cords = append(cords, cord)
		}

		if err == io.EOF {
			break
		}
	}

	for _, co := range cords {
		reg := regexp.MustCompile(co.Reg)
		if reg.Match(sql) {
			log.Infof("sql {%s} match regular {%s} success", sql, reg.String())

			stmt, err := sqlparser.Parse(string(sql))
			if err != nil {
				log.Errorf("parse ddl %s error{%v}", sql, err)
				return false, "", err
			}

			switch co.Level {
			case cordLevelDB:
				switch st := stmt.(type) {
				case *sqlparser.DBDDL:
					return true, fmt.Sprintf("%s\\..*", st.DBName), nil
				}
			}

			// all tables are matched
			return true, ".*\\..*", nil
		}
	}
	return false, "", nil
}
