package conf

import (
	"bufio"
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
)

// IsCoordinateSQL
func IsCoordinateSQL(sql []byte) (bool, error) {
	f, err := os.Open(cordName)
	if err != nil {
		log.Errorf("open file {%s} error{%v}", cordName, err)
		return false, err
	}
	defer f.Close()

	var regs []*regexp.Regexp

	buff := bufio.NewReader(f)
	for {
		line, err := buff.ReadString('\n')
		if err != nil && err != io.EOF {
			log.Errorf("read line {%s} error {%v}", line, err)
			return false, err
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		regs = append(regs, regexp.MustCompile(line))

		if err == io.EOF {
			break
		}
	}

	for _, re := range regs {
		if re.Match(sql) {
			log.Infof("sql {%s} match regular {%s} success", sql, re.String())
			return true, nil
		}
	}

	return false, nil
}

// GetTables according to sql type
func GetTables(ddl []byte) (string, error) {
	stmt, err := sqlparser.Parse(string(ddl))
	if err != nil {
		log.Errorf("parse ddl %s error{%v}", ddl, err)
		return "", err
	}

	switch st := stmt.(type) {
	case *sqlparser.DBDDL:
		return fmt.Sprintf("%s\\..*", st.DBName), nil
	}

	return "", nil
}
