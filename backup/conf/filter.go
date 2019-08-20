package conf

import (
	"bufio"
	"io"
	"os"
	"regexp"
	"strings"

	"github.com/zssky/log"
)

/**
* filter ddl no need to apply to MySQL
* eg. create / drop trigger
*/
const (
	filterName = "conf/filter.reg"
)

// IsFilteredSQL to filter
func IsFilteredSQL(ddl []byte) (bool, error) {
	f, err := os.Open(filterName)
	if err != nil {
		log.Errorf("open file {%s} error{%v}", filterName, err)
		return false, err
	}
	defer f.Close()

	var regs []*regexp.Regexp

	buff := bufio.NewReader(f)
	for {
		line, err := buff.ReadString('\n')
		if err != nil && io.EOF != err {
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
		if re.Match(ddl) {
			log.Infof("sql {%s} match regular {%s} success", ddl, re.String())
			return true, nil
		}
	}

	return false, nil
}
