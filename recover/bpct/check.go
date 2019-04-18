package bpct

import (
	"bufio"
	"encoding/json"
	"github.com/zssky/log"
	"io"
	"os"
	"regexp"

	"github.com/mysql-binlog/common/inter"
)

/***
checkpoint in case of the restart
*/

// CheckPoint check point
type CheckPoint struct {
	Time     string          `json:"time"`     // time for checkpoint for readable
	Table    string          `json:"table"`    // table name
	FileName *inter.FileName `json:"filename"` // file name
	Offset   int64           `json:"offset"`   // file offset
}

// ReadCheckPoint read check point
func ReadCheckPoint(f string) (map[string]*CheckPoint, error) {
	// checkpoint maps
	cms := make(map[string]*CheckPoint)

	if _, err := os.Stat(f); os.IsNotExist(err) {
		log.Warn("no config file exist ")
		return cms, nil
	}

	fi, err := os.OpenFile(f, os.O_RDONLY, inter.FileMode)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	defer fi.Close()

	reg := regexp.MustCompile("\\s+")

	br := bufio.NewReader(fi)
	for {
		// read from first then overwrite before
		a, _, c := br.ReadLine()
		if c == io.EOF {
			break
		}

		log.Debug("line ===", string(a), "===")

		if len(a) == 0 || len(reg.ReplaceAllString(string(a), "")) == 0 {
			continue
		}

		// checkpoint
		cp := &CheckPoint{}
		if err := json.Unmarshal(a, cp); err != nil {
			log.Error(err)
			return nil, err
		}

		cms[cp.Table] = cp
	}

	return cms, nil
}

// Write write checkpoint to json byte into writer
func (c *CheckPoint) Write(w io.Writer) error {
	b, err := json.Marshal(c)
	if err != nil {
		log.Error(err)
		return err
	}

	if _, err = w.Write(b); err != nil {
		return err
	}
	// write enter
	_, err = w.Write([]byte("\n"))
	return err
}
