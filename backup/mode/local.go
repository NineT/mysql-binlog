package mode

import (
	"fmt"
	"strings"

	"github.com/mysql-binlog/siddontang/go-mysql/replication"

	"github.com/mysql-binlog/common/final"
	"github.com/mysql-binlog/common/inter"
)

// LocalMode dump binlog from reading local binlog file
type LocalMode struct {
	Path      string // binlog文件的路径
	LatestPos *inter.BinlogPosition
}

// Handle handle reading binlog file
func (m *LocalMode) Handle(f func(ev *replication.BinlogEvent) bool, a *final.After) {
	defer func() {
		a.After()
	}()

	parser := replication.NewBinlogParser()

	// true: means continue, false: means arrive the final time
	flag := true

	onEventFunc := func(e *replication.BinlogEvent) error {
		flag = f(e)

		switch e.Header.EventType {
		case replication.ROTATE_EVENT:
			if e.Header.LogPos != 0 {
				re := e.Event.(*replication.RotateEvent)
				m.LatestPos.BinlogPos = uint32(re.Position)
				m.LatestPos.BinlogFile = string(re.NextLogName)
			}
		}
		return nil
	}

	preFile := m.LatestPos.BinlogFile

	for flag {
		select {
		case e := <-a.Errs:
			panic(e)
			return
		default:
			file := m.Path + "/" + m.LatestPos.BinlogFile
			if err := parser.ParseFile(file, int64(m.LatestPos.BinlogPos), onEventFunc); err != nil {
				panic(err)
				return
			}

			if strings.Compare(preFile, m.LatestPos.BinlogFile) == 0 {
				// binlog file name is not changed
				panic(fmt.Errorf("binlog file still the same %s", f))
				return
			}

			preFile = m.LatestPos.BinlogFile
		}
	}
}
