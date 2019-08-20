package log

import (
	"compress/zlib"
	"fmt"
	"github.com/mysql-binlog/common/inter"
	"github.com/zssky/log"
	"io"
	"os"
	"strconv"
	"strings"
)

/***
file list to record the log files which exists on the directory named as list
*/

const (
	logList = "list"
)

// LogList for binlog files as the index for the last one
type LogList struct {
	Dir      string    // directory for log list file
	Name     string    // log list file name
	FullName string    // full name for list file
	f        *os.File  // WriteEvent WriteEvent 生成binlog
	iw       io.Writer // iw io writer
}

// ListExist check for log list file
func ListExists(dir string) bool {
	dir = strings.TrimSuffix(dir, "/")
	return inter.Exists(fmt.Sprintf("%s/%s", dir, logList))
}

// RecoverList for log list
func RecoverList(dir string) (*LogList, error) {
	dir = strings.TrimSuffix(dir, "/")

	l := &LogList{
		Dir:      dir,
		Name:     logList,
		FullName: fmt.Sprintf("%s/%s", dir, logList),
	}

	f, err := os.OpenFile(l.FullName, os.O_APPEND|os.O_RDWR, inter.FileMode)
	if err != nil {
		log.Errorf("open file{%s} error{%v}", l.FullName, err)
		return nil, err
	}

	l.f = f  // set file
	l.iw = f // set file writer
	if false { // 开启压缩
		// zlib 压缩数据
		iw := zlib.NewWriter(f)

		// set writer
		l.iw = iw
	}

	return l, nil
}

// NewLogList for log writer
func NewLogList(dir string) (*LogList, error) {
	dir = strings.TrimSuffix(dir, "/")
	l := &LogList{
		Dir:      dir,
		Name:     logList,
		FullName: fmt.Sprintf("%s/%s", dir, logList),
	}

	f, err := inter.CreateFile(l.FullName)
	if err != nil {
		log.Errorf("create file{%s} error{%v}", l.FullName, err)
		return nil, err
	}

	l.f = f  // set file
	l.iw = f // set file writer
	if false { // 开启压缩
		// zlib 压缩数据
		iw := zlib.NewWriter(f)

		// set writer
		l.iw = iw
	}

	return l, nil
}

// Write data to log file list
func (l *LogList) Write(lf []byte) error {
	if _, err := l.iw.Write(lf); err != nil {
		log.Errorf("write data to log file list{%s} error{%v}", l.FullName, err)
		return err
	}

	if _, err := l.iw.Write([]byte("\n")); err != nil {
		log.Errorf("write enter to log file list{%s} error{%v}", l.FullName, err)
		return err
	}

	return nil
}

// Close all
func (l *LogList) Close() {
	f := l.f
	if f != nil {
		if err := f.Close(); err != nil {
			log.Warnf("close log list file{%s} error{%v}", l.FullName, err)
		}
	}
	l.f = nil
	l.iw = nil
}

// LastLine line data
func (l *LogList) Tail() (int64, error) {
	b, err := inter.LastLine(l.FullName)
	if err != nil {
		log.Errorf("read file{%s} tail error{%v}", l.FullName, err)
		return 0, err
	}

	if b == "" {
		// file is empty
		log.Warnf("file {%s} is empty", l.FullName)
		return 0, nil
	}

	t, err := strconv.ParseInt(strings.TrimSuffix(string(b), LogSuffix), 10, 64)
	if err != nil {
		log.Errorf("parse int{%s} error{%v}", strings.TrimSuffix(string(b), LogSuffix), err)
		return 0, err
	}

	return t, nil
}
