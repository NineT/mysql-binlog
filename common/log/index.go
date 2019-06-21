package log

import (
	"bytes"
	"container/list"
	"encoding/json"
	"fmt"
	"github.com/mysql-binlog/common/inter"
	"io"
	"os"

	"github.com/zssky/log"

	"github.com/mysql-binlog/common/meta"
)

/***
each one binlog file have one binlog index
index file is for gtid -> binlog offset and timestamp etc.
*/

const (
	// BinlogIndexFile const name
	BinlogIndexFile = "bin.index"
)

// IndexOffset including origin MySQL binlog offset and generated binlog offset
type IndexOffset struct {
	Dump  *meta.Offset `json:"dump"`  // origin MySQL dump offset
	Local *meta.Offset `json:"local"` // generated local binlog offset
}

// IndexWriter binlog index using seconds as well for quick offset get
type IndexWriter struct {
	dir  string    // binlog index file path
	curr uint32    // binlog timestamp
	fw   *os.File  // WriteEvent WriteEvent 生成binlog
	iw   io.Writer // iw io writer
}

// NewIndexWriter new index writer
func NewIndexWriter(dir string, curr uint32) (*IndexWriter, error) {
	name := fmt.Sprintf("%s/%d%s", dir, curr, BinlogIndexFile)
	_, err := os.Stat(name) //os.Stat获取文件信息
	flag := true
	if err != nil {
		if os.IsExist(err) {
			flag = true
		}
		flag = false
	}

	w := &IndexWriter{
		dir:  dir,
		curr: curr,
	}

	m := os.O_CREATE | os.O_RDWR
	if flag {
		m = os.O_RDWR | os.O_APPEND
	}

	// file exists
	f, err := os.OpenFile(name, m, os.FileMode(0666))
	if err != nil {
		log.Error("open index file{%s} error %v", name, err)
		return nil, err
	}
	w.fw = f
	w.iw = f

	return w, nil
}

// FlushIndex file for new timestamp
func (w *IndexWriter) FlushIndex(curr uint32) (*IndexWriter, error) {
	dir := w.dir
	w.Close()

	return NewIndexWriter(dir, curr)
}

// Close index writer
func (w *IndexWriter) Close() {
	if err := w.fw.Close(); err != nil {
		log.Errorf("close file{%s} error %s", fmt.Sprintf("%s/%d%s", w.dir, w.curr, BinlogIndexFile), err)
	}
	w.fw = nil
	w.iw = nil
}

// Write offset to index file
func (w *IndexWriter) Write(o *IndexOffset) error {
	b, err := json.Marshal(o)
	if err != nil {
		log.Errorf("json marshal offset{%v} error %v", o, err)
		return err
	}

	if _, err := w.iw.Write(b); err != nil {
		log.Errorf("write offset{%v} to index file{%s} error %v", o, fmt.Sprintf("%s/%d%s", w.dir, w.curr, BinlogIndexFile), err)
		return err
	}

	if _, err := w.iw.Write([]byte("\n")); err != nil {
		log.Errorf("write line to index file{%s} error %v", fmt.Sprintf("%s/%d%s", w.dir, w.curr, BinlogIndexFile), err)
		return err
	}

	return nil
}

// Latest offset for binlog file
func (w *IndexWriter) Latest() (*IndexOffset, error) {
	name := fmt.Sprintf("%s/%d%s", w.dir, w.curr, BinlogIndexFile)
	st, err := os.Stat(name) //os.Stat获取文件信息
	flag := true
	if err != nil {
		if os.IsExist(err) {
			flag = true
		}
		flag = false
	}

	if !flag {
		log.Warnf("file %s not exists", name)
		return nil, nil
	}

	// file size
	size := int64(st.Size())
	if size == 0 {
		// file size is empty
		log.Warnf("file{%s} is empty ", name)
		return nil, nil
	}

	f, err := os.OpenFile(name, os.O_RDONLY, inter.FileMode)
	if err != nil {
		log.Errorf("open file{%s} error{%v}", name, err)
		return nil, err
	}
	defer f.Close()

	data := list.New()

	// default read buffer size
	rbs := int64(100)

	// start position
	start := int64(-1*rbs - 1) // skip the last io.EOF

	// flag
	right := false
	for !right {
		if start+size <= 0 {
			// read to start
			start = -1 * size
			rbs = start + rbs + size
		}

		if _, err := f.Seek(start, 2); err != nil {
			log.Errorf("fseek (%d, 2) error %v", start, err)
			return nil, err
		}

		// buffer for reading cache
		buff := make([]byte, rbs)

		if _, err := f.Read(buff); err != nil {
			log.Errorf("read bytes from file{%s} error{%v}", name, err)
			return nil, err
		}

		idx := 0
		for i, b := range buff {
			switch {
			case start+size == 0:
				idx = -1
				right = true
			case b == '\n':
				idx = i
				// get the right position
				right = true
			}
		}

		if right {
			log.Debug(string(buff[idx+1:]))
			data.PushFront(buff[idx+1:])
		} else {
			log.Debug(string(buff))
			// put buffer to the front
			data.PushFront(buff)
			start = start - rbs
		}
	}

	log.Debugf("total file size{%d} start{%d}", st.Size(), start)

	b := bytes.NewBuffer(nil)
	for e := data.Front(); e != nil; e = e.Next() {
		if _, err := b.Write(e.Value.([]byte)); err != nil {
			log.Errorf("composite bytes from file{%s} error %v", name, err)
			return nil, err
		}
	}

	o := &IndexOffset{}
	if err := json.Unmarshal(b.Bytes(), o); err != nil {
		log.Errorf("unmarshal json{%s} error %v", b.Bytes(), err)
		return nil, err
	}

	return o, nil
}
