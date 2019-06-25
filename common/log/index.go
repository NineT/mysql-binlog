package log

import (
	"encoding/json"
	"fmt"
	"github.com/mysql-binlog/common/inter"
	"io"
	"os"
	"strings"

	"github.com/zssky/log"

	"github.com/mysql-binlog/common/meta"
)

/***
each one binlog file have one binlog index
index file is for gtid -> binlog offset and timestamp etc.
*/

const (
	// IndexSuffix const name
	IndexSuffix = ".index"
)

// IndexOffset including origin MySQL binlog offset and generated binlog offset
type IndexOffset struct {
	DumpFile string       `json:"file"`  // dump binlog file name
	DumpPos  uint32       `json:"pos"`   // dump binlog position
	Local    *meta.Offset `json:"local"` // generated local binlog offset
}

// IndexWriter binlog index using seconds as well for quick offset get
type IndexWriter struct {
	Name     string    // index file name
	FullName string    // full name
	dir      string    // binlog index file path
	curr     uint32    // binlog timestamp
	fw       *os.File  // WriteEvent WriteEvent 生成binlog
	iw       io.Writer // iw io writer
}

// IndexExists index file exits
func IndexExists(dir string, curr uint32) bool {
	name := fmt.Sprintf("%s/%d%s", dir, curr, IndexSuffix)
	return inter.Exists(name)
}

// NewIndexWriter new index writer
func NewIndexWriter(dir string, curr uint32) (*IndexWriter, error) {
	name := fmt.Sprintf("%d%s", curr, IndexSuffix)
	w := &IndexWriter{
		Name:     name,
		FullName: fmt.Sprintf("%s/%s", dir, name),
		dir:      dir,
		curr:     curr,
	}

	// create new index file
	f, err := inter.CreateFile(w.FullName)
	if err != nil {
		log.Error("open index file{%s} error %v", w.FullName, err)
		return nil, err
	}
	w.fw = f
	w.iw = f

	return w, nil
}

// RecoverIndex for no need to re-using the index writer
func RecoverIndex(dir string, curr uint32) (*IndexWriter, error) {
	dir = strings.TrimSuffix(dir, "/")

	name := fmt.Sprintf("%d%s", curr, IndexSuffix)
	w := &IndexWriter{
		Name:     name,
		FullName: fmt.Sprintf("%s/%s", dir, name),
		dir:      dir,
		curr:     curr,
	}

	// open the file
	f, err := os.OpenFile(w.FullName, os.O_APPEND|os.O_RDWR, inter.FileMode)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	// writer to the last tail event
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
		log.Errorf("close file{%s} error %s", fmt.Sprintf("%s/%d%s", w.dir, w.curr, IndexSuffix), err)
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
		log.Errorf("write offset{%v} to index file{%s} error %v", o, fmt.Sprintf("%s/%d%s", w.dir, w.curr, IndexSuffix), err)
		return err
	}

	if _, err := w.iw.Write([]byte("\n")); err != nil {
		log.Errorf("write line to index file{%s} error %v", fmt.Sprintf("%s/%d%s", w.dir, w.curr, IndexSuffix), err)
		return err
	}

	return nil
}

// Tail offset for binlog file
func (w *IndexWriter) Tail() (*IndexOffset, error) {
	name := fmt.Sprintf("%s/%d%s", w.dir, w.curr, IndexSuffix)

	b, err := inter.Tail(name)
	if err != nil {
		log.Errorf("read index file{%s} tail error{%v}", name, err)
		return nil, err
	}

	if b == nil {
		//
		log.Warnf("empty file{%s} ", name)
		return nil, nil
	}

	o := &IndexOffset{}
	if err := json.Unmarshal(b, o); err != nil {
		log.Errorf("unmarshal json{%s} error %v", string(b), err)
		return nil, err
	}

	return o, nil
}
