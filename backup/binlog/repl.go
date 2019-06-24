package binlog

import (
	"fmt"
	"runtime/debug"

	"github.com/mysql-binlog/siddontang/go-mysql/replication"
	"github.com/zssky/log"

	"github.com/mysql-binlog/common/final"
	"github.com/mysql-binlog/common/inter"
	blog "github.com/mysql-binlog/common/log"
	"github.com/mysql-binlog/common/meta"
)

// TableEventHandler 表的事件处理器
type TableEventHandler struct {
	Table     string               // Table name
	Size      int64                // current binlog size
	After     *final.After         // after math
	Stamp     int64                // timestamp for binlog file name eg. 1020040204.log
	EventChan chan *blog.DataEvent // transaction channel 事件队列
	GtidChan  chan []byte          // gtid chan using
	cid       int64                // cluster id
	offset    *meta.Offset         // offset for local binlog file
	binWriter *blog.BinlogWriter   // binlog writer
	idxWriter *blog.IndexWriter    // index writer
}

// NewEventHandler new event handler for binlog writer
func NewEventHandler(path, table string, curr uint32, cid int64, desc *blog.DataEvent, after *final.After, gch chan []byte) (*TableEventHandler, error) {
	log.Debug("new event handler for binlog writer and index writer")

	var binW *blog.BinlogWriter
	var idxW *blog.IndexWriter
	var off *meta.Offset

	dir := fmt.Sprintf("%s%s", path, table)
	log.Debug("binlog and index writer dir ", dir)

	if blog.WriterExists(path, table, curr) && blog.IndexExists(dir, curr) {

		// index exists then using index
		iw, err := blog.RecoverIndex(dir, curr)
		if err != nil {
			log.Errorf("recover index writer{%s/%d} error %v", dir, curr, err)
			return nil, err
		}

		// size is not 0
		o, err := iw.Latest()
		if err != nil {
			log.Errorf("get latest offset for index writer{%s/%d} error %v", dir, curr, err)
			return nil, err
		}

		off = o.Local

		bw, err := blog.RecoverWriter(path, table, curr, o.Local.BinPos, desc)
		if err != nil {
			log.Errorf("recover writer{%s/%d.log} using exist index{%s/%d} error %v", dir, curr, dir, curr, err)
			return nil, err
		}

		binW = bw
		idxW = iw
	} else {
		// index not exist as it is new for index writer and new for binlog writer as well if any one writer not exist then truncate file
		// binlog writer not exit never will the index writer exists
		bw, err := blog.NewBinlogWriter(path, table, curr, desc)
		if err != nil {
			log.Errorf("create binlog writer{%s/%d.log} error %v", dir, curr, err)
			return nil, err
		}

		// create new index writer for bw.dir
		iw, err := blog.NewIndexWriter(bw.Dir, curr)
		if err != nil {
			log.Errorf("create index writer for binlog {%s/%d.log} error %v", dir, curr, err)
			return nil, err
		}

		// bin writer and index writer
		binW = bw
		idxW = iw
	}

	return &TableEventHandler{
		cid:   cid,
		Table: table,
		After: after,
		Stamp: int64(curr),
		// channel init
		EventChan: make(chan *blog.DataEvent, inter.BufferSize),
		GtidChan:  gch,
		offset:    off, // offset for binlog file
		binWriter: binW,
		idxWriter: idxW,
	}, nil
}

// HandleLogEvent write log event data into k-v storage
func (h *TableEventHandler) HandleLogEvent() {
	defer func() {
		if err := recover(); err != nil {
			debug.PrintStack()
			h.After.Errs <- err
		}
	}()
	for {
		select {
		case <-h.After.Ctx.Done():
			// context done
			return
		case e, hasMore := <-h.EventChan:
			if !hasMore {
				log.Warnf("table{%s} event channel is closed", h.Table)
				return
			}

			// write to level db in case of merge
			if err := h.handle(e); err != nil {
				log.Error("handle event error exit channel ", err)
				debug.PrintStack()
				panic(err)
			}
		}
	}
}

// handle handle binlog event into binlog file
func (h *TableEventHandler) handle(t *blog.DataEvent) error {
	log.Debug("handle")
	if h.offset != nil {
		// current offset for
		co := &meta.Offset{
			ExedGtid: t.ExedGtid,
			TrxGtid:  t.TrxGtid,
			Time:     t.Header.Timestamp,
			BinFile:  string(t.BinFile),
			BinPos:   t.Header.LogPos,
		}

		flag, err := meta.LessEqual(co, h.offset)
		if err != nil {
			log.Errorf("compare offset{%v, %v} error %v", co, h.offset, err)
			return err
		}

		if flag {
			// current offset <= h.offset no need to rewrite event and index into file again the offset will reset until the new gtid event coming
			return nil
		}
	}

	// cache the origin log position
	pos := t.Header.LogPos

	switch t.Header.EventType {
	case replication.XID_EVENT:

		// write gtid
		if err := h.binWriter.WriteEvent(t); err != nil {
			log.Errorf("xid event write to binlog file{%s/%s} error{%v}", h.binWriter.Dir, h.binWriter.Name, err)
			return err
		}

		// gtid event then should remember the latest offset
		h.offset = h.binWriter.LastPos(h.cid, t.ExedGtid, t.TrxGtid, t.Header.Timestamp)

		// write offset to binlog index file
		if err := h.idxWriter.Write(&blog.IndexOffset{
			DumpFile: string(t.BinFile),
			DumpPos:  pos,
			Local:    h.offset,
		}); err != nil {
			log.Errorf("write xid index offset {%v} error %v", )
			return err
		}

		// new file
		nf, err := h.binWriter.CheckFlush(t.Header.Timestamp)
		if err != nil {
			log.Errorf("check binlog{%s/%s} flush error %v", h.binWriter.Dir, h.binWriter.Name, err)
			return err
		}

		if nf {
			// flush index file
			iw, err := h.idxWriter.FlushIndex(t.Header.Timestamp)
			if err != nil {
				log.Errorf("flush index writer {%s} error %v", h.binWriter.Dir, err)
				return err
			}
			h.idxWriter = iw
		}

		// return gtid
		h.GtidChan <- t.TrxGtid
	case replication.QUERY_EVENT:
		if t.IsDDL { // only ddl then take it as commit event to check whether it is the right to flush logs
			// write gtid
			if err := h.binWriter.WriteEvent(t); err != nil {
				log.Errorf("write query event into binlog file{%s/%s} error %v", h.binWriter.Dir, h.binWriter.Name, err)
				return err
			}

			// gtid event then should remember the latest offset
			h.offset = h.binWriter.LastPos(h.cid, t.ExedGtid, t.TrxGtid, t.Header.Timestamp)

			// write offset to binlog index file
			if err := h.idxWriter.Write(&blog.IndexOffset{
				DumpFile: string(t.BinFile),
				DumpPos:  pos,
				Local:    h.offset,
			}); err != nil {
				log.Errorf("write query event index{%s/%s} error{%v}", h.binWriter.Dir, h.binWriter.Name, err)
				return err
			}

			// new file
			nf, err := h.binWriter.CheckFlush(t.Header.Timestamp)
			if err != nil {
				log.Errorf("check binlog file{%s/%s} flush error{%v}", h.binWriter.Dir, h.binWriter.Name, err)
				return err
			}

			if nf {
				// flush index file
				iw, err := h.idxWriter.FlushIndex(t.Header.Timestamp)
				if err != nil {
					log.Errorf("flush index writer{%s} error %v", h.idxWriter.Name, err)
					return err
				}
				h.idxWriter = iw
			}

			// return gtid
			h.GtidChan <- t.TrxGtid
		} else {
			// write query event as well
			if err := h.binWriter.WriteEvent(t); err != nil {
				log.Errorf("write query event{%s} but not ddl on writer{%s/%s} error{%v}", string(t.Data), h.binWriter.Dir, h.binWriter.Name, err)
				return err
			}
		}
	default:
		// write event
		if err := h.binWriter.WriteEvent(t); err != nil {
			log.Errorf("write event {%s/%s} error{%v}", h.binWriter.Dir, h.binWriter.Name, err)
			return err
		}
	}

	return nil
}

// Close close table event handler
func (h *TableEventHandler) Close() {
	// close all channel @attention for this must called outside
	if h.EventChan != nil {
		close(h.EventChan)
	}

	// close binlog writer
	bw := h.binWriter
	if bw != nil {
		bw.Close()
	}

	// close index writer
	iw := h.idxWriter
	if iw != nil {
		iw.Close()
	}
}
