package binlog

import (
	"encoding/binary"
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
	mode         string               // dump mode type
	Table        string               // Table name
	Size         int64                // current binlog size
	After        *final.After         // after math
	Stamp        int64                // timestamp for binlog file name eg. 1020040204.log
	EventChan    chan *blog.DataEvent // transaction channel 事件队列
	lastRowEvent *blog.DataEvent      // save the last row event
	GtidChan     chan []byte          // gtid chan using
	cid          int64                // cluster id
	offset       *meta.Offset         // offset for local binlog file
	binWriter    *blog.BinlogWriter   // binlog writer
	idxWriter    *blog.IndexWriter    // index writer
	flWriter     *blog.LogList        // file list writer
}

// NewEventHandler new event handler for binlog writer
func NewEventHandler(mode, path, table string, curr uint32, cid int64, desc *blog.DataEvent, after *final.After, gch chan []byte) (*TableEventHandler, error) {
	log.Debug("new event handler")

	var binW *blog.BinlogWriter
	var idxW *blog.IndexWriter
	var flW *blog.LogList
	var off *meta.Offset

	dir := fmt.Sprintf("%s%s", path, table)
	log.Debugf("binlog and index writer dir %s/%d%s", dir, curr, blog.LogSuffix)

	if blog.ListExists(dir) { // list exist and size is not empty
		log.Debug("using file list")
		lw, err := blog.RecoverList(dir)
		if err != nil {
			log.Errorf("recover list log from directory{%s} error{%v}", dir, err)
			return nil, err
		}

		t, err := lw.Tail()
		if err != nil {
			log.Errorf("log list file{%s} tail error{%v}", lw.FullName, err)
			return nil, err
		}

		// reset timestamp
		curr = uint32(t)

		// index exists then using index
		iw, err := blog.RecoverIndex(dir, curr)
		if err != nil {
			log.Errorf("recover index writer{%s/%d%s} error{%v}", dir, curr, blog.IndexSuffix, err)
			return nil, err
		}

		// size is not 0
		o, err := iw.Tail()
		if err != nil {
			log.Errorf("get latest offset for index writer{%s/%d%s} error %v", dir, curr, blog.IndexSuffix, err)
			return nil, err
		}

		off = o.Local

		bw, err := blog.RecoverWriter(path, table, curr, o.Local.BinPos, desc)
		if err != nil {
			log.Errorf("recover writer{%s/%d%s} using exist index{%s/%d%s} error %v", dir, curr, blog.LogSuffix, dir, curr, blog.IndexSuffix, err)
			return nil, err
		}

		binW = bw
		idxW = iw
		flW = lw
	} else {
		log.Debugf("create new writer{%s/%d%s} and new index {%s/%d%s}", dir, curr, blog.LogSuffix, dir, curr, blog.LogSuffix)
		// index not exist as it is new for index writer and new for binlog writer as well if any one writer not exist then truncate file
		// binlog writer not exit never will the index writer exists
		bw, err := blog.NewBinlogWriter(path, table, curr, desc)
		if err != nil {
			log.Errorf("create binlog writer{%s/%d%s} error %v", dir, curr, blog.LogSuffix, err)
			return nil, err
		}

		// create new index writer for bw.dir
		iw, err := blog.NewIndexWriter(bw.Dir, curr)
		if err != nil {
			log.Errorf("create index writer for binlog {%s/%d%s} error %v", dir, curr, blog.LogSuffix, err)
			return nil, err
		}

		lw, err := blog.NewLogList(dir)
		if err != nil {
			log.Errorf("create new log list file on dir{%s} error{%v}", dir, err)
			return nil, err
		}

		if err := lw.Write([]byte(bw.Name)); err != nil {
			log.Errorf("write log file name{%s} to log file list{%s} error{%v}", bw.FullName, lw.FullName, err)
			return nil, err
		}

		// bin writer and index writer
		binW = bw
		idxW = iw
		flW = lw
	}

	return &TableEventHandler{
		cid:   cid,
		mode:  mode,
		Table: table,
		After: after,
		Stamp: int64(curr),
		// channel init
		EventChan: make(chan *blog.DataEvent, inter.BufferSize),
		GtidChan:  gch,
		offset:    off, // offset for binlog file
		binWriter: binW,
		idxWriter: idxW,
		flWriter:  flW,
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
	log.Debugf("handle %s %s %s", t.Header.EventType, t.ExedGtid, t.TrxGtid)
	if h.offset != nil {
		// current offset for
		co := &meta.Offset{
			ExedGtid: string(t.ExedGtid),
			TrxGtid:  string(t.TrxGtid),
			Time:     t.Header.Timestamp,
			BinFile:  t.BinFile,
			BinPos:   uint64(t.Header.LogPos),
		}

		flag, err := meta.LessEqual(co, h.offset)
		if err != nil {
			log.Errorf("compare offset{%v, %v} error %v", co, h.offset, err)
			return err
		}

		if flag {
			// push gtid into channel
			switch t.Header.EventType {
			case replication.XID_EVENT:
				// return gtid
				h.GtidChan <- t.TrxGtid
			case replication.QUERY_EVENT:
				if t.IsDDL || t.IsCommit {
					// return gtid
					h.GtidChan <- t.TrxGtid
				}
			}

			// current offset <= h.offset no need to rewrite event and index into file again the offset will reset until the new gtid event coming
			return nil
		}
	}

	// cache the origin log position
	pos := t.Header.LogPos

	switch t.Header.EventType {
	case replication.XID_EVENT:
		return h.commit(t, pos)

	case replication.QUERY_EVENT:
		if t.IsDDL { // only ddl then take it as commit event to check whether it is the right to flush logs
			return h.commit(t, pos)
		} else if t.IsCommit {
			return h.commit(t, pos)
		} else {
			// write query event as well
			if err := h.binWriter.WriteEvent(t); err != nil {
				log.Errorf("write query event{%s} but not ddl on writer{%s} error{%v}", string(t.Data), h.binWriter.FullName, err)
				return err
			}
		}
	case replication.WRITE_ROWS_EVENTv0,
		replication.WRITE_ROWS_EVENTv1,
		replication.WRITE_ROWS_EVENTv2,
		replication.DELETE_ROWS_EVENTv0,
		replication.DELETE_ROWS_EVENTv1,
		replication.DELETE_ROWS_EVENTv2,
		replication.UPDATE_ROWS_EVENTv0,
		replication.UPDATE_ROWS_EVENTv1,
		replication.UPDATE_ROWS_EVENTv2:
		switch h.mode {
		case inter.Integrated:
			// one binlog file for all data
			if err := h.binWriter.WriteEvent(t); err != nil {
				log.Errorf("write event {%s} to dir{%s} error{%v}", h.binWriter.FullName, h.binWriter.Dir, err)
				return err
			}
		case inter.Separated:
			// save last row event wait for flush when commit arrived
			pre := h.lastRowEvent

			h.lastRowEvent = t

			// write event
			if pre != nil {
				if err := h.binWriter.WriteEvent(pre); err != nil {
					log.Errorf("write event {%s} to dir{%s} error{%v}", h.binWriter.FullName, h.binWriter.Dir, err)
					return err
				}
			}
		}
	default:
		// write event
		if err := h.binWriter.WriteEvent(t); err != nil {
			log.Errorf("write event {%s} to dir{%s} error{%v}", h.binWriter.FullName, h.binWriter.Dir, err)
			return err
		}
	}

	return nil
}

// commit event to binlog
func (h *TableEventHandler) commit(t *blog.DataEvent, pos uint32) error {
	// write last row event and fix event stmt_flag
	if h.mode == inter.Separated && h.lastRowEvent != nil {
		// old flags
		of := binary.LittleEndian.Uint16(h.lastRowEvent.Data[h.lastRowEvent.RowsHeader.FlagsPos:])

		of |= blog.StmtEndFlag

		fs := make([]byte, 2)
		binary.LittleEndian.PutUint16(fs, of)

		h.lastRowEvent.Data[h.lastRowEvent.RowsHeader.FlagsPos] = fs[0]
		h.lastRowEvent.Data[h.lastRowEvent.RowsHeader.FlagsPos+1] = fs[1]

		// write gtid
		if err := h.binWriter.WriteEvent(h.lastRowEvent); err != nil {
			log.Errorf("xid event write to binlog file{%s} error{%v}", h.binWriter.FullName, err)
			return err
		}
	}

	// reset last row event
	h.lastRowEvent = nil

	// write gtid
	if err := h.binWriter.WriteEvent(t); err != nil {
		log.Errorf("xid event write to binlog file{%s} error{%v}", h.binWriter.FullName, err)
		return err
	}

	// gtid event then should remember the latest offset
	h.offset = h.binWriter.LastPos(h.cid, t.ExedGtid, t.TrxGtid, t.Header.Timestamp)

	// write offset to binlog index file
	if err := h.idxWriter.Write(&meta.IndexOffset{
		DumpFile: string(t.BinFile),
		DumpPos:  pos,
		Local:    h.offset,
	}); err != nil {
		log.Errorf("write xid index offset {%v} error %v", h.offset, err)
		return err
	}

	// new file
	nf, err := h.binWriter.CheckFlush(t.Header.Timestamp)
	if err != nil {
		log.Errorf("check binlog{%s} flush error %v", h.binWriter.FullName, err)
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

		if err := h.flWriter.Write([]byte(h.binWriter.Name)); err != nil {
			log.Errorf("write file name{%s} to file list{%s} error{%v}", h.binWriter.FullName, h.flWriter.FullName, err)
			return err
		}
	}

	// return gtid
	h.GtidChan <- t.TrxGtid
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
