package binlog

import (
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
	offset    *meta.Offset         // offset for local binlog file
	binWriter *blog.BinlogWriter   // binlog writer
	idxWriter *blog.IndexWriter    // index writer
}

// NewEventHandler
func NewEventHandler(path, table string, curr uint32, compress bool, desc *blog.DataEvent, after *final.After, gch chan []byte) (*TableEventHandler, error) {
	log.Debug("new event handler")
	// todo make sure that gtid event is behind the current

	bw, err := blog.NewBinlogWriter(path, table, curr, compress, desc)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	// create new index writer for bw.dir
	iw, err := blog.NewIndexWriter(bw.Dir, curr)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	return &TableEventHandler{
		Table: table,
		After: after,
		Stamp: int64(curr),
		// channel init
		EventChan: make(chan *blog.DataEvent, inter.BufferSize),
		GtidChan:  gch,
		binWriter: bw,
		idxWriter: iw,
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
				log.Warn("channel is closed")
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
	switch t.Header.EventType {
	case replication.GTID_EVENT:
		// gtid event then should remember the latest offset
		h.offset = h.binWriter.LastPos(t.SinGtid, t.Header.Timestamp)

		// write gtid
		if err := h.binWriter.WriteEvent(t); err != nil {
			return err
		}
	case replication.XID_EVENT:
		// write gtid
		if err := h.binWriter.WriteEvent(t); err != nil {
			return err
		}

		// write offset to binlog index file
		if err := h.idxWriter.Write(&blog.IndexOffset{
			Dump: &meta.Offset{
				ExedGtid: t.SinGtid,
				Time:     t.Header.Timestamp,
				BinFile:  string(t.BinFile),
				BinPos:   t.Header.LogPos,
			},
			Local: h.offset,
		}); err != nil {
			return err
		}

		// new file
		nf, err := h.binWriter.CheckFlush(t.Header.Timestamp)
		if err != nil {
			return err
		}

		if nf {
			// flush index file
			iw, err := h.idxWriter.FlushIndex(t.Header.Timestamp)
			if err != nil {
				return err
			}
			h.idxWriter = iw
		}

		// return gtid
		h.GtidChan <- t.SinGtid
	case replication.QUERY_EVENT:
		if t.IsDDL { // only ddl then take it as commit event to check whether it is the right to flush logs
			// write gtid
			if err := h.binWriter.WriteEvent(t); err != nil {
				return err
			}

			// write offset to binlog index file
			if err := h.idxWriter.Write(&blog.IndexOffset{
				Dump: &meta.Offset{
					ExedGtid: t.SinGtid,
					Time:     t.Header.Timestamp,
					BinFile:  string(t.BinFile),
					BinPos:   t.Header.LogPos,
				},
				Local: h.offset,
			}); err != nil {
				return err
			}

			// new file
			nf, err := h.binWriter.CheckFlush(t.Header.Timestamp)
			if err != nil {
				return err
			}

			if nf {
				// flush index file
				iw, err := h.idxWriter.FlushIndex(t.Header.Timestamp)
				if err != nil {
					return err
				}
				h.idxWriter = iw
			}

			// return gtid
			h.GtidChan <- t.SinGtid
		} else {
			// write query event as well
			if err := h.binWriter.WriteEvent(t); err != nil {
				return err
			}
		}
	default:
		// write event
		if err := h.binWriter.WriteEvent(t); err != nil {
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
}
