package binlog

import (
	"github.com/mysql-binlog/siddontang/go-mysql/replication"
	"sync"

	"github.com/zssky/log"

	"github.com/mysql-binlog/common/final"
	blog "github.com/mysql-binlog/common/log"
)

// TableEventHandler 表的事件处理器
type TableEventHandler struct {
	Table     string               // Table name
	Size      int64                // current binlog size
	After     *final.After         // after math
	Stamp     int64                // timestamp for binlog file name eg. 1020040204.log
	EventChan chan *blog.DataEvent // transaction channel 事件队列
	GtidChan  chan []byte          // gtid chan using
	Wg        *sync.WaitGroup      // Master wait group
	Writer    *blog.BinlogWriter   // binlog writer
}

// NewEventHandler
func NewEventHandler(path, table string, curr uint32, compress bool, desc *blog.DataEvent, after *final.After, gch chan []byte) (*TableEventHandler, error) {
	wg := &sync.WaitGroup{}

	w, err := blog.NewBinlogWriter(path, table, curr, compress, desc)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	evh := &TableEventHandler{
		Table: table,
		After: after,
		Stamp: int64(curr),
		// channel init
		EventChan: make(chan *blog.DataEvent, 64),
		GtidChan:  gch,
		Wg:        wg,
		Writer:    w,
	}

	return evh, nil
}

// HandleLogEvent write log event data into k-v storage
func (h *TableEventHandler) HandleLogEvent() {
	defer func() {
		if err := recover(); err != nil {
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
				panic(err)
			}
		}
	}
}

// handle handle binlog event into binlog file
func (h *TableEventHandler) handle(t *blog.DataEvent) error {
	// wg done
	defer h.Wg.Done()

	switch t.Header.EventType {
	case replication.XID_EVENT:
		// write gtid
		if err := h.Writer.WriteEvent(t); err != nil {
			return err
		}

		// return gtid
		h.GtidChan <- t.Gtid
	case replication.QUERY_EVENT:
		if t.IsDDL { // only ddl then write log
			// write gtid
			if err := h.Writer.WriteEvent(t); err != nil {
				return err
			}
		}
	default:
		// write gtid
		if err := h.Writer.WriteEvent(t); err != nil {
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
