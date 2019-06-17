package binlog

import (
	"sync"

	"github.com/zssky/log"

	"github.com/mysql-binlog/siddontang/go-mysql/replication"

	"github.com/mysql-binlog/common/final"
	blog "github.com/mysql-binlog/common/log"
)

// TableEventHandler 表的事件处理器
type TableEventHandler struct {
	Table     string             // Table name
	Size      int64              // current binlog size
	After     *final.After       // after math
	Stamp     int64              // timestamp for binlog file name eg. 1020040204.log
	EventChan chan *Transaction  // transaction channel 事件队列
	Wg        *sync.WaitGroup    // Master wait group
	Writer    *blog.BinlogWriter // binlog writer
}

// Transaction package
type Transaction struct {
	TableMap *blog.DataEvent // table map event
	Event    *blog.DataEvent // DataEvent  including update, Delete, insert
	Gtid     *blog.DataEvent // gtid event
	Begin    *blog.DataEvent // begin event
}

// NewEventHandler
func NewEventHandler(path, table string, curr uint32, compress bool, desc *blog.DataEvent, after *final.After) (*TableEventHandler, error) {
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
		EventChan: make(chan *Transaction, 64),
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
func (h *TableEventHandler) handle(t *Transaction) error {
	// wg done
	defer h.Wg.Done()

	// write begin
	switch t.Event.Header.EventType {
	case replication.WRITE_ROWS_EVENTv0,
		replication.WRITE_ROWS_EVENTv1,
		replication.WRITE_ROWS_EVENTv2,
		replication.DELETE_ROWS_EVENTv0,
		replication.DELETE_ROWS_EVENTv1,
		replication.DELETE_ROWS_EVENTv2,
		replication.UPDATE_ROWS_EVENTv0,
		replication.UPDATE_ROWS_EVENTv1,
		replication.UPDATE_ROWS_EVENTv2:

		// write gtid
		if err := h.Writer.WriteEvent(t.Gtid); err != nil {
			return err
		}

		// write begin
		if err := h.Writer.WriteEvent(t.Begin); err != nil {
			return err
		}

		// write table map event
		if err := h.Writer.WriteEvent(t.TableMap); err != nil {
			return err
		}

		// write data
		if err := h.Writer.WriteEvent(t.Event); err != nil {
			return err
		}

		// write commit event using header
		if err := h.Writer.Commit(t.Event.Header); err != nil {
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
