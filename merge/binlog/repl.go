package binlog

import (
	"bytes"
	"fmt"
	"reflect"
	"runtime/debug"
	"strings"
	"sync"

	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"github.com/syndtr/goleveldb/leveldb"

	"github.com/mysql-binlog/siddontang/go-mysql/mysql"
	"github.com/mysql-binlog/siddontang/go-mysql/replication"

	"github.com/mysql-binlog/common/db"
	"github.com/mysql-binlog/common/final"
	"github.com/mysql-binlog/common/pb"
)

// TableEventHandler 表的事件处理器
type TableEventHandler struct {
	Table string        // table name
	XID   uint64        // xid transaction id
	Meta  *db.TableMeta // meta information
	After *final.After  // after math

	TableMapEvent []byte                        // table map event
	Db            *leveldb.DB                   // db single db
	EventChan     chan *replication.BinlogEvent // Event channel 事件队列
	Wg            *sync.WaitGroup

	// row 事件头 // stmt_end_flag = 1 只保存1份 而不是每个unit当中都保存一份
	RowsHeader     *replication.RowsEventHeader
}

var (
	// Versioned 确认版本号标志 主要针对insert update delete
	Versioned = false

	// WriteRowsEvent version
	WriteRowsEvent = pb.EVENT_TYPE_WRITE_ROWS_EVENTv0

	// DeleteRowsEvent version
	DeleteRowsEvent = pb.EVENT_TYPE_DELETE_ROWS_EVENTv0

	// UpdateRowsEvent version
	UpdateRowsEvent = pb.EVENT_TYPE_UPDATE_ROWS_EVENTv0
)

// SetEventVersion  设置dml事件版本
func SetEventVersion(et replication.EventType) {
	if Versioned {
		return
	}
	log.Info("set event version ", et)

	switch et {
	case replication.WRITE_ROWS_EVENTv1,
		replication.DELETE_ROWS_EVENTv1,
		replication.UPDATE_ROWS_EVENTv1:
		WriteRowsEvent = pb.EVENT_TYPE_WRITE_ROWS_EVENTv1
		DeleteRowsEvent = pb.EVENT_TYPE_DELETE_ROWS_EVENTv1
		UpdateRowsEvent = pb.EVENT_TYPE_UPDATE_ROWS_EVENTv1
	case replication.WRITE_ROWS_EVENTv2,
		replication.DELETE_ROWS_EVENTv2,
		replication.UPDATE_ROWS_EVENTv2:
		WriteRowsEvent = pb.EVENT_TYPE_WRITE_ROWS_EVENTv2
		DeleteRowsEvent = pb.EVENT_TYPE_DELETE_ROWS_EVENTv2
		UpdateRowsEvent = pb.EVENT_TYPE_UPDATE_ROWS_EVENTv2
	}

	Versioned = true
}

// HandleLogEvent write log event data into k-v storage
func (r *TableEventHandler) HandleLogEvent() {
	defer func() {
		if err := recover(); err != nil {
			r.After.Errs <- err
			return
		}
	}()

	for {
		select {
		case p, hasMore := <-r.EventChan:
			if !hasMore {
				log.Warn("handle log event closed")
				return
			}

			if err := r.handle(p); err != nil {
				debug.PrintStack()
				panic(err)
			}
		}
	}
}

// Update2ProtoBytes  update events to protobuf message bytes
func (r *TableEventHandler) Update2ProtoBytes(ev *replication.BinlogEvent) error {
	re := ev.Event.(*replication.RowsEvent)
	rowVals := re.Rows
	for i := 0; i < len(rowVals); i += 2 {
		before := &pb.BytePair{
			Key:          []byte(r.genKey(rowVals[i])),
			ColumnBitmap: re.ColumnBitmap1,
		}
		before.Value = genValue(i, ev)

		after := &pb.BytePair{
			Key:          []byte(r.genKey(rowVals[i+1])),
			ColumnBitmap: re.ColumnBitmap2,
		}
		after.Value = genValue(i+1, ev)

		// db
		if err := handleUpdateOnDb(r.Db, before, after); err != nil {
			return err
		}
	}

	return nil
}

func handleUpdateOnDb(db *leveldb.DB, before *pb.BytePair, after *pb.BytePair) error {
	newUnit := &pb.BytesUnit{
		Tp:     UpdateRowsEvent,
		Key:    after.Key,
		Before: before,
		After:  after,
	}

	old, err := db.Get(before.Key, nil)
	if err != nil {
		// key not found then using update
		data, _ := proto.Marshal(newUnit)
		if err := db.Put(newUnit.Key, data, nil); err != nil {
			return err
		}
		return nil
	}

	oldUnit := &pb.BytesUnit{}
	if err := proto.Unmarshal(old, oldUnit); err != nil {
		return err
	}

	// remove old unit
	if err := db.Delete(before.Key, nil); err != nil {
		return err
	}
	log.Debug("update delete key ", before.Key)

	var data []byte
	switch oldUnit.Tp { // insert + update = insert after
	case WriteRowsEvent:
		newUnit.Before = nil
		newUnit.Tp = WriteRowsEvent
		data, _ = proto.Marshal(newUnit)

	case UpdateRowsEvent:
		unit := &pb.BytesUnit{
			Tp:     UpdateRowsEvent,
			Key:    newUnit.Key,
			Before: oldUnit.Before,
			After:  newUnit.After,
		}
		data, _ = proto.Marshal(unit)
	}
	log.Debug("update put key ", newUnit.Key)

	return db.Put(newUnit.Key, data, nil)
}

// Delete2ProtoBytes delete rows event to protobuf message bytes
func (r *TableEventHandler) Delete2ProtoBytes(ev *replication.BinlogEvent) error {
	re := ev.Event.(*replication.RowsEvent)
	for i, row := range re.Rows {
		before := &pb.BytePair{
			Key:          []byte(r.genKey(row)),
			ColumnBitmap: re.ColumnBitmap1,
		}

		// db
		if err := handleDeleteOnDb(r.Db, before, ev, i); err != nil {
			return err
		}
	}
	return nil
}

// handleDeleteOnDb: handle delete operation on 1 single level db
func handleDeleteOnDb(db *leveldb.DB, before *pb.BytePair, ev *replication.BinlogEvent, i int) error {
	oldData, err := db.Get(before.Key, nil)
	if err != nil {
		// key not found in db
		before.Value = genValue(i, ev)

		unit := &pb.BytesUnit{
			Tp:     DeleteRowsEvent,
			Key:    before.Key,
			Before: before,
			After:  nil,
		}
		data, _ := proto.Marshal(unit)
		if err := db.Put(unit.Key, data, nil); err != nil {
			return err
		}

		log.Debug("delete put key ", before.Key)
		return nil
	}

	oldUnit := &pb.BytesUnit{}
	if err := proto.Unmarshal(oldData, oldUnit); err != nil {
		return err
	}

	switch oldUnit.Tp {
	case WriteRowsEvent:
		if err := db.Delete(before.Key, nil); err != nil {
			return err
		}
	case UpdateRowsEvent: // update + delete => delete before
		newUnit := &pb.BytesUnit{
			Tp:     DeleteRowsEvent,
			Key:    oldUnit.Before.Key,
			Before: oldUnit.Before,
			After:  nil,
		}

		data, _ := proto.Marshal(newUnit)
		if err := db.Put(newUnit.Key, data, nil); err != nil {
			return err
		}
	}

	return nil
}

// Insert2ProtoBytes : insert rows events to protobuf message bytes
func (r *TableEventHandler) Insert2ProtoBytes(ev *replication.BinlogEvent) error {
	re := ev.Event.(*replication.RowsEvent)
	for i, row := range re.Rows {
		after := &pb.BytePair{
			Key:          []byte(r.genKey(row)),
			ColumnBitmap: re.ColumnBitmap1,
		}
		after.Value = genValue(i, ev)

		// db
		if err := handleInsertOnDb(r.Db, after); err != nil {
			return err
		}
	}

	return nil
}

// handleInsertOnDb : handle insert on db
func handleInsertOnDb(db *leveldb.DB, after *pb.BytePair) error {
	oldData, err := db.Get([]byte(after.Key), nil)
	if err != nil {
		// key not found
		unit := &pb.BytesUnit{
			Tp:     WriteRowsEvent,
			Key:    after.Key,
			Before: nil,
			After:  after,
		}

		data, _ := proto.Marshal(unit)
		if err := db.Put(unit.Key, data, nil); err != nil {
			return err
		}

		return nil
	}

	oldUnit := &pb.BytesUnit{}
	if err := proto.Unmarshal(oldData, oldUnit); err != nil {
		return err
	}

	newUnit := &pb.BytesUnit{
		Tp:     UpdateRowsEvent,
		Key:    after.Key,
		Before: oldUnit.Before,
		After:  after,
	}

	data, _ := proto.Marshal(newUnit)
	return db.Put(newUnit.Key, data, nil)
}

// genValue : get value from rawData according to row value index
func genValue(i int, ev *replication.BinlogEvent) []byte {
	raw := ev.RawData
	rowValueIndex := ev.Event.(*replication.RowsEvent).RowValueIndex
	return raw[rowValueIndex[i]:rowValueIndex[i+1]] // 共享内存
}

// genKey: generate k-v storage key
func (r *TableEventHandler) genKey(row []interface{}) string {
	if r.Meta.KeyIndex == nil {
		// 没有找到唯一索引 所有值作为key 值
		values, _ := toArray(row)
		return strings.Join(values, "")
	}

	keys := make([]string, len(r.Meta.KeyIndex))
	for i := 0; i < len(r.Meta.KeyIndex); i++ {
		val := row[r.Meta.KeyIndex[i]]
		switch reflect.TypeOf(val).Kind() {
		case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			keys[i] = fmt.Sprintf("%d", val)

		case reflect.Float32, reflect.Float64:
			keys[i] = fmt.Sprintf("%f", val)

		case reflect.Slice:
			v := val.([]uint8)
			buffer := bytes.NewBuffer([]byte("0x"))
			for i := 0; i < len(v); i++ {
				buffer.WriteString(fmt.Sprintf("%.2x", v[i]))
			}
			keys[i] = buffer.String()

		case reflect.String:
			v := mysql.Escape(val.(string))
			keys[i] = v
		}
	}

	return strings.Join(keys, "")
}

// toArray: if there is no unique key in table then all column ares used for key
func toArray(row []interface{}) ([]string, []bool) {
	strRow := make([]string, len(row))

	quotes := make([]bool, len(row))

	for idx, colVal := range row {
		switch reflect.TypeOf(colVal).Kind() {
		case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			strRow[idx] = fmt.Sprintf("%d", colVal)
			quotes[idx] = false

		case reflect.Float32, reflect.Float64:
			strRow[idx] = fmt.Sprintf("%f", colVal)
			quotes[idx] = false

		case reflect.Slice:
			v := colVal.([]uint8)
			buffer := bytes.NewBuffer([]byte("0x"))
			for i := 0; i < len(v); i++ {
				buffer.WriteString(fmt.Sprintf("%.2x", v[i]))
			}
			strRow[idx] = buffer.String()

		case reflect.String:
			v := mysql.Escape(colVal.(string))
			strRow[idx] = v
			quotes[idx] = true
		}
	}

	return strRow, quotes
}

// handle handle binlog event
func (r *TableEventHandler) handle(e *replication.BinlogEvent) error {
	defer r.Wg.Done()
	// write to level db in case of merge
	switch e.Header.EventType {
	case replication.WRITE_ROWS_EVENTv0,
		replication.WRITE_ROWS_EVENTv1,
		replication.WRITE_ROWS_EVENTv2:
		if r.RowsHeader == nil {
			r.RowsHeader = e.Event.(*replication.RowsEvent).RowsHeader
		}

		if err := r.Insert2ProtoBytes(e); err != nil {
			log.Error("insert into bytes ", err)
			return err
		}

	case replication.DELETE_ROWS_EVENTv0,
		replication.DELETE_ROWS_EVENTv1,
		replication.DELETE_ROWS_EVENTv2:
		if r.RowsHeader == nil {
			r.RowsHeader = e.Event.(*replication.RowsEvent).RowsHeader
		}

		if err := r.Delete2ProtoBytes(e); err != nil {
			log.Error("delete bytes error ", err)
			return err
		}

	case replication.UPDATE_ROWS_EVENTv0,
		replication.UPDATE_ROWS_EVENTv1,
		replication.UPDATE_ROWS_EVENTv2:
		if r.RowsHeader == nil {
			r.RowsHeader = e.Event.(*replication.RowsEvent).RowsHeader
		}

		if err := r.Update2ProtoBytes(e); err != nil {
			log.Error("update bytes error ", err)
			return err
		}
	}
	return nil
}
