package binlog

import (
	"bytes"
	"fmt"
	"github.com/mysql-binlog/common/final"
	"os"
	"reflect"
	"strings"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/zssky/log"

	"github.com/mysql-binlog/common/pb"
	"github.com/mysql-binlog/siddontang/go-mysql/mysql"
	"github.com/mysql-binlog/siddontang/go-mysql/replication"

	"github.com/mysql-binlog/common/db"
	"github.com/mysql-binlog/common/inter"
	blog "github.com/mysql-binlog/common/log"
)

// TableEventHandler 表的事件处理器
type TableEventHandler struct {
	Table string          // Table name
	XID   uint64          // transaction id
	Meta  *db.TableMeta   // Table Meta
	After *final.After    // after math

	TableMapEvent []byte                        // Table map event
	SecDb         *leveldb.DB                   // Second level db
	MinDb         *leveldb.DB                   // Minute level db
	HourDb        *leveldb.DB                   // Hour level db
	DayDb         *leveldb.DB                   // Day level db
	LastDayTime   uint32                        // last day backup time
	LastHourTime  uint32                        // last hour backup time
	LastMinTime   uint32                        // last minute backup time
	LastSecTime   uint32                        // last second backup time
	EventChan     chan *replication.BinlogEvent // Event channel 事件队列
	DayRestChan   chan *Rest                    // hour log file restitute channel 还原队列
	DayOffset     *inter.BinlogPosition         // DayOffset
	HourRestChan  chan *Rest                    // hour log file restitute channel 还原队列
	HourOffset    *inter.BinlogPosition         // HourOffset
	MinRestChan   chan *Rest                    // minute log file restitue channel
	MinOffset     *inter.BinlogPosition         // Minute offset
	SecRestChan   chan *Rest                    // second log file restitue channel
	SecOffset     *inter.BinlogPosition         // Second offset
	Wg            *sync.WaitGroup               // Master wait group

	// FormatDesc 事件
	FormatDesc *replication.FormatDescriptionEvent

	// 公共事件头
	EventHeader *replication.EventHeader

	// QueryEvent
	QueryEvent *replication.QueryEvent

	// RowsHeader  row 事件头 // stmt_end_flag = 1 只保存1份 而不是每个unit当中都保存一份
	RowsHeader *replication.RowsEventHeader
}

// Rest restitute  data from leveldb to binlog file
type Rest struct {
	NewPath string                // new path 新的leveldb 文件路径
	Client  inter.IClient         // remove client  用于写入数据
	Abp     *inter.AbsolutePath   // absolute path
	Have    bool                  // have data
	DDL     []byte                // ddl 语句
	Flags   uint16                // rows header flag
	Offset  *inter.BinlogPosition // Binlog Offset
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
		}
	}()
	for {
		select {
		case <-r.After.Ctx.Done():
			// context done
			return
		case e, hasMore := <-r.EventChan:
			if !hasMore {
				log.Warn("channel is closed")
				return
			}

			// write to level db in case of merge
			if err := r.handle(e); err != nil {
				log.Error("handle event error exit channel ", err)
				panic(err)
				return
			}
		}
	}
}

// update2ProtoBytes  update events to protobuf message bytes
func (r *TableEventHandler) update2ProtoBytes(ev *replication.BinlogEvent) error {
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

		if r.SecDb != nil {
			// second db
			if err := handleUpdateOnDb(r.SecDb, before, after); err != nil {
				return err
			}
		}

		if r.MinDb != nil {
			// minute db
			if err := handleUpdateOnDb(r.MinDb, before, after); err != nil {
				return err
			}
		}

		if r.HourDb != nil {
			// hour db
			if err := handleUpdateOnDb(r.HourDb, before, after); err != nil {
				return err
			}
		}
		// day db
		if err := handleUpdateOnDb(r.DayDb, before, after); err != nil {
			return err
		}
	}

	return nil
}

// handleUpdateOnDb handle update data on ldb
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
		return db.Put(newUnit.Key, data, nil)
	}

	oldUnit := &pb.BytesUnit{}
	if err := proto.Unmarshal(old, oldUnit); err != nil {
		return err
	}

	// remove old unit
	log.Debug("update delete key ", before.Key)
	if err := db.Delete(before.Key, nil); err != nil {
		return err
	}

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

// delete2ProtoBytes delete rows event to protobuf message bytes
func (r *TableEventHandler) delete2ProtoBytes(ev *replication.BinlogEvent) error {
	re := ev.Event.(*replication.RowsEvent)
	for i, row := range re.Rows {
		before := &pb.BytePair{
			Key:          []byte(r.genKey(row)),
			ColumnBitmap: re.ColumnBitmap1,
		}

		if r.SecDb != nil {
			// second db
			if err := handleDeleteOnDb(r.SecDb, before, ev, i); err != nil {
				return err
			}
		}

		if r.MinDb != nil {
			// minute db
			if err := handleDeleteOnDb(r.MinDb, before, ev, i); err != nil {
				return err
			}
		}

		if r.HourDb != nil {
			// hour db
			if err := handleDeleteOnDb(r.HourDb, before, ev, i); err != nil {
				return err
			}
		}

		// day db
		if err := handleDeleteOnDb(r.DayDb, before, ev, i); err != nil {
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
		log.Debug("delete put key ", before.Key)

		data, _ := proto.Marshal(unit)
		return db.Put(unit.Key, data, nil)
	}

	oldUnit := &pb.BytesUnit{}
	if err := proto.Unmarshal(oldData, oldUnit); err != nil {
		return err
	}

	switch oldUnit.Tp {
	case WriteRowsEvent:
		return db.Delete(before.Key, nil)
	case UpdateRowsEvent: // update + delete => delete before
		newUnit := &pb.BytesUnit{
			Tp:     DeleteRowsEvent,
			Key:    oldUnit.Before.Key,
			Before: oldUnit.Before,
			After:  nil,
		}

		data, _ := proto.Marshal(newUnit)
		return db.Put(newUnit.Key, data, nil)
	}

	return nil
}

// insert2ProtoBytes : insert rows events to protobuf message bytes
func (r *TableEventHandler) insert2ProtoBytes(ev *replication.BinlogEvent) error {
	re := ev.Event.(*replication.RowsEvent)
	for i, row := range re.Rows {
		after := &pb.BytePair{
			Key:          []byte(r.genKey(row)),
			ColumnBitmap: re.ColumnBitmap1,
		}
		after.Value = genValue(i, ev)

		if r.SecDb != nil {
			// second db
			if err := handleInsertOnDb(r.SecDb, after); err != nil {
				return err
			}
		}

		if r.MinDb != nil {
			// minute db
			if err := handleInsertOnDb(r.MinDb, after); err != nil {
				return err
			}
		}

		if r.HourDb != nil {
			// hour db
			if err := handleInsertOnDb(r.HourDb, after); err != nil {
				return err
			}
		}

		// day db
		if err := handleInsertOnDb(r.DayDb, after); err != nil {
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
		return db.Put(unit.Key, data, nil)
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
		log.Warn("table have no key index ", r.Table)
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
		if colVal == nil {
			log.Warn("column[", idx, "] is null")
			continue
		}
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

// HandleDayBinlogFile 处理binlog 文件
func (r *TableEventHandler) HandleDayBinlogFile() {
	defer func() {
		if err := recover(); err != nil {
			r.After.Errs <- err
		}
	}()
	for {
		select {
		case <-r.After.Ctx.Done():
			// context done
			return
		case p, hasMore := <-r.DayRestChan:
			if !hasMore {
				log.Warn("channel is closed")
				return
			}

			if err := p.Restitute(r); err != nil {
				log.Error("HandleDayBinlogFile file error ", err)
				panic(err)
				// exit for loop
				return
			}

			// 设置offset
			r.DayOffset = p.Offset
		}
	}

}

// HandleHourBinlogFile 处理binlog 文件
func (r *TableEventHandler) HandleHourBinlogFile() {
	defer func() {
		if err := recover(); err != nil {
			r.After.Errs <- err
		}
	}()
	for {
		select {
		case <-r.After.Ctx.Done():
			// context done
			return
		case p, hasMore := <-r.HourRestChan:
			if !hasMore {
				log.Warn("channel is closed")
				return
			}

			if err := p.Restitute(r); err != nil {
				log.Error("HandleHourBinlogFile file error ", err)
				panic(err)
				// exit for loop
				return
			}

			// 设置 offset
			r.HourOffset = p.Offset
		}
	}

}

// HandleMinBinlogFile 处理binlog 文件
func (r *TableEventHandler) HandleMinBinlogFile() {
	defer func() {
		if err := recover(); err != nil {
			r.After.Errs <- err
		}
	}()
	for {
		select {
		case <-r.After.Ctx.Done():
			// context done
			return
		case p, hasMore := <-r.MinRestChan:
			if !hasMore {
				log.Warn("channel is closed")
				return
			}

			if err := p.Restitute(r); err != nil {
				log.Error("HandleMinBinlogFile file error ", err)
				panic(err)
				// exit for loop
				return
			}

			// 设置 offset
			r.MinOffset = p.Offset
		}
	}

}

// HandleSecBinlogFile 处理binlog 文件
func (r *TableEventHandler) HandleSecBinlogFile() {
	defer func() {
		if err := recover(); err != nil {
			r.After.Errs <- err
		}
	}()
	for {
		select {
		case <-r.After.Ctx.Done():
			// context done
			return
		case p, hasMore := <-r.SecRestChan:
			if !hasMore {
				log.Warn("channel is closed")
				return
			}

			if err := p.Restitute(r); err != nil {
				log.Error("HandleSecBinlogFile file error ", err)
				panic(err)
				// exit for loop
				return
			}

			// 设置 offset
			r.SecOffset = p.Offset
		}
	}

}

// handle handle log event
func (r *TableEventHandler) handle(e *replication.BinlogEvent) error {
	// wg done
	defer r.Wg.Done()

	switch e.Header.EventType {
	case replication.WRITE_ROWS_EVENTv0,
		replication.WRITE_ROWS_EVENTv1,
		replication.WRITE_ROWS_EVENTv2:
		if r.RowsHeader == nil {
			r.RowsHeader = e.Event.(*replication.RowsEvent).RowsHeader
		}

		if err := r.insert2ProtoBytes(e); err != nil {
			return err
		}

	case replication.DELETE_ROWS_EVENTv0,
		replication.DELETE_ROWS_EVENTv1,
		replication.DELETE_ROWS_EVENTv2:
		if r.RowsHeader == nil {
			r.RowsHeader = e.Event.(*replication.RowsEvent).RowsHeader
		}

		if err := r.delete2ProtoBytes(e); err != nil {
			return err
		}

	case replication.UPDATE_ROWS_EVENTv0,
		replication.UPDATE_ROWS_EVENTv1,
		replication.UPDATE_ROWS_EVENTv2:
		if r.RowsHeader == nil {
			r.RowsHeader = e.Event.(*replication.RowsEvent).RowsHeader
		}

		if err := r.update2ProtoBytes(e); err != nil {
			return err
		}
	}
	return nil
}

// Close close table event handler
func (r *TableEventHandler) Close() {
	r.closeChan()
	r.closeDb()
	r.release()
}

// release memory
func (r *TableEventHandler) release() {
	r.Meta = nil
	r.TableMapEvent = nil
	r.FormatDesc = nil
	r.EventHeader = nil
	r.QueryEvent = nil
	r.RowsHeader = nil
}

// closeChan close all channel
func (r *TableEventHandler) closeChan() {
	// close all channel @attention for this must called outside
	if r.EventChan != nil {
		close(r.EventChan)
	}

	if r.DayRestChan != nil {
		close(r.DayRestChan)
	}

	if r.HourRestChan != nil {
		close(r.HourRestChan)
	}

	if r.MinRestChan != nil {
		close(r.MinRestChan)
	}

	if r.SecRestChan != nil {
		close(r.SecRestChan)
	}

}

// closeDb close level db
func (r *TableEventHandler) closeDb() {
	db := r.DayDb
	if db != nil {
		db.Close()
	}

	db = r.HourDb
	if db != nil {
		db.Close()
	}

	db = r.MinDb
	if db != nil {
		db.Close()
	}

	db = r.SecDb
	if db != nil {
		db.Close()
	}
}

// Restitute 还原数据
func (t *Rest) Restitute(r *TableEventHandler) error {
	defer func() {
		// 删除拷贝的数据文件
		log.Info("删除文件夹 ", t.NewPath)
		os.RemoveAll(t.NewPath)
	}()
	// relative path 相对路径
	rp := fmt.Sprintf("%s/%s", t.Abp.Host, t.Abp.Table)

	// client file
	f, _ := t.Client.CreateFile(rp, t.Abp.DstLogFileName())
	defer func() {
		// 关闭文件
		f.(inter.IFile).Close()

		// 压缩文件
		t.Client.CTarFiles(rp, t.Abp.FileType, t.Abp.Start, t.Abp.End)
	}()

	// using flags
	r.EventHeader.Flags = t.Flags

	// binlog writer
	bw := &blog.BinlogWriter{
		FormatDesc:    r.FormatDesc,
		EventHeader:   r.EventHeader,
		QueryEvent:    r.QueryEvent,
		TableMapEvent: r.TableMapEvent,
		W:             f,
		XID:           r.XID,
		NextPrefix:    []byte(t.Abp.NextPrefix()),
	}
	defer bw.Clear()

	if r.RowsHeader != nil {
		bw.RowsHeader = r.RowsHeader.Copy()
	}

	// 写文件头
	if err := bw.WriteBinlogFileHeader(); err != nil {
		return err
	}

	// write Desc event
	if err := bw.WriteDescEvent(); err != nil {
		return err
	}

	if t.Have { // 有数据
		if err := dumpBinlog(t.NewPath, bw); err != nil {
			return err
		}
	}

	if t.DDL != nil { // 有ddl语句
		if err := bw.WriteDDL(t.DDL); err != nil {
			return err
		}
	}

	return bw.WriteRotateEvent()
}

// dumpBinlog dump base64 file just like binlog output
func dumpBinlog(dbPath string, bw *blog.BinlogWriter) error {
	log.Debug("dbPath ", dbPath)
	db, err := leveldb.OpenFile(dbPath, nil)
	if err != nil {
		return err
	}
	defer db.Close()

	// 分三次读取 文件否则会有大量的table map event重复
	hi, hd, hu := have(db)

	if hi {
		if err := dumpData(db, bw, WriteRowsEvent); err != nil {
			return err
		}
	}

	if hd {
		if err := dumpData(db, bw, DeleteRowsEvent); err != nil {
			return err
		}
	}

	if hu {
		if err := dumpData(db, bw, UpdateRowsEvent); err != nil {
			return err
		}
	}

	return nil
}

// have 是否包含insert， delete, update
func have(db *leveldb.DB) (bool, bool, bool) {
	iter := db.NewIterator(nil, nil)
	defer iter.Release()

	// have insert
	hi := false

	// have delete
	hd := false

	// have update
	hu := false
	for iter.Next() {
		unit := &pb.BytesUnit{}
		proto.Unmarshal(iter.Value(), unit)
		if hi && hd && hu {
			// 都存在 則直接返回
			return hi, hd, hu
		}

		switch unit.Tp {
		case WriteRowsEvent:
			hi = true
		case DeleteRowsEvent:
			hd = true
		case UpdateRowsEvent:
			hu = true
		}
	}

	return hi, hd, hu
}

// dumpData 从leveldb中 dump update数据 写入到binlog 文件当中
func dumpData(db *leveldb.DB, bw *blog.BinlogWriter, t pb.EVENT_TYPE) error {
	iter := db.NewIterator(nil, nil)
	defer iter.Release()

	if err := bw.WriteQueryBegin(); err != nil {
		log.Error(err)
		return err
	}

	if err := bw.WriteTableMapEvent(); err != nil {
		log.Error(err)
		return err
	}

	// row values
	rows := bytes.NewBuffer(nil)

	// before bit map
	var bbm []byte

	// after bit map
	var abm []byte

	// header length
	var hl = len(bw.RowsHeader.Header)

	for iter.Next() {
		unit := &pb.BytesUnit{}
		if err := proto.Unmarshal(iter.Value(), unit); err != nil {
			log.Error(err)
			return err
		}
		//log.Debug("key ", string(unit.Key), ", binlog event type ", unit.Tp)

		// limitation of package size for MySQL : 1Mls
		if unit.Tp == t {
			// row
			r := bytes.NewBuffer(nil)

			switch t {
			case DeleteRowsEvent:
				// insert
				if bbm == nil {
					bbm = unit.Before.ColumnBitmap

					rows.Write(bbm)
				}
				r.Write(unit.Before.Value)

			case WriteRowsEvent:
				if abm == nil {
					abm = unit.After.ColumnBitmap

					rows.Write(abm)
				}
				r.Write(unit.After.Value)

			case UpdateRowsEvent:
				if abm == nil {
					abm = unit.After.ColumnBitmap
					bbm = unit.Before.ColumnBitmap

					rows.Write(bbm)
					rows.Write(abm)
				}
				// write value to row
				r.Write(unit.Before.Value)
				r.Write(unit.After.Value)

			}

			size := replication.EventHeaderSize + hl + rows.Len() + r.Len()
			if size > blog.BinlogBufferSize {
				// 大于 binlog cache size 完成 binlog write rows 事件

				// write event header
				if err := bw.WriteEventHeader(size-r.Len(), replication.EventType(t)); err != nil {
					log.Error(err)
					return err
				}

				// write row header
				if err := bw.WriteRowsHeader(blog.StmtEndFlag>>1, replication.EventType(t)); err != nil {
					log.Error(err)
					return err
				}

				// write row data
				if err := bw.WriteRowsEvent(rows.Bytes()); err != nil {
					log.Error(err)
					return err
				}

				// write event footer
				if err := bw.WriteEventFooter(); err != nil {
					log.Error(err)
					return err
				}

				// reset vars
				rows.Reset()

				switch t {
				case DeleteRowsEvent:
					// write after
					bbm = unit.Before.ColumnBitmap

					rows.Write(bbm)

				case WriteRowsEvent:
					// write before
					abm = unit.After.ColumnBitmap

					rows.Write(abm)

				case UpdateRowsEvent:
					// write before & after
					bbm = unit.Before.ColumnBitmap
					abm = unit.After.ColumnBitmap

					rows.Write(bbm)
					rows.Write(abm)
				}
				rows.Write(r.Bytes())
			} else {
				// 寫入數據
				rows.Write(r.Bytes())
			}
		}
	}

	// header size + rows header length + data length
	s := replication.EventHeaderSize + hl + rows.Len()

	// write event header
	if err := bw.WriteEventHeader(s, replication.EventType(t)); err != nil {
		return err
	}

	// write row header
	if err := bw.WriteRowsHeader(blog.StmtEndFlag, replication.EventType(t)); err != nil {
		return err
	}

	// write row data
	if err := bw.WriteRowsEvent(rows.Bytes()); err != nil {
		return err
	}

	if err := bw.WriteEventFooter(); err != nil {
		return err
	}

	// write commit event
	return bw.WriteXIDEvent()
}
