package handler

import (
	"bytes"
	"github.com/mysql-binlog/common/inter"
	"github.com/mysql-binlog/merge/binlog"
	"github.com/zssky/log"
	"os"
	"sync"

	"github.com/mysql-binlog/common/pb"
	"github.com/mysql-binlog/siddontang/go-mysql/replication"
	"github.com/golang/protobuf/proto"
	"github.com/syndtr/goleveldb/leveldb"

	blog "github.com/mysql-binlog/common/log"
)

// WriteData 数据转成 binlog bytes 写入IFile
func WriteData(path string, bw *blog.BinlogWriter, f inter.IFile, wg *sync.WaitGroup) error {
	defer func() {
		// 删除拷贝的数据文件
		log.Info("删除文件夹 ", path)
		os.RemoveAll(path)
		wg.Done()
	}()

	// take new writer
	bw.W = bytes.NewBuffer(nil)
	defer bw.Clear()

	if err := DumpBinlog(path, bw); err != nil {
		return err
	}

	// continue write data to
	_, err := f.Write(bw.W.(*bytes.Buffer).Bytes())
	return err
}

// WriteDDL 数据转成 binlog bytes 写入IFile
func WriteDDL(ddl []byte, bw *blog.BinlogWriter, f inter.IFile, wg *sync.WaitGroup) error {
	defer wg.Done()

	// take new writer
	bw.W = bytes.NewBuffer(nil)
	defer bw.Clear()

	if err := bw.WriteDDL(ddl); err != nil {
		return err
	}

	// continue write data to
	_, err := f.Write(bw.W.(*bytes.Buffer).Bytes())
	return err
}

// DumpBinlog dump base64 file just like binlog output
func DumpBinlog(dbPath string, bw *blog.BinlogWriter) error {
	log.Debug("dbPath ", dbPath)
	db, err := leveldb.OpenFile(dbPath, nil)
	if err != nil {
		return err
	}
	defer db.Close()

	// 分三次读取 文件否则会有大量的table map event重复
	hi, hd, hu, err := have(db)
	if err != nil {
		return err
	}

	if hi {
		if err := dumpData(db, bw, binlog.WriteRowsEvent); err != nil {
			return err
		}
	}

	if hd {
		if err := dumpData(db, bw, binlog.DeleteRowsEvent); err != nil {
			return err
		}
	}

	if hu {
		if err := dumpData(db, bw, binlog.UpdateRowsEvent); err != nil {
			return err
		}
	}
	return nil
}

// have 是否包含insert， delete, update
func have(db *leveldb.DB) (bool, bool, bool, error) {
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
		if err := proto.Unmarshal(iter.Value(), unit); err != nil {
			return false, false, false, err
		}
		log.Debug("key ", string(unit.Key), ", binlog event type ", unit.Tp)

		if hi && hd && hu {
			// 都存在 則直接返回
			return hi, hd, hu, nil
		}

		switch unit.Tp {
		case binlog.WriteRowsEvent:
			hi = true
		case binlog.DeleteRowsEvent:
			hd = true
		case binlog.UpdateRowsEvent:
			hu = true
		}
	}

	return hi, hd, hu, nil
}

// dumpData 从leveldb中 dump update数据 写入到binlog 文件当中
func dumpData(db *leveldb.DB, bw *blog.BinlogWriter, t pb.EVENT_TYPE) error {
	iter := db.NewIterator(nil, nil)
	defer iter.Release()

	if err := bw.WriteQueryBegin(); err != nil {
		return err
	}

	if err := bw.WriteTableMapEvent(); err != nil {
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
			return err
		}
		log.Debug("key ", string(unit.Key), ", binlog event type ", unit.Tp)

		// limitation of package size for MySQL : 1Mls
		if unit.Tp == t {
			// row
			r := bytes.NewBuffer(nil)

			switch t {
			case binlog.DeleteRowsEvent:
				// insert
				if bbm == nil {
					bbm = unit.Before.ColumnBitmap

					rows.Write(bbm)
				}
				r.Write(unit.Before.Value)

			case binlog.WriteRowsEvent:
				if abm == nil {
					abm = unit.After.ColumnBitmap

					rows.Write(abm)
				}
				r.Write(unit.After.Value)

			case binlog.UpdateRowsEvent:
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
					return err
				}

				// write row header
				if err := bw.WriteRowsHeader(blog.StmtEndFlag>>1, replication.EventType(t)); err != nil {
					return err
				}

				// write row data
				if err := bw.WriteRowsEvent(rows.Bytes()); err != nil {
					return err
				}

				// write event footer
				if err := bw.WriteEventFooter(); err != nil {
					return err
				}

				// reset vars
				rows.Reset()

				switch t {
				case binlog.DeleteRowsEvent:
					// write after
					bbm = unit.Before.ColumnBitmap

					rows.Write(bbm)

				case binlog.WriteRowsEvent:
					// write before
					abm = unit.After.ColumnBitmap

					rows.Write(abm)

				case binlog.UpdateRowsEvent:
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
