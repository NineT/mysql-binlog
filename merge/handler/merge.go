package handler

import (
	"bytes"
	"fmt"
	"github.com/mysql-binlog/common/final"
	"os"
	"strings"
	"sync"

	"github.com/mysql-binlog/siddontang/go-mysql/replication"
	"github.com/satori/go.uuid"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/zssky/log"

	cdb "github.com/mysql-binlog/common/db"
	"github.com/mysql-binlog/common/inter"
	blog "github.com/mysql-binlog/common/log"
	"github.com/mysql-binlog/common/regx"

	"github.com/mysql-binlog/merge/binlog"
	"github.com/mysql-binlog/merge/mode"
)

// DumpMode dump mode including remote and local
type DumpMode int8

const (
	// DumpModeLocal local dump which host == ip
	DumpModeLocal DumpMode = 0x1
	// DumpModeRemote remote dump which host != ip
	DumpModeRemote DumpMode = 0x2
)

// MergeConfig merge configuration
type MergeConfig struct {
	table          string                              // 注意table是传值 不是引用 临时变量 存储事件对应的表名
	desc           *replication.FormatDescriptionEvent // desc format desc event
	header         *replication.EventHeader            // common event header
	query          *replication.QueryEvent             // query event
	haveHeaderDone bool                                // haveHeaderDone 判断header 是否已经写入
	lastFlag       uint16                              // header flag
	RWg            *sync.WaitGroup                     // Routine wait group
	After          *final.After                        // After math for merge
	TempPath       string                              // 数据 kv 存储路径 临时存储路径
	SnapshotPath   string                              // 数据 kv 存储路径 快照存储路径
	Writer         inter.IFile                         // Writer 文件写入
	DumpMode       DumpMode                            // DumpMode dump 模式 {local,remote}

	StartPosition *inter.BinlogPosition // StartPosition 初始位置
	StopPosition  *inter.BinlogPosition // StopPosition 結束位置

	TableHandlers   map[string]*binlog.TableEventHandler // tableHandlers 每张表的操作
	DumpMySQLConfig *cdb.MetaConf                        // DumpMySQLConfig dump mysql 的操作
	Wgs             map[string]*sync.WaitGroup           // Wgs wait group 用来做携程之间的等待操作
}

// Start start merge
func (mc *MergeConfig) Start() {
	defer mc.Close()

	mc.haveHeaderDone = false

	var dump mode.IBinlogDump
	switch mc.DumpMode {
	case DumpModeLocal:
		dump = &mode.LocalMode{
			Path:      mc.DumpMySQLConfig.GetBinlogPath(),
			LatestPos: mc.StartPosition,
		}
	case DumpModeRemote:
		dump = &mode.RemoteMode{
			LatestPos: mc.StartPosition,
			Config:    mc.DumpMySQLConfig,
		}
	}

	dump.Handle(mc.EventHandler, mc.After)
}

// Close close when merge finished
func (mc *MergeConfig) Close() {
	// wait for routine execute over
	mc.RWg.Wait()

	if err := mc.flushTotal(); err != nil {
		log.Error(err)
	}

	// need to wait again
	mc.RWg.Wait()

	// remove temporary path
	if err := os.RemoveAll(mc.TempPath + mc.DumpMySQLConfig.Host); err != nil {
		log.Error(err)
	}

	// remove snapshot path
	if err := os.RemoveAll(mc.SnapshotPath + mc.DumpMySQLConfig.Host); err != nil {
		log.Error(err)
	}

	if err := mc.Writer.Close(); err != nil {
		log.Error(err)
	}
}

// EventHandler handle event
func (mc *MergeConfig) EventHandler(ev *replication.BinlogEvent) bool {
	defer func() {
		if err := recover(); err != nil {
			// recover error no next time arrive here
			mc.After.Errs <- err
			return
		}
	}()

	curr := ev.Header.Timestamp
	mc.StartPosition.Timestamp = int64(curr)
	mc.StartPosition.BinlogPos = ev.Header.LogPos
	mc.lastFlag = ev.Header.Flags

	// arrived to the final position
	if mc.isFinalPos() {
		return false
	}

	//ev.RowHeader.Timestamp
	switch ev.Header.EventType {
	case replication.UNKNOWN_EVENT:
	case replication.START_EVENT_V3:
	case replication.QUERY_EVENT:
		qe, _ := ev.Event.(*replication.QueryEvent)
		mc.query = qe

		log.Debug(string(qe.Query))

		switch strings.ToUpper(string(qe.Query)) {
		case "BEGIN":
		case "COMMIT":
		case "ROLLBACK":
		case "SAVEPOINT":
		default:
			// here ddl may have two statement
			log.Info("schema ", string(qe.Schema), " ddl ", string(qe.Query))

			// wait for the previous ddl is done
			mc.RWg.Wait()
			for _, ddl := range bytes.Split(qe.Query, []byte(";")) {
				if tbs, matched := regx.Parse(qe.Schema, ddl); matched { // 匹配表名成功
					for _, tb := range tbs {
						flag := takeDDL(ev, ev.Header.Flags)
						if err := mc.flushTableData(tb, flag, ddl); err != nil {
							panic(err)
						}
					}
				} else {
					// no matched table then just write ddl
					if err := mc.refreshData("", ev.Header.Flags, ddl, nil); err != nil {
						panic(err)
					}
				}
			}
		}
	case replication.STOP_EVENT:
	case replication.ROTATE_EVENT:
		re, _ := ev.Event.(*replication.RotateEvent)
		log.Debug("next binlog file name " + string(re.NextLogName))
		mc.StartPosition.BinlogFile = string(re.NextLogName)
		mc.StartPosition.BinlogPos = 4

	case replication.INTVAR_EVENT:
	case replication.LOAD_EVENT:
	case replication.SLAVE_EVENT:
	case replication.CREATE_FILE_EVENT:
	case replication.APPEND_BLOCK_EVENT:
	case replication.EXEC_LOAD_EVENT:
	case replication.DELETE_FILE_EVENT:
	case replication.NEW_LOAD_EVENT:
	case replication.RAND_EVENT:
	case replication.USER_VAR_EVENT:
	case replication.FORMAT_DESCRIPTION_EVENT:
		// each binlog file have one format description event then
		if mc.desc == nil {
			// 保存当前事件
			mc.desc = ev.Event.(*replication.FormatDescriptionEvent)
			mc.header = ev.Header
			if err := mc.writeLogHeader(); err != nil {
				panic(err)
			}
			break
		}

		nfd := ev.Event.(*replication.FormatDescriptionEvent)
		if !isFormatDescEventEqual(nfd, // 判断事件类型以及 版本号是否一致
			mc.desc) {
			log.Debug("format desc event is not equals")
			// 将所有数据重新写入文件
			if err := mc.flushTotal(); err != nil {
				panic(err)
			}

			// 保存当前事件
			mc.desc = nfd
			// 重置版本号
			binlog.Versioned = false
		}

	case replication.XID_EVENT:
	case replication.BEGIN_LOAD_QUERY_EVENT:
	case replication.EXECUTE_LOAD_QUERY_EVENT:
	case replication.TABLE_MAP_EVENT:
		tme, _ := ev.Event.(*replication.TableMapEvent)
		mc.table = fmt.Sprintf("%s.%s", inter.CharStd(string(tme.Schema)),
			inter.CharStd(string(tme.Table)))

		if _, ok := mc.TableHandlers[mc.table]; !ok {
			wg := &sync.WaitGroup{}

			evh := &binlog.TableEventHandler{
				Table: mc.table,
				Meta:  mc.DumpMySQLConfig.NewTableMeta(mc.table),
				Db: cdb.InitDb(&inter.AbsolutePath{
					TmpSrc: mc.TempPath,
					Host:   mc.DumpMySQLConfig.Host,
					Table:  mc.table,
				}),

				EventChan: make(chan *replication.BinlogEvent, 64),
				Wg:        wg,
			}

			// final position  remove crc32 encode 默认打开 去除悬挂引用 copy 新数据
			fp := len(ev.RawData) - blog.CRC32Size
			if mc.desc.ChecksumAlgorithm != replication.BINLOG_CHECKSUM_ALG_CRC32 {
				fp = len(ev.RawData)
			}
			bt := make([]byte, fp-replication.EventHeaderSize)
			copy(bt, ev.RawData[replication.EventHeaderSize:fp])
			evh.TableMapEvent = bt

			mc.TableHandlers[mc.table] = evh
			mc.Wgs[mc.table] = wg

			// 事件队列
			go evh.HandleLogEvent()
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

		binlog.SetEventVersion(ev.Header.EventType) // set version
		h := mc.TableHandlers[mc.table]
		// add 1
		h.Wg.Add(1)

		h.EventChan <- ev

	case replication.INCIDENT_EVENT:
	case replication.HEARTBEAT_EVENT:
	case replication.IGNORABLE_EVENT:
	case replication.ROWS_QUERY_EVENT: // query ddl
	case replication.GTID_EVENT:
		ge := ev.Event.(*replication.GTIDEvent)
		u, _ := uuid.FromBytes(ge.SID)
		if err := mc.StartPosition.GTIDSet.Update(fmt.Sprintf("%s:%d", u.String(), ge.GNO)); err != nil {
			log.Error("gtid update error ", err)
			panic(err)
		}

	case replication.ANONYMOUS_GTID_EVENT:
	case replication.PREVIOUS_GTIDS_EVENT:
	default:
	}

	return true
}

// isFormatDescEventEqual 判断两个binlog 描述事件是否一致
func isFormatDescEventEqual(ev1 *replication.FormatDescriptionEvent, ev2 *replication.FormatDescriptionEvent) bool {
	return ev1.Version == ev2.Version &&
		bytes.Equal(ev1.ServerVersion, ev2.ServerVersion) &&
		ev1.EventHeaderLength == ev2.EventHeaderLength &&
		bytes.Equal(ev1.EventTypeHeaderLengths, ev2.EventTypeHeaderLengths)
}

// takeDDL take ddl sql
func takeDDL(ev *replication.BinlogEvent, flag uint16) uint16 {
	qe := ev.Event.(*replication.QueryEvent)
	if ev.Header.Flags&inter.LogEventSuppressUseF == 0 && qe.Schema != nil && len(qe.Schema) != 0 {
		// should use db
		flag = inter.LogEventSuppressUseF
	}

	return flag
}

// flushTableData flush data to sql file
func (mc *MergeConfig) flushTableData(tb []byte, flag uint16, ddl []byte) error {
	table := inter.CharStd(string(tb))

	log.Debug("current table ", table)

	h, _ := mc.TableHandlers[table]
	if h != nil {
		mc.Wgs[table].Wait() // wait for data package handled over
	}

	return mc.refreshData(table, flag, ddl, h)
}

// flushTotal flush data to sql file
func (mc *MergeConfig) flushTotal() error {
	// must wait for all ddl and data write to file
	mc.RWg.Wait()

	for key, h := range mc.TableHandlers {
		// wait for merged data over
		mc.Wgs[key].Wait()
		log.Debug("dump base64 code ", key)

		if err := mc.refreshData(key, mc.lastFlag, nil, h); err != nil {
			return err
		}
	}
	return nil
}

// refreshData 刷新数据
func (mc *MergeConfig) refreshData(table string, flag uint16, ddl []byte, h *binlog.TableEventHandler) error {
	ap := &inter.AbsolutePath{
		TmpSrc:   mc.TempPath,
		TmpDst:   mc.SnapshotPath,
		FileType: inter.FileType(""),
		Host:     mc.DumpMySQLConfig.Host,
		Table:    table,
	}

	haveData := false
	if h != nil {
		haveData = isNotEmpty(h.Db)
	}

	isDDLEmpty := false
	if ddl == nil || len(ddl) == 0 {
		isDDLEmpty = true
	}

	// haveData data and ddl as well
	if haveData && !isDDLEmpty {
		// data first ddl second
		nd, np, err := mc.newCopiedDb(ap)
		if err != nil {
			return err
		}
		h.Db = nd

		mc.RWg.Add(2)
		go func(errs chan interface{}) {
			// execute in order
			if err := WriteData(np, mc.newLogWriter(ap, h), mc.Writer, mc.RWg); err != nil {
				errs <- err
				return
			}
			if err := WriteDDL(ddl, mc.newLogWriter(ap, nil), mc.Writer, mc.RWg); err != nil {
				errs <- err
				return
			}
		}(mc.After.Errs)
		return nil
	}

	// only write db data and exchange db
	if haveData {
		// haveData data
		// need to close first
		if err := h.Db.Close(); err != nil {
			return err
		}

		// exchange db
		db, err := mc.exchange(ap, mc.Writer, h)
		if err != nil {
			return err
		}

		h.Db = db
	}

	// only write ddl
	if !isDDLEmpty {
		// write ddl
		mc.RWg.Add(1)
		go func(errs chan interface{}) {
			if err := WriteDDL(ddl, mc.newLogWriter(ap, nil), mc.Writer, mc.RWg); err != nil {
				errs <- err
				return
			}
		}(mc.After.Errs)
	}

	return nil
}

func (mc *MergeConfig) writeLogHeader() error {
	log.Debug("write binlog file header")
	bw := &blog.BinlogWriter{
		EventHeader: mc.header.Copy(),
		FormatDesc:  mc.desc,
		f:           mc.Writer,
	}

	defer bw.Clear()

	// write binlog header
	if err := bw.WriteBinlogFileHeader(); err != nil {
		return err
	}

	// write Desc event
	if err := bw.WriteDescEvent(); err != nil {
		return err
	}

	// flag for header
	mc.haveHeaderDone = true
	return nil
}

func (mc *MergeConfig) isFinalPos() bool {
	start := mc.StartPosition
	stop := mc.StopPosition
	if stop.Timestamp != 0 && stop.Timestamp <= start.Timestamp {
		return true
	}

	if stop.BinlogFile != "" && strings.EqualFold(stop.BinlogFile, start.BinlogFile) && stop.BinlogPos <= start.BinlogPos {
		return true
	}

	if stop.GTIDSet != nil && start.GTIDSet != nil && start.GTIDSet.Contain(stop.GTIDSet) {
		return true
	}

	return false
}

// exchange : exchange new path
func (mc *MergeConfig) exchange(ap *inter.AbsolutePath, f inter.IFile, h *binlog.TableEventHandler) (*leveldb.DB, error) {
	// new db and new path
	nd, np, err := mc.newCopiedDb(ap)
	if err != nil {
		return nil, err
	}

	// multi-goroutine write in once without order is ok because each table write in 1 block
	mc.RWg.Add(1)

	go func() {
		if err := WriteData(np, mc.newLogWriter(ap, h), f, mc.RWg); err != nil {

		}
	}()

	return nd, nil
}

func (mc *MergeConfig) newCopiedDb(ap *inter.AbsolutePath) (*leveldb.DB, string, error) {
	// src path
	src := ap.TmpSourcePath()

	// dst path
	dst := ap.TmpDstPath()
	log.Debug("source path ", src, ", dest path ", dst)

	// copy db
	nd, np, err := cdb.CopyDb(src, dst)
	if err != nil {
		log.Error(err)
		return nil, "", err
	}

	log.Debug("move data from ", src, " to ", np)

	return nd, np, nil
}

// newLogWriter create log writer
func (mc *MergeConfig) newLogWriter(ap *inter.AbsolutePath, h *binlog.TableEventHandler) *blog.BinlogWriter {
	bw := &blog.BinlogWriter{
		EventHeader: mc.header.Copy(),
		NextPrefix:  []byte(ap.NextPrefix()),
	}

	// 并发写入 文件写入是完整的块操作 不会出现块与块之间异常
	if h != nil {
		tme := make([]byte, len(h.TableMapEvent))
		copy(tme, h.TableMapEvent)

		bw.TableMapEvent = tme
		bw.XID = h.XID

		if h.RowsHeader != nil {
			bw.RowsHeader = h.RowsHeader
		}
	}

	if mc.query != nil {
		bw.QueryEvent = mc.query.Copy().(*replication.QueryEvent)
	}

	return bw
}

// isNotEmpty false: 表示有數據， 否則表示無數據
func isNotEmpty(db *leveldb.DB) bool {
	iter := db.NewIterator(nil, nil)
	defer iter.Release()
	return iter.Next()
}
