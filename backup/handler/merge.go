package handler

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/mysql-binlog/siddontang/go-mysql/replication"
	"github.com/satori/go.uuid"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/zssky/log"

	cdb "github.com/mysql-binlog/common/db"
	"github.com/mysql-binlog/common/final"
	"github.com/mysql-binlog/common/inter"
	blog "github.com/mysql-binlog/common/log"
	"github.com/mysql-binlog/common/regx"

	"github.com/mysql-binlog/backup/binlog"
	"github.com/mysql-binlog/backup/mode"
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
	startTime     uint32        // startTime  : first binlog time stamp
	lastEventTime uint32        // last event time:
	lastFlags     uint16        // last flag
	table         string        // 注意table是传值 不是引用 临时变量 存储事件对应的表名
	FinalTime     int64         // FinalTime 终止时间 表示dump到此结束不在继续dump
	TempPath      string        // 数据 kv 存储路径 临时存储路径
	SnapshotPath  string        // 数据 kv 存储路径 快照存储路径
	Client        inter.IClient // Client 存储客户端
	After         *final.After  // After math for merge

	DumpMode DumpMode // DumpMode dump 模式 {local,remote}

	Period inter.FileType // Period 周期

	StartPosition  *inter.BinlogPosition // StartPosition 初始位置
	LatestPosition *inter.BinlogPosition // LatestPosition 最新位置

	header *replication.EventHeader            // event header
	desc   *replication.FormatDescriptionEvent // desc event
	query  *replication.QueryEvent             // query event

	TableHandlers   map[string]*binlog.TableEventHandler // TableHandlers 每张表的操作
	DumpMySQLConfig *cdb.MetaConf                        // DumpMySQLConfig dump mysql 的操作
	MetaMySQLConfig *cdb.MetaConf                        // MetaMySQLConfig 元数据信息
	Wgs             map[string]*sync.WaitGroup           // Wgs wait group 用来做携程之间的等待操作
}

// Start start merge
func (mc *MergeConfig) Start() {
	defer mc.Close()

	var dump mode.IBinlogDump
	switch mc.DumpMode {
	case DumpModeLocal:
		dump = &mode.LocalMode{
			Path:      mc.DumpMySQLConfig.GetBinlogPath(),
			LatestPos: mc.LatestPosition,
		}
	case DumpModeRemote:
		dump = &mode.RemoteMode{
			LatestPos: mc.LatestPosition,
			Config:    mc.DumpMySQLConfig,
		}
	}

	// start dump
	dump.Handle(mc.EventHandler, mc.After)
}

// Close close when merge finished
func (mc *MergeConfig) Close() {
	if err := mc.flushTotal(mc.lastFlags, mc.lastEventTime); err != nil {
		log.Error(err)
	}

	mc.closeHandler()

	if err := os.RemoveAll(mc.TempPath + mc.DumpMySQLConfig.Host); err != nil {
		log.Error(err)
	}

	// update meta
	mc.MetaMySQLConfig.UpdatePosition(mc.LatestPosition)
}

// EventHandler handle event false: arrived the terminal time, true: means continue
func (mc *MergeConfig) EventHandler(ev *replication.BinlogEvent) bool {
	defer func() {
		if err := recover(); err != nil {
			// recover error no next time arrive here
			mc.After.Errs <- err
			return
		}

		if err := mc.tick(ev); err != nil {
			mc.After.Errs <- err
			return
		}

		// save Last Event Time
		mc.lastEventTime = ev.Header.Timestamp
		mc.lastFlags = ev.Header.Flags
	}()

	curr := ev.Header.Timestamp
	if curr != 0 && mc.startTime == 0 {
		mc.startTime = curr
	}

	// 大于dump 停止时间
	if int64(ev.Header.Timestamp)-mc.FinalTime >= 0 {
		log.Warn("time is arrived %d", mc.FinalTime)
		// means finished
		return false
	}

	//ev.RowHeader.Timestamp
	switch ev.Header.EventType {
	case replication.UNKNOWN_EVENT:
	case replication.START_EVENT_V3:
	case replication.QUERY_EVENT:
		qe, _ := ev.Event.(*replication.QueryEvent)
		mc.query = qe

		switch strings.ToUpper(string(qe.Query)) {
		case "BEGIN":
		case "COMMIT":
		case "ROLLBACK":
		case "SAVEPOINT":
		default:
			// here ddl may have two statement
			log.Info("schema ", string(qe.Schema), " ddl ", string(qe.Query))

			for _, ddl := range bytes.Split(qe.Query, []byte(";")) {
				if tbs, matched := regx.Parse(ddl, qe.Schema); matched { // 匹配表名成功
					for _, tb := range tbs {
						flag := takeDDL(ev, ev.Header.Flags)
						if err := mc.flushTableData(curr, string(tb), flag, ddl); err != nil {
							panic(err)
						}
					}
				} else {
					// not match tables ddl sql then just write to common packet
					log.Debug("not matched")
					if err := mc.flushTableData(curr, inter.Public, ev.Header.Flags, ddl); err != nil {
						panic(err)
					}
				}
			}
		}
	case replication.STOP_EVENT:
	case replication.ROTATE_EVENT:
		re, _ := ev.Event.(*replication.RotateEvent)
		log.Debug("next binlog file name " + string(re.NextLogName))
		mc.LatestPosition.BinlogFile = string(re.NextLogName)
		mc.LatestPosition.BinlogPos = 4

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
			break
		}

		nfd := ev.Event.(*replication.FormatDescriptionEvent)
		if !isFormatDescEventEqual(nfd, // 判断事件类型以及 版本号是否一致
			mc.desc) {
			// 将所有数据重新写入文件
			if err := mc.flushTotal(mc.lastFlags, mc.lastEventTime); err != nil {
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

		h, ok := mc.TableHandlers[mc.table]
		if !ok {
			mc.newHandler(uint32(curr), mc.table)
			mc.TableHandlers[mc.table].TableMapEvent = mc.CopyMapEvent(ev)
		} else if h.TableMapEvent == nil {
			// have already create handler check event map is ok
			h.TableMapEvent = mc.CopyMapEvent(ev)
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
		if err := mc.LatestPosition.GTIDSet.Update(fmt.Sprintf("%s:%d", u.String(), ge.GNO)); err != nil {
			panic(err)
		}

	case replication.ANONYMOUS_GTID_EVENT:
	case replication.PREVIOUS_GTIDS_EVENT:
	default:
	}

	//
	if ev.Header != nil && ev.Header.LogPos != 0 {
		mc.LatestPosition.BinlogPos = ev.Header.LogPos
		mc.LatestPosition.Timestamp = int64(curr)
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
func (mc *MergeConfig) flushTableData(curr uint32, tb string, flags uint16, ddl []byte) error {
	table := inter.CharStd(tb)

	log.Debug("table ", table, " to flush")
	h, ok := mc.TableHandlers[table]
	if ok { // table key 存在
		log.Debug("wait for table ", table, " to accomplished")
		mc.Wgs[table].Wait() // 等待关闭
	} else {
		// get table handler
		mc.newHandler(curr, table)
		h = mc.TableHandlers[table]
	}

	return mc.flushData(table, flags, ddl, curr, h)
}

// flushData flush data for storage like cfs
func (mc *MergeConfig) flushData(table string, flags uint16, ddl []byte, end uint32, h *binlog.TableEventHandler) error {
	switch mc.Period {
	case inter.SECOND:
		if err := mc.refreshSecDb(table, flags, ddl, end, h); err != nil {
			return err
		}

		if err := mc.refreshMinDb(table, flags, ddl, end, h); err != nil {
			return err
		}

		if err := mc.refreshHourDb(table, flags, ddl, end, h); err != nil {
			return err
		}

		if err := mc.refreshDayDb(table, flags, ddl, end, h); err != nil {
			return err
		}
	case inter.MINUTE:

		if err := mc.refreshMinDb(table, flags, ddl, end, h); err != nil {
			return err
		}

		if err := mc.refreshHourDb(table, flags, ddl, end, h); err != nil {
			return err
		}

		if err := mc.refreshDayDb(table, flags, ddl, end, h); err != nil {
			return err
		}
	case inter.HOUR:
		if err := mc.refreshHourDb(table, flags, ddl, end, h); err != nil {
			return err
		}

		if err := mc.refreshDayDb(table, flags, ddl, end, h); err != nil {
			return err
		}
	case inter.DAY:

		if err := mc.refreshDayDb(table, flags, ddl, end, h); err != nil {
			return err
		}
	}

	return nil
}

// flushTotal flush data to sql file
func (mc *MergeConfig) flushTotal(flags uint16, end uint32) error {
	for key, h := range mc.TableHandlers {
		// wait for merged data over

		log.Debug("wait for table ", key, " to accomplished")
		mc.Wgs[key].Wait()

		if err := mc.flushData(key, flags, nil, end, h); err != nil {
			return err
		}
	}
	return nil
}

// initDb 根据模式 周期初始化db
func (mc *MergeConfig) initDb(period inter.FileType, table string, h *binlog.TableEventHandler) {
	switch period {
	case inter.DAY:
		// 备份周期为天
		h.DayDb = cdb.InitDb(&inter.AbsolutePath{
			TmpSrc:   mc.TempPath,
			Host:     mc.DumpMySQLConfig.Host,
			Table:    table,
			FileType: inter.DAY,
		})

	case inter.HOUR:
		// 备份周期为小时
		h.HourDb = cdb.InitDb(&inter.AbsolutePath{
			TmpSrc:   mc.TempPath,
			Host:     mc.DumpMySQLConfig.Host,
			Table:    table,
			FileType: inter.HOUR,
		})
		h.DayDb = cdb.InitDb(&inter.AbsolutePath{
			TmpSrc:   mc.TempPath,
			Host:     mc.DumpMySQLConfig.Host,
			Table:    table,
			FileType: inter.DAY,
		})

	case inter.MINUTE:
		// 备份周期为分钟
		h.MinDb = cdb.InitDb(&inter.AbsolutePath{
			TmpSrc:   mc.TempPath,
			Host:     mc.DumpMySQLConfig.Host,
			Table:    table,
			FileType: inter.MINUTE,
		})
		h.HourDb = cdb.InitDb(&inter.AbsolutePath{
			TmpSrc:   mc.TempPath,
			Host:     mc.DumpMySQLConfig.Host,
			Table:    table,
			FileType: inter.HOUR,
		})
		h.DayDb = cdb.InitDb(&inter.AbsolutePath{
			TmpSrc:   mc.TempPath,
			Host:     mc.DumpMySQLConfig.Host,
			Table:    table,
			FileType: inter.DAY,
		})

	case inter.SECOND:
		// 备份周期为秒
		h.SecDb = cdb.InitDb(&inter.AbsolutePath{
			TmpSrc:   mc.TempPath,
			Host:     mc.DumpMySQLConfig.Host,
			Table:    table,
			FileType: inter.SECOND,
		})
		h.MinDb = cdb.InitDb(&inter.AbsolutePath{
			TmpSrc:   mc.TempPath,
			Host:     mc.DumpMySQLConfig.Host,
			Table:    table,
			FileType: inter.MINUTE,
		})
		h.HourDb = cdb.InitDb(&inter.AbsolutePath{
			TmpSrc:   mc.TempPath,
			Host:     mc.DumpMySQLConfig.Host,
			Table:    table,
			FileType: inter.HOUR,
		})
		h.DayDb = cdb.InitDb(&inter.AbsolutePath{
			TmpSrc:   mc.TempPath,
			Host:     mc.DumpMySQLConfig.Host,
			Table:    table,
			FileType: inter.DAY,
		})

	}
}

// CopyMapEvent copy table map event
func (mc *MergeConfig) CopyMapEvent(ev *replication.BinlogEvent) []byte {
	// final position  remove crc32 encode 默认打开 去除悬挂引用 copy 新数据
	fp := len(ev.RawData) - blog.CRC32Size
	if mc.desc.ChecksumAlgorithm != replication.BINLOG_CHECKSUM_ALG_CRC32 {
		fp = len(ev.RawData)
	}
	bt := make([]byte, fp-replication.EventHeaderSize)
	copy(bt, ev.RawData[replication.EventHeaderSize:fp])
	return bt
}

// newHandler generate table handler
func (mc *MergeConfig) newHandler(curr uint32, table string) {
	wg := &sync.WaitGroup{}

	evh := &binlog.TableEventHandler{
		Table: table,
		Meta:  mc.DumpMySQLConfig.NewTableMeta(table),
		After: mc.After,

		LastDayTime:  curr,
		LastHourTime: curr,
		LastMinTime:  curr,
		LastSecTime:  curr,

		// channel init
		EventChan:    make(chan *replication.BinlogEvent, 64),
		DayRestChan:  make(chan *binlog.Rest, 64),
		HourRestChan: make(chan *binlog.Rest, 64),
		MinRestChan:  make(chan *binlog.Rest, 64),
		SecRestChan:  make(chan *binlog.Rest, 64),

		// 需要拷贝 一个format desc event 以防更新产生变化
		FormatDesc:  mc.desc.Copy().(*replication.FormatDescriptionEvent),
		EventHeader: mc.header.Copy(),

		Wg: wg,
	}

	// if query event is no nil
	if mc.query != nil {
		evh.QueryEvent = mc.query.Copy().(*replication.QueryEvent)
	}

	// initDb 初始化db
	mc.initDb(mc.Period, table, evh)

	mc.TableHandlers[table] = evh
	mc.Wgs[table] = wg

	// 事件队列
	go evh.HandleLogEvent()

	// 顺序处理day 日志文件
	go evh.HandleDayBinlogFile()

	// 顺序处理hour 日志文件
	go evh.HandleHourBinlogFile()

	// 顺序处理 minute 日志文件
	go evh.HandleMinBinlogFile()

	// 顺序处理 second 日志文件
	go evh.HandleSecBinlogFile()
}

// refresh : refresh all Db
func (mc *MergeConfig) refresh(t inter.FileType, table string, flag uint16, ddl []byte, end uint32, r *binlog.TableEventHandler) error {
	switch t {
	case inter.DAY:
		// flush day, hour, minute , second db
		switch mc.Period {
		case inter.DAY:
			// start day back up => start: mc.LastDayTime, end curr
			if err := mc.refreshDayDb(table, flag, nil, end, r); err != nil {
				return err
			}
		case inter.HOUR:
			// start day back up => start: mc.LastDayTime, end curr
			if err := mc.refreshDayDb(table, flag, nil, end, r); err != nil {
				return err
			}

			// refresh hour
			if err := mc.refreshHourDb(table, flag, nil, end, r); err != nil {
				return err
			}
		case inter.MINUTE:
			// start day back up => start: mc.LastDayTime, end curr
			if err := mc.refreshDayDb(table, flag, nil, end, r); err != nil {
				return err
			}

			// refresh hour
			if err := mc.refreshHourDb(table, flag, nil, end, r); err != nil {
				return err
			}

			// start min backup => start: mc.LastMinTime, end curr
			if err := mc.refreshMinDb(table, flag, nil, end, r); err != nil {
				return err
			}
		case inter.SECOND:
			// start day back up => start: mc.LastDayTime, end curr
			if err := mc.refreshDayDb(table, flag, nil, end, r); err != nil {
				return err
			}

			// refresh hour
			if err := mc.refreshHourDb(table, flag, nil, end, r); err != nil {
				return err
			}

			// start min backup => start: mc.LastMinTime, end curr
			if err := mc.refreshMinDb(table, flag, nil, end, r); err != nil {
				return err
			}

			// bigger than second using second backup
			if err := mc.refreshSecDb(table, flag, nil, end, r); err != nil {
				return err
			}
		}
	case inter.HOUR:
		// flush hour, minute, second db
		switch mc.Period {
		case inter.HOUR:
			// refresh hour
			if err := mc.refreshHourDb(table, flag, nil, end, r); err != nil {
				return err
			}
		case inter.MINUTE:
			// refresh hour
			if err := mc.refreshHourDb(table, flag, nil, end, r); err != nil {
				return err
			}

			// start min backup => start: mc.LastMinTime, end curr
			if err := mc.refreshMinDb(table, flag, nil, end, r); err != nil {
				return err
			}
		case inter.SECOND:
			// refresh hour
			if err := mc.refreshHourDb(table, flag, nil, end, r); err != nil {
				return err
			}

			// start min backup => start: mc.LastMinTime, end curr
			if err := mc.refreshMinDb(table, flag, nil, end, r); err != nil {
				return err
			}

			// bigger than second using second backup
			if err := mc.refreshSecDb(table, flag, nil, end, r); err != nil {
				return err
			}
		}
	case inter.MINUTE:
		// flush minute, second db
		switch mc.Period {
		case inter.MINUTE:
			// start min backup => start: mc.LastMinTime, end curr
			if err := mc.refreshMinDb(table, flag, nil, end, r); err != nil {
				return err
			}
		case inter.SECOND:
			// start min backup => start: mc.LastMinTime, end curr
			if err := mc.refreshMinDb(table, flag, nil, end, r); err != nil {
				return err
			}

			// bigger than second using second backup
			if err := mc.refreshSecDb(table, flag, nil, end, r); err != nil {
				return err
			}
		}
	case inter.SECOND:
		// flush second db
		switch mc.Period {
		case inter.SECOND:
			// bigger than second using second backup
			if err := mc.refreshSecDb(table, flag, nil, end, r); err != nil {
				return err
			}
		}
	}
	return nil
}

// refreshDayDb refresh hour level db
func (mc *MergeConfig) refreshDayDb(table string, flag uint16, ddl []byte, end uint32, r *binlog.TableEventHandler) error {
	// mc.TempPath, mc.SnapshotPath, mc.DumpMySQLConfig.Host
	ap := &inter.AbsolutePath{
		TmpSrc:   mc.TempPath,
		TmpDst:   mc.SnapshotPath,
		FileType: inter.DAY,
		Host:     mc.DumpMySQLConfig.Host,
		Table:    table,
		Start:    int64(r.LastDayTime),
		End:      int64(end),
	}

	// have data
	have := isNotEmpty(r.DayDb)

	log.Debug("day src ", mc.TempPath, ", table=", table, " db is not empty ? ", have, ", for start = ", r.LastDayTime, ", end=", end)

	if err := r.DayDb.Close(); err != nil {
		return err
	}
	// overwrite db
	ddb, err := mc.exchange(flag, ddl, have, ap, r)
	if err != nil {
		return err
	}
	r.DayDb = ddb
	r.LastDayTime = end
	return nil
}

// refreshHourDb refresh hour level db
func (mc *MergeConfig) refreshHourDb(table string, flag uint16, ddl []byte, end uint32, r *binlog.TableEventHandler) error {
	ap := &inter.AbsolutePath{
		TmpSrc:   mc.TempPath,
		TmpDst:   mc.SnapshotPath,
		FileType: inter.HOUR,
		Host:     mc.DumpMySQLConfig.Host,
		Table:    table,
		Start:    int64(r.LastHourTime),
		End:      int64(end),
	}

	// have data
	have := isNotEmpty(r.HourDb)

	log.Debug("hour src ", mc.TempPath, ", table=", table, " db is not empty ? ", have, ", for start = ", r.LastHourTime, ", end=", end)

	if err := r.HourDb.Close(); err != nil {
		log.Error("close db error ", err)
	}
	// overwrite db
	hdb, err := mc.exchange(flag, ddl, have, ap, r)
	if err != nil {
		return err
	}
	r.HourDb = hdb
	r.LastHourTime = end

	return nil
}

// refreshMinDb refresh minute level db
func (mc *MergeConfig) refreshMinDb(table string, flag uint16, ddl []byte, end uint32, h *binlog.TableEventHandler) error {
	ap := &inter.AbsolutePath{
		TmpSrc:   mc.TempPath,
		TmpDst:   mc.SnapshotPath,
		FileType: inter.MINUTE,
		Host:     mc.DumpMySQLConfig.Host,
		Table:    table,
		Start:    int64(h.LastMinTime),
		End:      int64(end),
	}

	// have data
	have := isNotEmpty(h.MinDb)

	log.Debug("min src ", mc.TempPath, ", table=", table, " db is not empty ? ", have, ", for start = ", h.LastMinTime, ", end=", end)

	if err := h.MinDb.Close(); err != nil {
		log.Error("close minute db error ", err)
	}
	// overwrite db
	mdb, err := mc.exchange(flag, ddl, have, ap, h)
	if err != nil {
		return err
	}
	h.MinDb = mdb
	h.LastMinTime = end
	return nil
}

// refreshSecDb refresh second level db
func (mc *MergeConfig) refreshSecDb(table string, flag uint16, ddl []byte, end uint32, h *binlog.TableEventHandler) error {
	ap := &inter.AbsolutePath{
		TmpSrc:   mc.TempPath,
		TmpDst:   mc.SnapshotPath,
		FileType: inter.SECOND,
		Host:     mc.DumpMySQLConfig.Host,
		Table:    table,
		Start:    int64(h.LastSecTime),
		End:      int64(end),
	}

	// not empty
	have := isNotEmpty(h.SecDb)

	log.Debug("sec src ", mc.TempPath, ", table=", table, " sdb is not empty ? ", have, ", for start = ", h.LastSecTime, ", end=", end)

	if err := h.SecDb.Close(); err != nil {
		return err
	}
	// overwrite sdb
	sdb, err := mc.exchange(flag, ddl, have, ap, h)
	if err != nil {
		return err
	}
	h.SecDb = sdb
	h.LastSecTime = end

	return nil
}

// exchange : exchange new path
func (mc *MergeConfig) exchange(flag uint16, ddl []byte, have bool, ap *inter.AbsolutePath, h *binlog.TableEventHandler) (*leveldb.DB, error) {
	// src path
	src := ap.TmpSourcePath()

	// dst path
	dst := ap.TmpDstPath()
	log.Debug("source path ", src, ", dest path ", dst)

	// copy db
	nd, np, err := cdb.CopyDb(src, dst)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	log.Debug("move data from ", src, " to ", np)

	// 直接删除文件则 ok
	if ddl == nil && !have {
		// 删除拷贝的数据文件
		log.Info("删除文件夹 ", np)
		if err := os.RemoveAll(np); err != nil {
			log.Error(err)
		}
		// no error
		return nd, nil
	}

	// 存在数据需要写入 串行处理文件 不再并行处理 否则打包压缩文件麻烦
	// ddl is not null or have data
	// day db in new routine
	b := &binlog.Rest{
		NewPath: np,
		Client:  mc.Client,
		Abp:     ap,
		Have:    have,
		DDL:     ddl,
		Flags:   flag,
		Offset:  mc.LatestPosition.Copy(),
	}
	switch ap.FileType {
	case inter.DAY:
		h.DayRestChan <- b
	case inter.HOUR:
		h.HourRestChan <- b
	case inter.MINUTE:
		h.MinRestChan <- b
	case inter.SECOND:
		h.SecRestChan <- b
	}

	return nd, err
}

// LastPosition get the last position that dump get for error occur
func (mc *MergeConfig) LastPosition() *inter.BinlogPosition {
	// for event handler find the max sec offset

	// offset function
	of := func(h *binlog.TableEventHandler, period inter.FileType) *inter.BinlogPosition {
		switch period {
		case inter.DAY:
			return h.DayOffset
		case inter.HOUR:
			return h.HourOffset
		case inter.MINUTE:
			return h.MinOffset
		case inter.SECOND:
			return h.SecOffset
		}
		return nil
	}

	// max offset
	var mo *inter.BinlogPosition
	for t, h := range mc.TableHandlers {
		// right table right offset
		rof := of(h, mc.Period)

		log.Debug("table ", t, " offset ", rof)

		if mo == nil {
			mo = rof
			continue
		}

		if mo.Less(rof) {
			mo = rof
		}
	}

	// may be nil instead
	return mo
}

// closeHandler close handler
func (mc *MergeConfig) closeHandler() {
	for t, h := range mc.TableHandlers {
		log.Info("close table ", t, " all channels")
		h.Close()
	}
}

func (mc *MergeConfig) tick(ev *replication.BinlogEvent) error {
	curr := ev.Header.Timestamp
	// start, end => (start, end]
	if curr == 0 || mc.startTime == 0 {
		// 1st time just return
		return nil
	}

	for table, h := range mc.TableHandlers {
		mc.Wgs[table].Wait()
		switch {
		case curr-h.LastDayTime >= inter.DaySeconds:
			if err := mc.refresh(inter.DAY, table, ev.Header.Flags, nil, curr, h); err != nil {
				return err
			}

		case curr-h.LastHourTime >= inter.HourSeconds:
			//switch mc.Period {
			//case inter.HOUR, inter.MINUTE, inter.SECOND:
			//	// start hour back up => start: mc.LastHourTime, end curr
			//	mc.refresh(inter.HOUR, table, nil, curr, h)
			//}

			// start hour back up => start: mc.LastHourTime, end curr
			if err := mc.refresh(inter.HOUR, table, ev.Header.Flags, nil, curr, h); err != nil {
				return err
			}

		case curr-h.LastMinTime >= inter.MinSeconds:
			//
			//switch mc.Period {
			//case inter.MINUTE, inter.SECOND:
			//	// start min backup => start: mc.LastMinTime, end curr
			//	mc.refresh(inter.MINUTE, table, nil, curr, h)
			//}

			// start min backup => start: mc.LastMinTime, end curr
			if err := mc.refresh(inter.MINUTE, table, ev.Header.Flags, nil, curr, h); err != nil {
				return err
			}

		case curr-h.LastSecTime >= inter.Second:
			//switch mc.Period {
			//case inter.SECOND:
			//	// bigger than second using second backup
			//	mc.refresh(inter.SECOND, table, nil, curr, h)
			//}

			// bigger than second using second backup
			if err := mc.refresh(inter.SECOND, table, ev.Header.Flags, nil, curr, h); err != nil {
				return err
			}
		}
	}
	return nil
}

// isNotEmpty
func isNotEmpty(db *leveldb.DB) bool {
	iter := db.NewIterator(nil, nil)
	defer iter.Release()
	return iter.Next()
}
