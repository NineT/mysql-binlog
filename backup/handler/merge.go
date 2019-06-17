package handler

import (
	"bytes"
	"fmt"
	"strings"
	"sync"

	"github.com/mysql-binlog/siddontang/go-mysql/replication"
	"github.com/satori/go.uuid"
	"github.com/zssky/log"

	cdb "github.com/mysql-binlog/common/db"
	"github.com/mysql-binlog/common/final"
	"github.com/mysql-binlog/common/inter"
	blog "github.com/mysql-binlog/common/log"
	"github.com/mysql-binlog/common/regx"

	"github.com/mysql-binlog/backup/binlog"
	"github.com/mysql-binlog/backup/mode"
)

// MergeConfig merge conf
type MergeConfig struct {
	startTime       uint32                               // startTime  : first binlog time stamp
	lastEventTime   uint32                               // last event time:
	lastFlags       uint16                               // last flag
	table           string                               // 注意table是传值 不是引用 临时变量 存储事件对应的表名
	FinalTime       int64                                // FinalTime 终止时间 表示dump到此结束不在继续dump
	Compress        bool                                 // compress
	SnapshotPath    string                               // 数据 kv 存储路径 快照存储路径
	After           *final.After                         // After math for merge
	StartPos        *cdb.BinlogOffset                    // start binlog offset
	formatDesc      *replication.BinlogEvent             // format description event
	latestBegin     *blog.DataEvent                      // begin for latest
	latestGtid      *blog.DataEvent                      // gtid for latest
	latestTableMap  *blog.DataEvent                      // table map event
	TableHandlers   map[string]*binlog.TableEventHandler // TableHandlers 每张表的操作
	DumpMySQLConfig *cdb.MetaConf                        // DumpMySQLConfig dump mysql 的操作
	Wgs             map[string]*sync.WaitGroup           // Wgs wait group 用来做携程之间的等待操作
}

// Start start merge
func (mc *MergeConfig) Start() {
	defer mc.Close()

	dump := &mode.RemoteMode{
		LatestPos: mc.StartPos,
		Config:    mc.DumpMySQLConfig,
	}

	// start dump
	dump.Handle(mc.EventHandler, mc.After)
}

// Close close when merge finished
func (mc *MergeConfig) Close() {
	mc.closeHandler()
}

// EventHandler handle event false: arrived the terminal time, true: means continue
func (mc *MergeConfig) EventHandler(ev *replication.BinlogEvent) bool {
	defer func() {
		if err := recover(); err != nil {
			// recover error no next time arrive here
			mc.After.Errs <- err
			return
		}

		// save Last DataEvent Time
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

		switch strings.ToUpper(string(qe.Query)) {
		case "BEGIN":
			mc.latestBegin = blog.Binlog2Data(ev, mc.formatDesc.Event.(*replication.FormatDescriptionEvent).ChecksumAlgorithm)
		case "COMMIT":
		case "ROLLBACK":
		case "SAVEPOINT":
		default:
			// here ddl may have two statement
			log.Debug("schema ", string(qe.Schema), " ddl ", string(qe.Query))

			for _, ddl := range bytes.Split(qe.Query, []byte(";")) {
				if tbs, matched := regx.Parse(ddl, qe.Schema); matched { // 匹配表名成功
					for _, tb := range tbs {
						// write ddl to table handler
						mc.writeTableQueryEvent(tb, newQueryEvent(ev.Header, qe, ddl))
					}
				} else {
					// not match tables ddl sql then just write to common packet
					log.Debug("not matched")
					// write ddl to public table space
					mc.writeTableQueryEvent([]byte(inter.Public), newQueryEvent(ev.Header, qe, ddl))
				}
			}
		}
	case replication.STOP_EVENT:
	case replication.ROTATE_EVENT:
		re, _ := ev.Event.(*replication.RotateEvent)
		log.Debug("next binlog file name " + string(re.NextLogName))

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
		mc.formatDesc = ev
	case replication.XID_EVENT:
	case replication.BEGIN_LOAD_QUERY_EVENT:
	case replication.EXECUTE_LOAD_QUERY_EVENT:
	case replication.TABLE_MAP_EVENT:
		tme, _ := ev.Event.(*replication.TableMapEvent)
		mc.table = fmt.Sprintf("%s.%s", inter.CharStd(string(tme.Schema)),
			inter.CharStd(string(tme.Table)))

		if _, ok := mc.TableHandlers[mc.table]; !ok {
			mc.newHandler(curr, mc.table)
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
		h := mc.TableHandlers[mc.table]
		// add 1
		h.Wg.Add(1)

		h.EventChan <- &binlog.Transaction{
			TableMap: mc.latestTableMap,
			Event:    blog.Binlog2Data(ev, mc.formatDesc.Event.(*replication.FormatDescriptionEvent).ChecksumAlgorithm),
			Gtid:     mc.latestGtid,
			Begin:    mc.latestBegin,
		}

	case replication.INCIDENT_EVENT:
	case replication.HEARTBEAT_EVENT:
	case replication.IGNORABLE_EVENT:
	case replication.ROWS_QUERY_EVENT: // query ddl
	case replication.GTID_EVENT:
		ge := ev.Event.(*replication.GTIDEvent)
		u, err := uuid.FromBytes(ge.SID)
		if err != nil {
			panic(err)
		}

		if err := mc.StartPos.GTIDSet.Update(fmt.Sprintf("%s:%d", u.String(), ge.GNO)); err != nil {
			panic(err)
		}
		// save the latest gtid event
		mc.latestGtid = blog.Binlog2Data(ev, mc.formatDesc.Event.(*replication.FormatDescriptionEvent).ChecksumAlgorithm)

	case replication.ANONYMOUS_GTID_EVENT:
	case replication.PREVIOUS_GTIDS_EVENT:
	default:
	}

	return true
}

// newQueryEvent new query evetnt
func newQueryEvent(header *replication.EventHeader, qe *replication.QueryEvent, ddl []byte) *replication.BinlogEvent {
	var event *replication.QueryEvent
	qe.Query = ddl
	if err := event.Decode(qe.Encode()); err != nil || event == nil {
		panic(err)
	}

	return &replication.BinlogEvent{
		Header: header.Copy(),
		Event:  qe,
	}
}

// newHandler generate table handler
func (mc *MergeConfig) newHandler(curr uint32, table string) {
	log.Info("table binlog file path with current ", fmt.Sprintf("%s%s%d.log", mc.SnapshotPath, table, curr))

	evh, err := binlog.NewEventHandler(mc.SnapshotPath, table, curr, mc.Compress, blog.Binlog2Data(mc.formatDesc, mc.formatDesc.Event.(*replication.FormatDescriptionEvent).ChecksumAlgorithm), mc.After)
	if err != nil {
		panic(err)
	}
	mc.TableHandlers[table] = evh
	mc.Wgs[table] = evh.Wg

	// 事件队列
	go evh.HandleLogEvent()
}

// closeHandler close handler
func (mc *MergeConfig) closeHandler() {
	for t, h := range mc.TableHandlers {
		log.Info("close table ", t, " all channels")
		h.Close()
	}
}

// writeTableQueryEvent write query event into binlog file
func (mc *MergeConfig) writeTableQueryEvent(table []byte, ev *replication.BinlogEvent) {
	if _, ok := mc.TableHandlers[string(table)]; !ok {
		// table not exist so create new
		mc.newHandler(ev.Header.Timestamp, string(table))
	}

	h := mc.TableHandlers[string(table)]

	h.EventChan <- &binlog.Transaction{
		TableMap: mc.latestTableMap,
		Event:    blog.Binlog2Data(ev, mc.formatDesc.Event.(*replication.FormatDescriptionEvent).ChecksumAlgorithm),
		Gtid:     mc.latestGtid,
		Begin:    mc.latestBegin,
	}
}
