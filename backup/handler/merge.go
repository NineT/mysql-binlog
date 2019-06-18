package handler

import (
	"bytes"
	"container/list"
	"fmt"
	"github.com/mysql-binlog/common/meta"
	"github.com/mysql-binlog/siddontang/go-mysql/mysql"
	"github.com/mysql-binlog/siddontang/go-mysql/replication"
	"github.com/satori/go.uuid"
	"github.com/zssky/log"
	"strings"
	"time"

	cdb "github.com/mysql-binlog/common/db"
	"github.com/mysql-binlog/common/final"
	"github.com/mysql-binlog/common/inter"
	blog "github.com/mysql-binlog/common/log"
	"github.com/mysql-binlog/common/regx"

	"github.com/mysql-binlog/backup/binlog"
)

// MergeConfig merge conf
type MergeConfig struct {
	Compress        bool                                 // compress
	SnapshotPath    string                               // 数据 kv 存储路径 快照存储路径
	After           *final.After                         // After math for merge
	DumpMySQLConfig *cdb.MetaConf                        // DumpMySQLConfig dump mysql 的操作
	binFile         string                               // binlog file
	lastEventTime   uint32                               // last event time:
	lastFlags       uint16                               // last flag
	table           string                               // 注意table是传值 不是引用 临时变量 存储事件对应的表名
	formatDesc      *replication.BinlogEvent             // format description event
	latestBegin     *blog.DataEvent                      // begin for latest
	latestUUID      []byte                               // latest uuid
	latestGtid      *blog.DataEvent                      // gtid for latest
	relatedTables   map[string]string                    // relatedTables gtid related tables
	tableHandlers   map[string]*binlog.TableEventHandler // tableHandlers 每张表的操作
	gc              chan []byte                          // gtid channel using for gtid channels
	offsets         *list.List                           // offsets for gtid list
}

// NewMergeConfig new merge config
func NewMergeConfig(compress bool, path string, off *meta.Offset, dump *cdb.MetaConf) *MergeConfig {
	m := &MergeConfig{
		Compress:        compress,
		SnapshotPath:    inter.StdPath(path),
		DumpMySQLConfig: dump,
		relatedTables:   make(map[string]string),
		tableHandlers:   make(map[string]*binlog.TableEventHandler),
		gc:              make(chan []byte, inter.BufferSize),
	}

	// header flag take this then take the newly offset
	m.offsets = list.New()
	m.offsets.PushBack(&meta.Offset{
		OriGtid: off.OriGtid,
		Counter: 0,
		Header:  true,
	})

	return m
}

// Start start merge
func (mc *MergeConfig) Start() {
	defer mc.Close()

	cfg := replication.BinlogSyncerConfig{
		ServerID: 1011,
		Flavor:   "mysql",
		Host:     mc.DumpMySQLConfig.Host,
		Port:     uint16(mc.DumpMySQLConfig.Port),
		User:     mc.DumpMySQLConfig.User,
		Password: mc.DumpMySQLConfig.Password,
	}

	syncer := replication.NewBinlogSyncer(cfg)
	defer syncer.Close()

	var streamer *replication.BinlogStreamer
	var err error

	gs, err := mysql.ParseMysqlGTIDSet(string(mc.offsets.Front().Value.(*meta.Offset).OriGtid))
	if err != nil {
		log.Fatal(err)
	}

	if streamer, err = syncer.StartSyncGTID(gs); err != nil {
		log.Error("error sync data using gtid ", err)
		panic(err)
	}

	// true means continue, false means to stop
	for {
		select {
		case e := <-mc.After.Errs:
			// wait for errors
			panic(e)
		case g := <-mc.gc:
			// take the priority to clear offset
			var tmp *list.Element
			for e := mc.offsets.Front(); e != nil; e = e.Next() {
				o := e.Value.(*meta.Offset)
				if bytes.EqualFold(o.OriGtid, g) {
					o.Counter --
					if o.Counter == 0 {
						// gtid event flush to binlog file
						tmp = e
					}
					break
				}
			}

			if tmp != nil {
				// remove element from list and header will never be null
				pre := tmp.Prev()
				o := mc.offsets.Remove(tmp).(*meta.Offset)
				log.Debugf("remove gtid %v", o)

				pg, err := mysql.ParseMysqlGTIDSet(string(pre.Value.(*meta.Offset).OriGtid))
				if err != nil {
					log.Error("parse previous gtid error ", err)
					panic(err)
				}

				if err := pg.Update(string(o.OriGtid)); err != nil {
					log.Error("update gtid error previous gtid:", string(pre.Value.(*meta.Offset).OriGtid), ", next gtid:", string(o.OriGtid), ", error ", err)
					panic(err)
				}

				// reset gtid
				pre.Value.(*meta.Offset).MergedGtid = []byte(pg.String())
			}
		default:
			// check write is block make sure that write cannot hang
			// consider the worst situation for only one channel exists then size(offset) must < inter.BufferSize
			if mc.offsets.Len() >= inter.BufferSize {
				log.Warnf("buffer is full for offset size %d >= %d wait for 1.sec for buffer clearing", mc.offsets.Len(), inter.BufferSize)
				time.Sleep(time.Second * 1)
				break
			}

			ev, err := streamer.GetEvent(mc.After.Ctx)
			if err != nil {
				log.Error("error handle binlog event ", err)
				panic(err)
			}
			mc.EventHandler(ev)
		}
	}
}

// Close close when merge finished
func (mc *MergeConfig) Close() {
	mc.closeHandler()
}

// EventHandler handle event false: arrived the terminal time, true: means continue
func (mc *MergeConfig) EventHandler(ev *replication.BinlogEvent) {
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

	//ev.RowHeader.Timestamp
	switch ev.Header.EventType {
	case replication.UNKNOWN_EVENT:
	case replication.START_EVENT_V3:
	case replication.QUERY_EVENT:
		qe, _ := ev.Event.(*replication.QueryEvent)

		switch strings.ToUpper(string(qe.Query)) {
		case "BEGIN":
			mc.latestBegin = blog.Binlog2Data(ev, mc.formatDesc.Event.(*replication.FormatDescriptionEvent).ChecksumAlgorithm, mc.latestUUID, false)
		case "COMMIT":
		case "ROLLBACK":
		case "SAVEPOINT":
		default:
			// here ddl may have two statement
			log.Debug("schema ", string(qe.Schema), " ddl ", string(qe.Query))

			for _, ddl := range bytes.Split(qe.Query, []byte(";")) {
				if tbs, matched := regx.Parse(ddl, qe.Schema); matched { // 匹配表名成功
					for _, tb := range tbs {
						// write table to table
						mc.relatedTables[string(tb)] = string(tb)

						// write ddl to table handler
						mc.writeQueryEvent(tb, newQueryEvent(ev.Header, qe, ddl))
					}

					// append offset
					mc.offsets.PushBack(&meta.Offset{
						OriGtid: mc.latestUUID,
						Counter: len(tbs),
						Header:  false,
						Time:    ev.Header.Timestamp,
						BinFile: mc.binFile,
						BinPos:  ev.Header.LogPos,
					})
				} else {
					// common packet with ddl write to each table
					log.Debug("not matched")
					for _, h := range mc.tableHandlers {
						// gtid
						h.EventChan <- mc.latestGtid

						// ddl event
						h.EventChan <- blog.Binlog2Data(ev, mc.formatDesc.Event.(*replication.FormatDescriptionEvent).ChecksumAlgorithm, mc.latestUUID, true)
					}

					// append offset
					mc.offsets.PushBack(&meta.Offset{
						OriGtid: mc.latestUUID,
						Counter: len(mc.tableHandlers),
						Header:  false,
						Time:    ev.Header.Timestamp,
						BinFile: mc.binFile,
						BinPos:  ev.Header.LogPos,
					})
				}
			}
		}
	case replication.STOP_EVENT:
	case replication.ROTATE_EVENT:
		re, _ := ev.Event.(*replication.RotateEvent)

		// save binlog file
		mc.binFile = string(re.NextLogName)
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
		for t := range mc.relatedTables {
			// write xid commit event to each table
			mc.tableHandlers[t].EventChan <- blog.Binlog2Data(ev, mc.formatDesc.Event.(*replication.FormatDescriptionEvent).ChecksumAlgorithm, mc.latestUUID, false)
		}

		mc.offsets.PushBack(&meta.Offset{
			OriGtid: mc.latestUUID,
			Counter: len(mc.relatedTables),
			Header:  false,
			Time:    ev.Header.Timestamp,
			BinFile: mc.binFile,
			BinPos:  ev.Header.LogPos,
		})
	case replication.BEGIN_LOAD_QUERY_EVENT:
	case replication.EXECUTE_LOAD_QUERY_EVENT:
	case replication.TABLE_MAP_EVENT:
		tme, _ := ev.Event.(*replication.TableMapEvent)
		mc.table = fmt.Sprintf("%s.%s", inter.CharStd(string(tme.Schema)),
			inter.CharStd(string(tme.Table)))

		// remember related tables
		mc.relatedTables[mc.table] = mc.table

		if _, ok := mc.tableHandlers[mc.table]; !ok {
			mc.newHandler(curr, mc.table, mc.gc)
		}

		h := mc.tableHandlers[mc.table]
		// gtid
		h.EventChan <- mc.latestGtid

		// begin
		h.EventChan <- mc.latestBegin

		// table map event
		h.EventChan <- blog.Binlog2Data(ev, mc.formatDesc.Event.(*replication.FormatDescriptionEvent).ChecksumAlgorithm, mc.latestUUID, false)

	case replication.WRITE_ROWS_EVENTv0,
		replication.WRITE_ROWS_EVENTv1,
		replication.WRITE_ROWS_EVENTv2,
		replication.DELETE_ROWS_EVENTv0,
		replication.DELETE_ROWS_EVENTv1,
		replication.DELETE_ROWS_EVENTv2,
		replication.UPDATE_ROWS_EVENTv0,
		replication.UPDATE_ROWS_EVENTv1,
		replication.UPDATE_ROWS_EVENTv2:
		// write event into even channel
		mc.tableHandlers[mc.table].EventChan <- blog.Binlog2Data(ev, mc.formatDesc.Event.(*replication.FormatDescriptionEvent).ChecksumAlgorithm, mc.latestUUID, false)

	case replication.INCIDENT_EVENT:
	case replication.HEARTBEAT_EVENT:
	case replication.IGNORABLE_EVENT:
	case replication.ROWS_QUERY_EVENT: // query ddl
	case replication.GTID_EVENT:
		// reset the previous map
		mc.relatedTables = make(map[string]string)

		ge := ev.Event.(*replication.GTIDEvent)
		u, err := uuid.FromBytes(ge.SID)
		if err != nil {
			panic(err)
		}

		// save the latest gtid event
		mc.latestGtid = blog.Binlog2Data(ev, mc.formatDesc.Event.(*replication.FormatDescriptionEvent).ChecksumAlgorithm, mc.latestUUID, false)

		// latest uuid
		mc.latestUUID = []byte(fmt.Sprintf("%s:%d", u.String(), ge.GNO))

	case replication.ANONYMOUS_GTID_EVENT:
	case replication.PREVIOUS_GTIDS_EVENT:
	default:
	}
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
func (mc *MergeConfig) newHandler(curr uint32, table string, gch chan []byte) {
	log.Info("table binlog file path with current ", fmt.Sprintf("%s%s%d.log", mc.SnapshotPath, table, curr))

	evh, err := binlog.NewEventHandler(mc.SnapshotPath, table, curr, mc.Compress, blog.Binlog2Data(mc.formatDesc, mc.formatDesc.Event.(*replication.FormatDescriptionEvent).ChecksumAlgorithm, mc.latestUUID, false), mc.After, gch)
	if err != nil {
		panic(err)
	}
	mc.tableHandlers[table] = evh

	// 事件队列
	go evh.HandleLogEvent()
}

// closeHandler close handler
func (mc *MergeConfig) closeHandler() {
	for t, h := range mc.tableHandlers {
		log.Info("close table ", t, " all channels")
		h.Close()
	}
}

// writeQueryEvent write query event into binlog file
func (mc *MergeConfig) writeQueryEvent(table []byte, ev *replication.BinlogEvent) {
	if _, ok := mc.tableHandlers[string(table)]; !ok {
		// table not exist so create new
		mc.newHandler(ev.Header.Timestamp, string(table), mc.gc)
	}

	h := mc.tableHandlers[string(table)]

	// gtid
	h.EventChan <- mc.latestGtid

	// ddl event
	h.EventChan <- blog.Binlog2Data(ev, mc.formatDesc.Event.(*replication.FormatDescriptionEvent).ChecksumAlgorithm, mc.latestUUID, true)
}
