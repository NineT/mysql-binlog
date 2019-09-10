package handler

import (
	"container/list"
	"fmt"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"time"

	"github.com/mysql-binlog/siddontang/go-mysql/mysql"
	"github.com/mysql-binlog/siddontang/go-mysql/replication"
	"github.com/satori/go.uuid"
	"github.com/zssky/log"

	"github.com/mysql-binlog/common/final"
	"github.com/mysql-binlog/common/inter"
	blog "github.com/mysql-binlog/common/log"
	"github.com/mysql-binlog/common/meta"
	"github.com/mysql-binlog/common/regx"

	"github.com/mysql-binlog/backup/binlog"
)

// MergeConfig merge conf
type MergeConfig struct {
	After         *final.After                         // After math for merge
	mode          string                               // binlog mode separated, integrated for binlog event on each table
	closed        int32                                // closed flag
	err           error                                // error for status
	path          string                               // 数据 kv 存储路径 快照存储路径
	ins           *meta.Instance                       // MySQL instance for host, port, user, password
	syncer        *replication.BinlogSyncer            // binlog syncer
	cid           int64                                // cluster id
	binFile       string                               // binlog file
	lastEventTime uint32                               // last event time:
	lastFlags     uint16                               // last flag
	formatDesc    *replication.BinlogEvent             // format description event
	checksumAlg   byte                                 // checksumAlg
	latestBegin   *blog.DataEvent                      // begin for latest
	gtid          mysql.GTIDSet                        // current integrate gtid
	latestGtid    *blog.DataEvent                      // gtid for latest
	relatedTables map[string]bool                      // relatedTables gtid related tables
	tableHandlers map[string]*binlog.TableEventHandler // tableHandlers 每张表的操作
	gc            chan []byte                          // gtid channel using for gtid channels
	offsets       *list.List                           // offsets for gtid list
}

// NewMergeConfig new merge config
func NewMergeConfig(path string, off *meta.Offset, i *meta.Instance, mode string) (*MergeConfig, error) {
	m := &MergeConfig{
		mode:          mode,
		closed:        0,
		path:          inter.StdPath(path),
		ins:           i,
		relatedTables: make(map[string]bool),
		tableHandlers: make(map[string]*binlog.TableEventHandler),
		gc:            make(chan []byte, inter.BufferSize),
	}

	g, err := mysql.ParseMysqlGTIDSet(string(off.ExedGtid))
	if err != nil {
		log.Errorf("parse gtid{%s} error{%v}", string(off.ExedGtid), err)
		return nil, err
	}

	// gtid for all now
	m.gtid = g

	// header flag take this then take the newly offset
	m.offsets = list.New()

	// copy make sure that not modified by another
	bt := make([]byte, len(off.ExedGtid))
	copy(bt, off.ExedGtid)
	off.TrxGtid = string(bt)
	off.Counter = 0
	off.Header = true
	m.offsets.PushBack(off)

	// set cluster id
	m.cid = off.CID

	return m, nil
}

// Start start merge
func (mc *MergeConfig) Start() {
	defer mc.Close()

	cfg := replication.BinlogSyncerConfig{
		ServerID: 1011,
		Flavor:   "mysql",
		Host:     mc.ins.Host,
		Port:     uint16(mc.ins.Port),
		User:     mc.ins.User,
		Password: mc.ins.Password,
	}

	mc.syncer = replication.NewBinlogSyncer(cfg)
	defer mc.syncer.Close()

	var streamer *replication.BinlogStreamer
	var err error

	gs, err := mysql.ParseMysqlGTIDSet(string(mc.offsets.Front().Value.(*meta.Offset).TrxGtid))
	if err != nil {
		log.Errorf("parse gtid{%s} error{%v}", string(mc.offsets.Front().Value.(*meta.Offset).TrxGtid), err)
		mc.err = err
		return
	}

	if streamer, err = mc.syncer.StartSyncGTID(gs); err != nil {
		log.Error("error sync data using gtid ", err)
		mc.err = err
		return
	}

	defer mc.After.After()

	// true means continue, false means to stop
	for {
		select {
		case e := <-mc.After.Errs:
			// wait for errors
			mc.err = e.(error)
			panic(e)
		case g, hasMore := <-mc.gc:
			if !hasMore {
				// channel is closed
				log.Warn("gtid channel is closed")
				return
			}
			// take the priority to clear offset
			var tmp *list.Element
			for e := mc.offsets.Front(); e != nil; e = e.Next() {
				o := e.Value.(*meta.Offset)
				if strings.EqualFold(o.TrxGtid, string(g)) {
					o.Counter --
					if o.Counter <= 0 {
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
				log.Debugf("remove gtid %s, executed gtid %s", o.TrxGtid, o.ExedGtid)

				pg, err := mysql.ParseMysqlGTIDSet(string(pre.Value.(*meta.Offset).ExedGtid))
				if err != nil {
					log.Error("parse previous gtid error ", err)
					mc.After.Errs <- err
					break
				}

				if err := pg.Update(string(o.ExedGtid)); err != nil {
					log.Error("update gtid error previous gtid:", string(pre.Value.(*meta.Offset).TrxGtid), ", next gtid:", string(o.TrxGtid), ", error ", err)
					mc.After.Errs <- err
					break
				}

				// reset gtid
				pre.Value.(*meta.Offset).ExedGtid = pg.String()
			}
		default:
			// check write is block make sure that write cannot hang
			// consider the worst situation for only one channel exists then size(offset) must < inter.BufferSize
			if mc.offsets.Len() >= (inter.BufferSize / 4) {
				log.Warnf("buffer is full for offset size %d >= %d wait for 1.sec for buffer clearing because storage write slowly", mc.offsets.Len(), inter.BufferSize)
				time.Sleep(time.Second)
				break
			}

			ev, err := streamer.GetEvent(mc.After.Ctx)
			if err != nil {
				log.Error("error handle binlog event ", err)
				mc.After.Errs <- err
				break
			}
			mc.EventHandler(ev)
		}
	}
}

// Close close when merge finished
func (mc *MergeConfig) Close() {
	if atomic.CompareAndSwapInt32(&mc.closed, 0, 1) {
		// close syncer again
		mc.syncer.Close()

		mc.closeHandler()

		return
	}

	log.Infof("already closed yet")
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
			mc.latestBegin = blog.Binlog2Data(ev, mc.checksumAlg, mc.latestGtid.TrxGtid, []byte(mc.gtid.String()), mc.binFile, false, false)
		case "COMMIT":
			mc.handleCommit(ev)
		case "ROLLBACK":
		case "SAVEPOINT":
		default:
			// here ddl may have two statement
			log.Debug("schema ", string(qe.Schema), " ddl ", string(qe.Query), ", event size ", len(ev.RawData))

			// offset counter
			var c int
			switch mc.mode {
			case inter.Integrated:
				var table = ""
				if _, ok := mc.tableHandlers[table]; !ok {
					mc.newHandler(curr, table, mc.gc)
				}

				// just write empty table name
				mc.relatedTables[""] = true

				mc.tableHandlers[""].EventChan <- mc.latestGtid.Copy()

				mc.tableHandlers[""].EventChan <- blog.Binlog2Data(ev, mc.checksumAlg, mc.latestGtid.TrxGtid, []byte(mc.gtid.String()), mc.binFile, true, false)
			case inter.Separated:
				// single log for each table
				if tbs, matched := regx.Parse(qe.Query, qe.Schema); matched { // 匹配表名成功
					for _, tb := range tbs {
						// write table to table
						mc.relatedTables[string(tb)] = true

						// write ddl to table handler
						mc.writeQueryEvent(tb, ev)
					}

					log.Debug("push offset to position list")
					c = len(tbs)
				} else {
					// common packet with ddl write to each table
					log.Debug("not matched")
					for _, h := range mc.tableHandlers {
						// gtid
						h.EventChan <- mc.latestGtid.Copy()

						// ddl event
						h.EventChan <- blog.Binlog2Data(ev, mc.checksumAlg, mc.latestGtid.TrxGtid, []byte(mc.gtid.String()), mc.binFile, true, false)
					}
					c = len(mc.tableHandlers)
				}
			}

			// append offset
			mc.offsets.PushBack(&meta.Offset{
				CID:      mc.cid,
				TrxGtid:  string(mc.latestGtid.TrxGtid),
				ExedGtid: string(mc.latestGtid.TrxGtid), // newly then make executed gtid = transaction gtid
				Counter:  c,
				Header:   false,
				Time:     ev.Header.Timestamp,
				BinFile:  mc.binFile,
				BinPos:   uint64(ev.Header.LogPos),
			})
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
		mc.checksumAlg = mc.formatDesc.Event.(*replication.FormatDescriptionEvent).ChecksumAlgorithm
	case replication.XID_EVENT:
		mc.handleCommit(ev)

	case replication.BEGIN_LOAD_QUERY_EVENT:
	case replication.EXECUTE_LOAD_QUERY_EVENT:
	case replication.TABLE_MAP_EVENT:
		tme, _ := ev.Event.(*replication.TableMapEvent)

		var table string
		switch mc.mode {
		case inter.Integrated:
			table = "" // always to empty
		case inter.Separated:
			table = fmt.Sprintf("%s.%s", inter.CharStd(string(tme.Schema)),
				inter.CharStd(string(tme.Table)))
		}

		// remember related tables
		if _, ok := mc.relatedTables[table]; !ok {
			mc.relatedTables[table] = true
		}

		if _, ok := mc.tableHandlers[table]; !ok {
			mc.newHandler(curr, table, mc.gc)
		}

		h := mc.tableHandlers[table]

		// one table => one binlog file => one gtid event & begin event in 1 trx
		if mc.relatedTables[table] {
			// gtid
			h.EventChan <- mc.latestGtid.Copy()

			// begin
			h.EventChan <- mc.latestBegin.Copy()

			mc.relatedTables[table] = false
		}


		log.Debugf("write table map event %s, table id %d ", tme.Table, tme.TableID)
		// table map event
		h.EventChan <- blog.Binlog2Data(ev, mc.checksumAlg, mc.latestGtid.TrxGtid, []byte(mc.gtid.String()), mc.binFile, false, false)

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
		re := ev.Event.(*replication.RowsEvent)
		var table string
		switch mc.mode {
		case inter.Integrated:
			table = "" // always to empty
		case inter.Separated:
			table = fmt.Sprintf("%s.%s", inter.CharStd(string(re.Table.Schema)),
				inter.CharStd(string(re.Table.Table)))
		}

		log.Debugf("rows table map event %s, table id %d ", re.Table.Table, re.Table.TableID)
		mc.tableHandlers[table].EventChan <- blog.Binlog2Data(ev, mc.checksumAlg, mc.latestGtid.TrxGtid, []byte(mc.gtid.String()), mc.binFile, false, false)

	case replication.INCIDENT_EVENT:
	case replication.HEARTBEAT_EVENT:
	case replication.IGNORABLE_EVENT:
	case replication.ROWS_QUERY_EVENT: // query ddl
	case replication.GTID_EVENT:
		// reset the previous map
		mc.relatedTables = make(map[string]bool)

		ge := ev.Event.(*replication.GTIDEvent)
		u, err := uuid.FromBytes(ge.SID)
		if err != nil {
			debug.PrintStack()
			panic(err)
		}

		// single gtid
		sg := fmt.Sprintf("%s:%d", u.String(), ge.GNO)
		if err := mc.gtid.Update(sg); err != nil {
			debug.PrintStack()
			log.Errorf("update gtid{%s} error %v", sg, err)
			panic(err)
		}

		// save the latest gtid event
		mc.latestGtid = blog.Binlog2Data(ev, mc.checksumAlg, []byte(sg), []byte(mc.gtid.String()), mc.binFile, false, false)

	case replication.ANONYMOUS_GTID_EVENT:
	case replication.PREVIOUS_GTIDS_EVENT:
	default:
	}
}

// newHandler generate table handler
func (mc *MergeConfig) newHandler(curr uint32, table string, gch chan []byte) {
	log.Info("table binlog file path with current ", fmt.Sprintf("%s/%s/%d.log", mc.path, table, curr))

	evh, err := binlog.NewEventHandler(mc.mode, mc.path, table, curr, mc.cid, blog.Binlog2Data(mc.formatDesc, mc.checksumAlg, mc.latestGtid.TrxGtid, []byte(mc.gtid.String()), mc.binFile, false, false), mc.After, gch)
	if err != nil {
		debug.PrintStack()
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

	// close gc channel
	close(mc.gc)
}

// writeQueryEvent write query event into binlog file
func (mc *MergeConfig) writeQueryEvent(table []byte, ev *replication.BinlogEvent) {
	if _, ok := mc.tableHandlers[string(table)]; !ok {
		// table not exist so create new
		mc.newHandler(ev.Header.Timestamp, string(table), mc.gc)
	}

	h := mc.tableHandlers[string(table)]

	log.Debugf("write latest gtid")
	// gtid
	h.EventChan <- mc.latestGtid

	log.Debugf("write ddl")
	// ddl event
	h.EventChan <- blog.Binlog2Data(ev, mc.checksumAlg, mc.latestGtid.TrxGtid, []byte(mc.gtid.String()), mc.binFile, true, false)

	log.Debugf("finish writeQueryEvent")
}

// NewlyOffset for binlog dump position
func (mc *MergeConfig) NewlyOffset() *meta.Offset {
	idx := 0
	for e := mc.offsets.Front(); e != nil; e = e.Next() {
		idx ++
		o := e.Value.(*meta.Offset)
		log.Infof("current index {%d} , offset {%v}", idx, o)
	}
	return mc.offsets.Front().Value.(*meta.Offset)
}

// Status for dump status: nil is normal and occur error means something wrong
func (mc *MergeConfig) Status() error {
	return mc.err
}

// handleCommit
func (mc *MergeConfig) handleCommit(ev *replication.BinlogEvent) {
	for t := range mc.relatedTables {
		// write xid commit event to each table
		mc.tableHandlers[t].EventChan <- blog.Binlog2Data(ev, mc.checksumAlg, mc.latestGtid.TrxGtid, []byte(mc.gtid.String()), mc.binFile, false, true)
	}

	mc.offsets.PushBack(&meta.Offset{
		CID:      mc.cid,
		TrxGtid:  string(mc.latestGtid.TrxGtid),
		ExedGtid: string(mc.latestGtid.TrxGtid), // newly then make executed gtid = transaction gtid
		Counter:  len(mc.relatedTables),
		Header:   false,
		Time:     ev.Header.Timestamp,
		BinFile:  mc.binFile,
		BinPos:   uint64(ev.Header.LogPos),
	})
}
