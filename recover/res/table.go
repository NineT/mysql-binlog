package res

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/zssky/log"

	"github.com/mysql-binlog/siddontang/go-mysql/mysql"
	"github.com/mysql-binlog/siddontang/go-mysql/replication"

	"github.com/mysql-binlog/common/inter"
	clog "github.com/mysql-binlog/common/log"
	"github.com/mysql-binlog/common/meta"
	"github.com/mysql-binlog/common/regx"
	"github.com/mysql-binlog/common/utils"

	"github.com/mysql-binlog/recover/bpct"
	"github.com/mysql-binlog/recover/conf"
)

/***
* recover on table each table have one routine
*
*/
const (
	// 	StmtEndFlag        = 1
	StmtEndFlag = 1

	logSuffix = ".log"

	done = "done"

	canceled = "canceled"
)

type int64s []int64

func (s int64s) Len() int           { return len(s) }
func (s int64s) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s int64s) Less(i, j int) bool { return s[i] < s[j] }

// TableRecover
type TableRecover struct {
	table string           // table name
	path  string           // table binlog path
	time  int64            // final time for binlog recover
	ctx   context.Context  // context
	off   *meta.Offset     // start offset
	pos   *LogPosition     // log position
	cg    mysql.GTIDSet    // current gtid set
	og    mysql.GTIDSet    // origin gtid offset
	i     *bpct.Instance   // one table one connection
	user  string           // mysql user
	pass  string           // mysql pass
	port  int              // mysql port
	wg    *sync.WaitGroup  // wait group for outside
	errs  chan error       // error channel
	coCh  chan interface{} // coordinate channel

	// followings are for event type
	parser *replication.BinlogParser // binlog parser
	desc   []byte                    // desc format event
	buffer bytes.Buffer              // byte buffer
}

// NewTable for outside use
func NewTable(table, clusterPath string, time int64, ctx context.Context, o *meta.Offset, user, pass string, port int, wg *sync.WaitGroup, errs chan error, coCh chan interface{}) (*TableRecover, error) {
	if strings.HasSuffix(clusterPath, "/") {
		clusterPath = strings.TrimSuffix(clusterPath, "/")
	}

	og, err := mysql.ParseMysqlGTIDSet(o.ExedGtid)
	if err != nil {
		log.Errorf("parse executed gtid{%s} error{%v}", o.ExedGtid, err)
		return nil, err
	}

	l, err := NewLogPosition(o.ExedGtid, o.Time)
	if err != nil {
		log.Errorf("new log position gtid{%s} error{%v}", o.ExedGtid, err)
		return nil, err
	}

	return &TableRecover{
		table:  table,
		path:   fmt.Sprintf("%s/%s", clusterPath, table),
		time:   time,
		ctx:    ctx,
		off:    o,
		pos:    l,
		og:     og,
		user:   user,
		pass:   pass,
		port:   port,
		wg:     wg,
		errs:   errs,
		coCh:   coCh,
		parser: replication.NewBinlogParser(),
	}, nil
}

// ID for routine
func (t *TableRecover) ID() string {
	return fmt.Sprintf("%s/%d", t.path, t.time)
}

// ExecutedGTID for gtid and timestamp
func (t *TableRecover) ExecutedGTID() string {
	return t.pos.executed.String()
}

// selectLogs to apply MySQL binlog
func (t *TableRecover) selectLogs(start, end int64) (int64s, error) {
	ss, err := latestTime(start, t.path)
	if err != nil {
		log.Errorf("get latestTime log file according timestamp{%d} error{%v}", start, err)
		return nil, err
	}

	return rangeLogs(ss, end, t.path)
}

// Recover
func (t *TableRecover) Recover() {
	defer func() {
		if err := recover(); err != nil {
			if strings.EqualFold(err.(error).Error(), done) {
				// do nothing
				log.Infof("recover table {%s} done for timestamp{%d}", t.table, t.time)
			} else if strings.EqualFold(err.(error).Error(), canceled) {
				log.Info("table {%s} recovery canceled", t.table)
			} else {
				t.errs <- err.(error)
			}
		}
		log.Infof("table {%s} recover finish", t.table)

		// close instance
		if t.i != nil {
			t.i.Close()
		}

		// decrease
		t.wg.Done()
	}()

	// New local MySQL connection POOl
	ins, err := bpct.NewInstance(t.user, t.pass, t.port)
	if err != nil {
		log.Errorf("create MySQL instance error %v", err)
		t.errs <- err
		return
	}

	t.i = ins

	// take selected log files
	lfs, err := t.selectLogs(int64(t.off.Time), t.time)
	if err != nil {
		log.Errorf("select logs[%d, %d] error %v", t.off.Time, t.time, err)
		t.errs <- err
		return
	}

	if len(lfs) == 0 {
		log.Warnf("no logs for table {%s} to recover", t.table)
		return
	}

	onEventFunc := func(e *replication.BinlogEvent) error {
		select {
		case <-t.ctx.Done():
			panic(fmt.Errorf(canceled))
		default:
			if int64(e.Header.Timestamp) > t.time {
				log.Infof("table{%s}, event pos{%d}, event type{%s}", t.table, e.Header.LogPos, e.Header.EventType)
				panic(fmt.Errorf(done))
			}

			// update timestamp
			t.pos.UpdateTime(e.Header.Timestamp)

			switch e.Header.EventType {
			case replication.FORMAT_DESCRIPTION_EVENT:
				if err := t.i.Begin(); err != nil {
					log.Error(err)
					panic(err)
				}

				t.desc = utils.Base64Encode(e.RawData)
				// sql executor
				if err := t.i.Execute([]byte(fmt.Sprintf("BINLOG '\n%s\n'%s", t.desc, inter.Delimiter))); err != nil {
					log.Errorf("execute binlog description event error{%v}", err)
					panic(err)
				}

				// sql executor commit
				if err := t.i.Commit(); err != nil {
					log.Errorf("execute binlog desc event commit error{%v}", err)
					panic(err)
				}
			case replication.QUERY_EVENT:
				if t.cg != nil && t.og.Contain(t.cg) {
					log.Debugf("current gtid{%s} already executed on snapshot {%s}", t.cg.String(), t.off.ExedGtid)
					return nil
				}

				qe := e.Event.(*replication.QueryEvent)
				switch strings.ToUpper(string(qe.Query)) {
				case "BEGIN":
					if err := t.i.Begin(); err != nil {
						log.Error(err)
						panic(err)
					}
				case "COMMIT":
					if err := t.i.Commit(); err != nil {
						log.Error(err)
						panic(err)
					}
				case "ROLLBACK":
				case "SAVEPOINT":
				default:

					b, err := conf.IsFilteredSQL(qe.Query)
					if err != nil {
						log.Warnf("ddl %s filtered check error{%v}", qe.Query, err)
						panic(err)
					}

					if b {
						log.Warnf("ddl %s is filtered", qe.Query)
						return nil
					}

					if err := t.coordinate(qe); err != nil {
						panic(err)
					}

					if err := t.i.Begin(); err != nil {
						log.Error(err)
						panic(err)
					}
					// first use db
					if e.Header.Flags&inter.LogEventSuppressUseF == 0 && qe.Schema != nil && len(qe.Schema) != 0 {
						use := fmt.Sprintf("use %s", qe.Schema)
						if err := t.i.Execute([]byte(use)); err != nil {
							log.Errorf("sql {%s} execute error{%v}", use, err)
							panic(err)
						}
					}

					// then execute ddl
					if err := t.i.Execute(qe.Query); err != nil {
						log.Error(err)
						panic(err)
					}
					if err := t.i.Commit(); err != nil {
						log.Error(err)
						panic(err)
					}
				}
			case replication.XID_EVENT:
				if t.cg != nil && t.og.Contain(t.cg) {
					log.Debugf("current gtid{%s} already executed on snapshot {%s}", t.cg.String(), t.off.ExedGtid)
					return nil
				}

				if err := t.i.Commit(); err != nil {
					log.Error(err)
					panic(err)
				}
			case replication.TABLE_MAP_EVENT:
				if t.cg != nil && t.og.Contain(t.cg) {
					log.Debugf("current gtid{%s} already executed on snapshot {%s}", t.cg.String(), t.off.ExedGtid)
					return nil
				}
				// write to buffer first
				t.buffer.WriteString(fmt.Sprintf("BINLOG '\n%s", utils.Base64Encode(e.RawData)))
			case replication.WRITE_ROWS_EVENTv0,
				replication.WRITE_ROWS_EVENTv1,
				replication.WRITE_ROWS_EVENTv2,
				replication.DELETE_ROWS_EVENTv0,
				replication.DELETE_ROWS_EVENTv1,
				replication.DELETE_ROWS_EVENTv2,
				replication.UPDATE_ROWS_EVENTv0,
				replication.UPDATE_ROWS_EVENTv1,
				replication.UPDATE_ROWS_EVENTv2:
				// check current gtid
				if t.cg != nil && t.og.Contain(t.cg) {
					log.Debugf("current gtid{%s} already executed on snapshot {%s}", t.cg.String(), t.off.ExedGtid)
					return nil
				}

				// all data into no uniq check and foreign key check
				// all rows into no foreign key check and on uniq key check
				re := e.Event.(*replication.RowsEvent)
				re.Flags = re.Flags | clog.RowEventNoForeignKeyChecks | clog.RowEventNoUniqueKeyChecks

				fs := make([]byte, 2)
				binary.LittleEndian.PutUint16(fs, re.Flags)
				e.RawData[replication.EventHeaderSize+re.RowsHeader.FlagsPos] = fs[0]
				e.RawData[replication.EventHeaderSize+re.RowsHeader.FlagsPos+1] = fs[1]

				t.buffer.WriteString(fmt.Sprintf("\n%s", utils.Base64Encode(e.RawData)))

				if e.Event.(*replication.RowsEvent).Flags == StmtEndFlag {
					t.buffer.WriteString(fmt.Sprintf("\n'%s", inter.Delimiter))

					// execute
					bs := t.buffer.Bytes()
					if err := t.i.Execute(bs); err != nil {
						log.Errorf("table {%s} execute on gtid{%s} error{%v}", t.table, t.cg.String(), err)
						panic(err)
					}

					// reset buffer for reuse
					t.buffer.Reset()
				}
			case replication.GTID_EVENT:
				g := e.Event.(*replication.GTIDEvent)

				u, err := uuid.FromBytes(g.SID)
				if err != nil {
					log.Errorf("parse uuid from gtid event error{%v}", err)
					panic(err)
				}

				s := fmt.Sprintf("%s:%d", u.String(), g.GNO)
				c, err := mysql.ParseMysqlGTIDSet(s)
				if err != nil {
					log.Errorf("parse og{%s} error {%v}", s, err)
					panic(err)
				}
				t.cg = c

				if err := t.pos.UpdateGTID(s); err != nil {
					log.Errorf("update gtid{%s} to merged gtid set{%s} error{%v}", s, t.pos.String(), err)
					panic(err)
				}
			}
			return nil
		}
	}

	// range log files
	for _, l := range lfs {
		// parse each local binlog files
		lf := fmt.Sprintf("%s/%d%s", t.path, l, inter.LogSuffix)
		log.Debugf("parse binlog file{%s}", lf)

		if err := t.parser.ParseFile(lf, 4, onEventFunc); err != nil {
			if strings.EqualFold(err.Error(), done) {
				// do nothing
				log.Infof("recover table {%s} done for timestamp{%d}", t.table, t.time)
				return
			}

			if strings.EqualFold(err.Error(), canceled) {
				log.Info("table {%s} recovery canceled", t.table)
				return
			}

			// write check to io writer
			log.Error("parse file error ", lf, ", error ", err)
			t.errs <- err
			return
		}
	}
}

// coordinate with other table in case of interact influence
func (t *TableRecover) coordinate(qe *replication.QueryEvent) error {
	b, err := conf.IsCoordinateSQL(qe.Query)
	if err != nil {
		return err
	}

	// is coordinate sql then retrieve table regs
	if b {
		log.Infof("match coordinate sql")
		tbReg, err := conf.GetTables(qe.Query)
		if err != nil {
			return err
		}

		// ddl related tables
		ac := make(chan interface{}, 2)
		t.coCh <- &SyncData{
			Table:    t.table,
			GTID:     t.cg.String(),
			DDL:      qe.Query,
			AckCh:    ac,
			TableReg: tbReg,
		}

		for {
			select {
			case <-t.ctx.Done():
				log.Warnf("context done on table {%s}", t.table)
				return nil
			case a := <-ac:
				d := a.(*AckData)
				if d.Err != nil {
					log.Errorf("ack from coordinate error{%v}", d.Err)
					return d.Err
				}

				log.Infof("ack from coordinate success full on gtid event{%s}", d.GTID)
				return nil
			}
		}
	}

	// ddl related to more than 1 tables then it should using coordinator
	ddlTbs := make(map[string]string)
	for _, ddl := range bytes.Split(qe.Query, []byte(";")) {
		if tbs, matched := regx.Parse(ddl, qe.Schema); matched { // 匹配表名成功
			for _, tb := range tbs {
				ddlTbs[string(tb)] = string(tb)
			}
		}
	}

	if len(ddlTbs) > 1 {
		var tbs []string
		for tb := range ddlTbs {
			tbs = append(tbs, tb)
		}

		// ddl related tables
		ac := make(chan interface{}, 2)
		t.coCh <- &SyncData{
			Table:         t.table,
			GTID:          t.cg.String(),
			DDL:           qe.Query,
			AckCh:         ac,
			RelatedTables: tbs,
		}

		for {
			select {
			case <-t.ctx.Done():
				log.Warnf("context done on table {%s}", t.table)
				return nil
			case a := <-ac:
				d := a.(*AckData)
				if d.Err != nil {
					log.Errorf("ack from coordinate error{%v}", d.Err)
					return d.Err
				}

				log.Infof("ack from coordinate success full on all related tables %v", tbs)
				return nil
			}
		}
	}

	return nil
}
