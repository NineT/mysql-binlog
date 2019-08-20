package res

import (
	"context"
	"regexp"
	"strings"
	"sync"

	"github.com/zssky/log"

	"github.com/mysql-binlog/common/inter"
	"github.com/mysql-binlog/common/meta"

	"github.com/mysql-binlog/recover/bpct"
)

/***
* coordinates for ddl conflict
* eg: RENAME TABLE `b2b_trade100`.`b2b_order_main` TO `b2b_trade100`.`_b2b_order_main_old`, `b2b_trade100`.`_b2b_order_main_new` TO `b2b_trade100`.`b2b_order_main`"
* cross three tables including three threads
*/

// Coordinator
type Coordinator struct {
	SyncCh  chan interface{}              // SyncCh channel for SyncData
	wg      *sync.WaitGroup               // wait group for new adding
	ctx     context.Context               // ctx Context
	inst    *bpct.Instance                // instance
	counter map[string]int                // counter
	acks    map[string][]chan interface{} // ack channels
	tables map[string]string              // tables
	time   int64                          // final time for binlog recover
	off    *meta.Offset                   // start offset
	path   string                         // table binlog path
	user   string                         // mysql user
	pass   string                         // mysql pass
	port   int                            // mysql port
	errs   chan error                     // error channel
}

// SyncData coordinate request
type SyncData struct {
	Table         string           // full table name
	GTID          string           // current gtid event
	DDL           []byte           // ddl sql
	AckCh         chan interface{} // ack channel for coordinate to send back
	RelatedTables []string         // related tables
	TableReg      string           // table regular
}

// AckData to client that current gtid is done continue
type AckData struct {
	GTID string // current gtid event
	Err  error  // Err occur
}

// NewCoordinator for table syncer
func NewCoordinator(user, pass string, port int, time int64, off *meta.Offset, clusterPath string, tbs []string, wg *sync.WaitGroup, ctx context.Context, errs chan error) (*Coordinator, error) {
	i, err := bpct.NewInstance(user, pass, port)
	if err != nil {
		log.Errorf("new coordinator Err{%v}", err)
		return nil, err
	}

	tables := make(map[string]string)
	for _, tb := range tbs {
		tables[tb] = tb
	}

	return &Coordinator{
		SyncCh:  make(chan interface{}, 64),
		ctx:     ctx,
		wg:      wg,
		inst:    i,
		counter: make(map[string]int),
		acks: make(map[string][]chan interface{}),
		tables: tables,

		time: time,
		off:  off,
		path: clusterPath,
		user: user,
		pass: pass,
		port: port,
		errs: errs,
	}, nil
}

// Sync data
func (c *Coordinator) Sync() {
	defer c.inst.Close()

	for {
		select {
		case <-c.ctx.Done():
			log.Warnf("context done for Coordinator")
			return
		case s, hasMore := <-c.SyncCh:
			if hasMore {
				// channel is closed
				log.Warnf("channel on coordinator is closed")
				return
			}

			// sync data
			d := s.(*SyncData)

			if d.TableReg != "" {
				// take related tables
				var rtbs []string
				reg := regexp.MustCompile(d.TableReg)
				for _, t := range c.tables {
					if reg.Match([]byte(t)) {
						rtbs = append(rtbs, t)
					}
				}
				d.RelatedTables = rtbs
			}

			// std table
			tb := inter.CharStd(strings.TrimSpace(d.Table))

			// check all table is on recovering
			if _, ok := c.tables[tb]; !ok {
				// todo start table recover in case not exist
				c.wg.Add(1)

				// table on recovering
				log.Warnf("table {%s} no on recovering list now to start", d.Table)

				tr, err := NewTable(tb, c.path, c.time, c.ctx, c.off, c.user, c.pass, c.port, c.wg, c.errs, c.SyncCh)
				if err != nil {
					// error occur then exit
					log.Errorf("error for table {%s} recover error{%v}", tb, err)
					c.errs <- err
					return
				}

				go tr.Recover()
				c.tables[tb] = tb
			}

			// remember channel as the communicator between coordinator and table routing
			if _, ok := c.acks[d.GTID]; ok {
				c.acks[d.GTID] = append(c.acks[d.GTID], d.AckCh)
			} else {
				var chs []chan interface{}
				chs = append(chs, d.AckCh)
				c.acks[d.GTID] = chs
			}

			log.Infof("table {%s} arrived to gtid{%s} for ddl{%s}", d.Table, d.GTID, d.DDL)
			_, ok := c.counter[d.GTID]
			if ok {
				// if counter already exists then minus as the table message come
				c.counter[d.GTID] = c.counter[d.GTID] - 1
			} else {
				// current table hold 1 position
				c.counter[d.GTID] = len(d.RelatedTables) - 1
			}

			// if c.counter[d.GTID] == 0
			if c.counter[d.GTID] == 0 {
				log.Infof("all tables{%s} are arrived to the same gtid{%s}", d.RelatedTables, d.GTID)

				// current gtid counter is equals to 0, means all table have arrived to this gtid event
				delete(c.counter, d.GTID) // remove gtid from map

				a := AckData{
					GTID: d.GTID,
					Err:  nil,
				}

				// execute current ddl on instance once
				if err := c.inst.Execute(d.DDL); err != nil {
					// execute current ddl Err
					a.Err = err
				}

				log.Infof("write ack to each channel")
				for _, ch := range c.acks[d.GTID] {
					ch <- a
				}
				delete(c.acks, d.GTID) // remove gtid from map
			}
		}
	}
}
