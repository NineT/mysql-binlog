package mode

import (
	"github.com/zssky/log"

	"github.com/mysql-binlog/siddontang/go-mysql/mysql"
	"github.com/mysql-binlog/siddontang/go-mysql/replication"

	"github.com/mysql-binlog/common/db"
	"github.com/mysql-binlog/common/final"
	"github.com/mysql-binlog/common/inter"
)

// RemoteMode remote mode from creating slave dump connection to MySQL
type RemoteMode struct {
	LatestPos *inter.BinlogPosition
	Config    *db.MetaConf
}

// Handle handle data from reading binlog events on connection
func (m *RemoteMode) Handle(f func(ev *replication.BinlogEvent) bool, a *final.After) {
	defer func() {
		a.After()
	}()
	cfg := replication.BinlogSyncerConfig{
		ServerID: 1011,
		Flavor:   "mysql",
		Host:     m.Config.Host,
		Port:     uint16(m.Config.Port),
		User:     m.Config.User,
		Password: m.Config.Password,
	}

	syncer := replication.NewBinlogSyncer(cfg)
	defer syncer.Close()

	var streamer *replication.BinlogStreamer
	var err error
	if m.LatestPos.GTIDSet != nil && m.LatestPos.GTIDSet.String() != "" {
		if streamer, err = syncer.StartSyncGTID(m.LatestPos.GTIDSet); err != nil {
			log.Error("error sync data using gtid ", err)
			panic(err)
		}
	} else {
		if streamer, err = syncer.StartSync(mysql.Position{
			Name: m.LatestPos.BinlogFile,
			Pos:  uint32(m.LatestPos.BinlogPos),
		}); err != nil {
			log.Error("err sync data using binlog file & offset ", err)
			panic(err)
		}
	}

	// true means continue, false means to stop
	flag := true
	for flag {
		select {
		case e := <-a.Errs:
			// wait for errors
			panic(e)
			return
		default:
			ev, err := streamer.GetEvent(a.Ctx)
			if err != nil {
				log.Error("error handle binlog event ", err)
				panic(err)
				return
			}
			flag = f(ev)
		}
	}
}
