package mode

import (
	"context"
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
	if m.LatestPos.GTIDSet != nil && m.LatestPos.GTIDSet.String() != "" {
		s, err := syncer.StartSyncGTID(m.LatestPos.GTIDSet)
		if err != nil {
			panic(err)
		}
		streamer = s
	} else {
		s, err := syncer.StartSync(mysql.Position{
			Name: m.LatestPos.BinlogFile,
			Pos:  uint32(m.LatestPos.BinlogPos),
		})
		if err != nil {
			panic(err)
		}
		streamer = s
	}


	flag := true
	for flag {
		ev, err := streamer.GetEvent(context.Background())
		if err != nil {
			panic(err)
		}
		flag = f(ev)
	}
}
