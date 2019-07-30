package utils

import (
	"encoding/json"
	"github.com/mysql-binlog/common/meta"
	"testing"

	"github.com/zssky/log"
)

func TestExecuteShellCmd(t *testing.T) {
	cs := []string{
		"cp shell.go /tmp/",
		"sed -i 's/server-id.*/server-id	= 000000/g' ./my.cnf",
		"sed -i 's/datadir.*/datadir=\\/tmp\\/mysql\\/9292929/g' ./my.cnf",
	}

	for _, c := range cs {
		log.Infof("execute command %s", c)
		if _, _, err := ExeShell(c); err != nil {
			log.Fatal(err)
		}
	}
}

func TestExeShell(t *testing.T) {
	o := &meta.Offset{
		CID: 72164,
		ExedGtid: "60638862-a85e-11e9-b75d-fa16483438ea:1-233",
		TrxGtid: "60638862-a85e-11e9-b75d-fa16483438ea:233",
		Time:  1564468887,
		BinFile: "mysql-bin.000001",
		BinPos: 86795,
	}

	bt, err := json.Marshal(o)
	if err != nil {
		log.Fatal(err)
	}

	log.Infof("%s", string(bt))
}
