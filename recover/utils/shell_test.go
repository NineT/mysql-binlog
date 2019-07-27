package utils

import (
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
