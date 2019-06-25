package log

import (
	"fmt"
	"testing"

	"github.com/zssky/log"
)

func TestNewLogList(t *testing.T) {
	l, err := NewLogList("/tmp")
	if err != nil {
		log.Fatal(err)
	}

	defer l.Close()
}

func TestLogList_Write(t *testing.T) {
	l, err := NewLogList("/tmp")
	if err != nil {
		log.Fatal(err)
	}

	if err := l.Write([]byte("1561366108.log")); err != nil {
		log.Fatal(err)
	}
}

func TestListExists(t *testing.T) {
	fmt.Println(ListExists("/tmp"))
}

func TestRecoverList(t *testing.T) {
	l, err := RecoverList("/tmp")
	if err != nil {
		log.Fatal(err)
	}

	log.Infof("log list{%v}", l)

	if err := l.Write([]byte("mysql-bin.00012424")); err != nil {
		log.Fatal(err)
	}
}