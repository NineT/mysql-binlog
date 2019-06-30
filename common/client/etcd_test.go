package client

import (
	"fmt"
	"testing"

	"github.com/zssky/log"

	"github.com/mysql-binlog/common/meta"
)

func TestNewEtcdMeta(t *testing.T) {
	_, err := NewEtcdMeta("http://127.0.0.1:2379", "v1")
	if err != nil {
		log.Fatal(err)
	}
}

func TestEtcdMeta_ReadOffset(t *testing.T) {
	m, err := NewEtcdMeta("http://127.0.0.1:2379", "v1")
	if err != nil {
		log.Fatal(err)
	}

	o, err := m.ReadOffset(1)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(o.BinFile)
	fmt.Println(o.BinPos)
	fmt.Println(string(o.ExedGtid))
	fmt.Println(string(o.TrxGtid))
}

func TestEtcdMeta_SaveOffset(t *testing.T) {
	m, err := NewEtcdMeta("http://127.0.0.1:2379", "v1")
	if err != nil {
		log.Fatal(err)
	}

	err = m.SaveOffset(&meta.Offset{
		CID:      100,
		ExedGtid: "2d784ad8-8f7a-4916-858e-d7069e5a24b2:1-30000",
		Time:     1560868592,
		BinFile:  "mysql-bin.000242",
		BinPos:   154,
		Counter:  0,
		Header:   true,
		TrxGtid:  "2d784ad8-8f7a-4916-858e-d7069e5a24b2:1-3000",
	})
	if err != nil {
		log.Fatal(err)
	}
}

func TestEtcdMeta_DeleteOffset(t *testing.T) {
	m, err := NewEtcdMeta("http://127.0.0.1:2379", "v1")
	if err != nil {
		log.Fatal(err)
	}

	log.Fatal(m.DeleteOffset(0))
}

func TestEtcdMeta_DeleteInstance(t *testing.T) {
	m, err := NewEtcdMeta("http://127.0.0.1:2379", "v1")
	if err != nil {
		log.Fatal(err)
	}

	log.Fatal(m.DeleteInstance(100))
}

func TestEtcdMeta_SaveInstance(t *testing.T) {
	m, err := NewEtcdMeta("http://127.0.0.1:2379", "v1")
	if err != nil {
		log.Fatal(err)
	}

	if err := m.SaveInstance(&meta.Instance{
		CID:      100,
		Host:     "127.0.0.1",
		Port:     3306,
		User:     "root",
		Password: "secret",
	}); err != nil {
		log.Fatal(err)
	}
}
