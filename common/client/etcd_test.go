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

func TestEtcdMeta_Read(t *testing.T) {
	m, err := NewEtcdMeta("http://127.0.0.1:2379", "v1")
	if err != nil {
		log.Fatal(err)
	}

	o, err := m.Read(0)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(o.BinFile)
	fmt.Println(o.BinPos)
	fmt.Println(string(o.IntGtid))
	fmt.Println(string(o.SinGtid))
}

func TestEtcdMeta_Save(t *testing.T) {
	m, err := NewEtcdMeta("http://127.0.0.1:2379", "v1")
	if err != nil {
		log.Fatal(err)
	}

	err = m.Save(&meta.Offset{
		ClusterID: 100,
		IntGtid:   []byte("2d784ad8-8f7a-4916-858e-d7069e5a24b2:1-30000"),
		Time:      1560868592,
		BinFile:   "mysql-bin.000242",
		BinPos:    154,
		Counter:   0,
		Header:    true,
		SinGtid:   []byte("2d784ad8-8f7a-4916-858e-d7069e5a24b2:1-3000"),
	})
	if err != nil {
		log.Fatal(err)
	}
}

func TestEtcdMeta_Delete(t *testing.T) {
	m, err := NewEtcdMeta("http://127.0.0.1:2379", "v1")
	if err != nil {
		log.Fatal(err)
	}

	log.Fatal(m.Delete(0))
}
