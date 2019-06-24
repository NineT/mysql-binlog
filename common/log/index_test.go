package log

import (
	"fmt"
	"github.com/mysql-binlog/common/meta"
	"github.com/zssky/log"
	"testing"
)

func TestNewIndexWriter(t *testing.T) {
	w, err := NewIndexWriter("/tmp", 1333)
	if err != nil {
		log.Fatal(err)
	}

	defer w.Close()
}

func TestIndexWriter_Write(t *testing.T) {
	w, err := NewIndexWriter("/tmp", 2482974972)
	if err != nil {
		log.Fatal(err)
	}

	defer w.Close()

	for i := 0; i < 1; i ++ {
		if err := w.Write(&IndexOffset{
			Dump: &meta.Offset{
				CID:      100,
				ExedGtid: []byte("9042c001-a53d-4ffd-ab93-9c7ac202c2cb:1-1030030"),
				Time:     2482974972 + uint32(i)*100,
				BinFile:  "mysql-bin.000014",
				BinPos:   49274 + uint32(i)*100,
			},
			Local: &meta.Offset{
				CID:      100,
				ExedGtid: []byte("9042c001-a53d-4ffd-ab93-9c7ac202c2cb:1-1030030"),
				Time:     2482974972 + uint32(i)*100,
				BinFile:  "2482974972.log",
				BinPos:   28000 + uint32(i)*100,
			},
		}); err != nil {
			log.Fatal(err)
		}
	}
}

func TestIndexWriter_Latest(t *testing.T) {
	w, err := NewIndexWriter("/tmp", 2482974972)
	if err != nil {
		log.Fatal(err)
	}

	defer w.Close()

	o, err := w.Latest()
	if err != nil {
		log.Fatal(err)
	}

	if o == nil {
		fmt.Println("empty file")
		return
	}

	fmt.Println(o.Dump.Time)
}
