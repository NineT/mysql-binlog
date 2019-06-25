package log

import (
	"encoding/json"
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
			DumpFile: "mysql-bin.000001",
			DumpPos:  4,
			Local: &meta.Offset{
				CID:      100,
				ExedGtid: "9042c001-a53d-4ffd-ab93-9c7ac202c2cb:1-1030030",
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
	w, err := NewIndexWriter("/export/backup/1/test.t_name", 1561362616)
	if err != nil {
		log.Fatal(err)
	}

	defer w.Close()

	o, err := w.Tail()
	if err != nil {
		log.Fatal(err)
	}

	if o == nil {
		fmt.Println("empty file")
		return
	}

	fmt.Println(o.DumpFile)
}

func TestIndexExists(t *testing.T) {
	js := `{"file":"mysql-bin.000001","pos":1417,"local":{"clusterid":1,"exedgtid":"NGY5ZWM0ZjItNDE4MC0xMWU5LWEWM0ZjItNDE4MC0xMWU5LWE2NmMtOGMxNjQ1MzUwYmM4OjEtNg==","trxgtid":"NGY5ZWM0ZjItNDE4MC0xMWU5LWE2NmMtOGMxNjQ1MzUwYmM4OjY=","time":1561362618,"file":"/export/backup/1/test.t_name/1561362616.log","pos":408}}`
	o := IndexOffset{}
	if err := json.Unmarshal([]byte(js), &o); err != nil {
		log.Fatal(err)
	}
	log.Info(o.DumpFile)
}
