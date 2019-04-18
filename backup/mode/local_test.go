package mode

import (
	"fmt"
	"testing"

	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
	"runtime/debug"
	"strings"

	"github.com/mysql-binlog/siddontang/go-mysql/mysql"
	"github.com/mysql-binlog/siddontang/go-mysql/replication"

	"github.com/mysql-binlog/common/regx"
	"github.com/mysql-binlog/common/utils"
)

func TestReadBinlogFile(t *testing.T) {
	pos := &mysql.Position{
		//Name: "/tmp/sec_1551166500_1551166505.log",
		//Name: "/export/backup/store/127.0.0.1/test.mq/sec_1551163429_1551166400.log",
		//Name: "/export/backup/store/127.0.0.1/test.mq/sec_1551166500_1551166505.log",
		Name: "/export/servers/mysql/data/mysql-bin.000002",
		Pos:  uint32(4207)}

	parser := replication.NewBinlogParser()

	handler := func(e *replication.BinlogEvent) error {
		switch e.Header.EventType {
		case replication.FORMAT_DESCRIPTION_EVENT:
			/**
						crc32  1 = 145
			crc32  2 = 243
			crc32  3 = 208
			crc32  4 = 209
			new crc32  0 = 145
			new crc32  1 = 243
			new crc32  2 = 208
			new crc32  3 = 209
			*/
			handleFormatDescEvent(e)

			compareDesc(e)
		case replication.ROTATE_EVENT:
			handleRotateEvent(e, pos)
		case replication.QUERY_EVENT:
			handleQueryEvent(e)

			qe, _ := e.Event.(*replication.QueryEvent)

			fmt.Println(string(qe.Query))

			switch strings.ToUpper(string(qe.Query)) {
			case "BEGIN":
			case "COMMIT":
			case "ROLLBACK":
			case "SAVEPOINT":
			default:
				// here ddl may have two statement
				fmt.Println("schema ", string(qe.Schema), " ddl ", string(qe.Query))

				for _, ddl := range bytes.Split(qe.Query, []byte(";")) {
					if tbs, matched := regx.RegMatch(qe.Schema, ddl); matched { // 匹配表名成功
						for _, tb := range tbs {
							fmt.Println(string(tb))
						}
					}
				}
			}

		case replication.WRITE_ROWS_EVENTv2:
			handleInsertEvent(e)
		case replication.UPDATE_ROWS_EVENTv2:
			handleUpdateEvent(e)
		case replication.DELETE_ROWS_EVENTv2:
			handleDeleteEvent(e)
		}
		return nil
	}
	for {
		if err := parser.ParseFile(pos.Name, int64(pos.Pos), handler); err != nil {
			fmt.Println(err)
			debug.PrintStack()
			break
		}
	}
}

func compareDesc(e *replication.BinlogEvent) {
	desc := e.Event.(*replication.FormatDescriptionEvent)
	if bytes.Equal(e.RawData[19:len(e.RawData)-4], desc.Encode()) {
		fmt.Println("bytes are equals")
	} else {
		fmt.Println("bytes not equals")
	}

	if bytes.Equal(desc.Copy().(*replication.FormatDescriptionEvent).Encode(), e.RawData[19:len(e.RawData)-4]) {
		fmt.Println("重新编码 再拷贝 再解码 数据仍然一致 ")
	} else {
		fmt.Println("重新编码 再拷贝 再解码 数据不一致 ")
	}
}

func handleDeleteEvent(e *replication.BinlogEvent) {
	wre := e.Event.(*replication.RowsEvent)
	fIndex := wre.RowValueIndex[len(wre.RowValueIndex)-1]
	fmt.Println("raw data ", e.RawData)
	fmt.Fprintf(os.Stdout, "%s\n", utils.Base64Encode(e.RawData))

	crc := crc32.ChecksumIEEE(e.RawData[:fIndex])
	fmt.Println(crc&0xff, "\t", (crc>>8)&0xff, "\t", (crc>>16)&0xff, "\t", (crc>>24)&0xff)

	fmt.Println("index is onver")
}

func handleUpdateEvent(e *replication.BinlogEvent) {
	wre := e.Event.(*replication.RowsEvent)
	fIndex := wre.RowValueIndex[len(wre.RowValueIndex)-1]
	fmt.Println("raw data ", e.RawData)
	fmt.Fprintf(os.Stdout, "%s\n", utils.Base64Encode(e.RawData))

	crc := crc32.ChecksumIEEE(e.RawData[:fIndex])
	fmt.Println(crc&0xff, "\t", (crc>>8)&0xff, "\t", (crc>>16)&0xff, "\t", (crc>>24)&0xff)

	fmt.Println("index is onver")
}

func handleInsertEvent(e *replication.BinlogEvent) {
	wre := e.Event.(*replication.RowsEvent)
	fIndex := wre.RowValueIndex[len(wre.RowValueIndex)-1]
	fmt.Println("raw data ", e.RawData)
	fmt.Fprintf(os.Stdout, "%s\n", utils.Base64Encode(e.RawData))

	crc := crc32.ChecksumIEEE(e.RawData[:fIndex])
	fmt.Println(crc&0xff, "\t", (crc>>8)&0xff, "\t", (crc>>16)&0xff, "\t", (crc>>24)&0xff)

	fmt.Println("index is over")
}

func handleQueryEvent(e *replication.BinlogEvent) {
	qe := e.Event.(*replication.QueryEvent)
	fmt.Println("schema ", string(qe.Schema), ", query ", string(qe.Query))
	fmt.Println(qe)
}

func handleRotateEvent(e *replication.BinlogEvent, pos *mysql.Position) {
	re := e.Event.(*replication.RotateEvent)
	pos.Pos = uint32(re.Position)
	pos.Name = string(re.NextLogName)

	raw := e.RawData[19 : len(e.RawData)-4]
	rt := re.Encode()
	if bytes.Equal(rt, raw) {
		fmt.Println("rotate event using raw data")
	}
	// real crc32
	for i, b := range e.RawData[len(e.RawData)-4:] {
		fmt.Println("rotate crc32 ", i, "=", int(b))
	}

	cr := make([]byte, 4)

	// 取初4个 crc32 字节意外的所有字节做crc校验
	crc := crc32.ChecksumIEEE(e.RawData[:len(e.RawData)-4])

	binary.LittleEndian.PutUint32(cr, crc)

	for i, b := range cr {
		fmt.Println("new crc32 ", i, "=", int(b))
	}
}

func handleFormatDescEvent(e *replication.BinlogEvent) {
	desc := e.Event.(*replication.FormatDescriptionEvent)

	ecs := desc.Encode()
	raw := e.RawData[19 : len(e.RawData)-5]
	if bytes.Equal(ecs, raw) {
		// 比较两者字节是否相等
		fmt.Println("format description event decode and encode are consistently")
	} else {
		fmt.Println("format description event decode != encode bytes")
	}

	// real crc32
	for i, b := range e.RawData[len(e.RawData)-5:] {
		fmt.Println("crc32 ", i, "=", int(b))
	}

	cr := make([]byte, 4)

	// 取初4个 crc32 字节意外的所有字节做crc校验
	crc := crc32.ChecksumIEEE(e.RawData[:len(e.RawData)-4])

	binary.LittleEndian.PutUint32(cr, crc)

	for i, b := range cr {
		fmt.Println("new crc32 ", i, "=", int(b))
	}

	crc = crc32.ChecksumIEEE(e.RawData[:17])
	crc = crc32.Update(crc, crc32.IEEETable, e.RawData[17:19])
	crc = crc32.Update(crc, crc32.IEEETable, e.RawData[19:len(e.RawData)-4])

	fmt.Println("data size ", len(e.RawData)-4, ", header data size ", e.Header.EventSize)

	binary.LittleEndian.PutUint32(cr, crc)

	for i, b := range cr {
		fmt.Println("get calculate crc32 ", i, "=", int(b))
	}

	fmt.Println("over")
}

func TestLocalMode_Handle(t *testing.T) {
	a := "7ujnm8ikm9ok,0ol.,7uj6yh65tg6yh87ujm"
	fmt.Println(a[0 : len(a)-1])
	fmt.Println(a[:len(a)-2])
	fmt.Println(a[:len(a)-3])
	fmt.Println(a[:len(a)-4])
	fmt.Println(a[:len(a)-5])
}
