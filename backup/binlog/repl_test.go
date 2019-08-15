package binlog

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/zssky/log"

	"github.com/mysql-binlog/siddontang/go-mysql/mysql"
	"github.com/mysql-binlog/siddontang/go-mysql/replication"
)

var syncer *replication.BinlogSyncer

func initSyncer() {
	cfg := replication.BinlogSyncerConfig{
		ServerID: 100,
		Flavor:   "mysql",
		Host:     "127.0.0.1",
		Port:     3306,
		User:     "root",
		Password: "secret",
	}

	syncer = replication.NewBinlogSyncer(cfg)
}

func TestNewEventHandler(t *testing.T) {
	g1 := `4f9ec4f2-4180-11e9-a66c-8c1645350bc8:1-3`
	g2 := `4f9ec4f2-4180-11e9-a66c-8c1645350bc8:4`

	gs1, err := mysql.ParseMysqlGTIDSet(g1)
	if err != nil {
		log.Fatal(err)
	}

	if err := gs1.Update(g2); err != nil {
		log.Fatal(err)
	}

	log.Info("update gtid successfully ", gs1.String())
}

func TestBinlogReplication(t *testing.T) {
	initSyncer()
	defer syncer.Close()

	file := "mysql-bin.000001"
	pos := uint32(4)
	streamer, _ := syncer.StartSync(
		mysql.Position{
			Name: file,
			Pos:  pos})

	for {
		ev, err := streamer.GetEvent(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		switch ev.Header.EventType {
		case replication.UNKNOWN_EVENT:
		case replication.START_EVENT_V3:
		case replication.QUERY_EVENT:
			qe, _ := ev.Event.(*replication.QueryEvent)
			fmt.Println(string(qe.Query))
		case replication.STOP_EVENT:
		case replication.ROTATE_EVENT:
			re, _ := ev.Event.(*replication.RotateEvent)
			fmt.Println(re.NextLogName)
		case replication.INTVAR_EVENT:
		case replication.LOAD_EVENT:
		case replication.SLAVE_EVENT:
		case replication.CREATE_FILE_EVENT:
		case replication.APPEND_BLOCK_EVENT:
		case replication.EXEC_LOAD_EVENT:
		case replication.DELETE_FILE_EVENT:
		case replication.NEW_LOAD_EVENT:
		case replication.RAND_EVENT:
		case replication.USER_VAR_EVENT:
		case replication.FORMAT_DESCRIPTION_EVENT:
		case replication.XID_EVENT:
		case replication.BEGIN_LOAD_QUERY_EVENT:
		case replication.EXECUTE_LOAD_QUERY_EVENT:
		case replication.TABLE_MAP_EVENT:
			tme, _ := ev.Event.(*replication.TableMapEvent)
			fmt.Println(string(tme.Table))

		case replication.WRITE_ROWS_EVENTv0,
			replication.WRITE_ROWS_EVENTv1,
			replication.WRITE_ROWS_EVENTv2:
			re, _ := ev.Event.(*replication.RowsEvent)
			fmt.Println("write rows header length ", len(re.RowsHeader.Header))

		case replication.DELETE_ROWS_EVENTv0,
			replication.DELETE_ROWS_EVENTv1,
			replication.DELETE_ROWS_EVENTv2:
			re, _ := ev.Event.(*replication.RowsEvent)
			fmt.Println("delete rows header length ", len(re.RowsHeader.Header))

		case replication.UPDATE_ROWS_EVENTv0,
			replication.UPDATE_ROWS_EVENTv1,
			replication.UPDATE_ROWS_EVENTv2:
			re, _ := ev.Event.(*replication.RowsEvent)
			fmt.Println("update header length ", len(re.RowsHeader.Header))

			for idx, colVal := range re.Rows[0] {
				fmt.Println("column index ", idx, " type is ", reflect.TypeOf(colVal), reflect.TypeOf(colVal).Kind())
				switch reflect.TypeOf(colVal).Kind() {
				case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					fmt.Println(fmt.Sprintf("%d", colVal))
				case reflect.Float32, reflect.Float64:
					fmt.Println(fmt.Sprintf("%f", colVal))
				case reflect.Slice:
					v := colVal.([]uint8)
					buffer := bytes.NewBuffer([]byte("0x"))
					for i := 0; i < len(v); i++ {
						buffer.WriteString(fmt.Sprintf("%.2x", v[i]))
					}
					fmt.Println(buffer)
				case reflect.String:
					v := mysql.Escape(colVal.(string))
					fmt.Println(v)
					if err := ioutil.WriteFile("./output2.txt", []byte(v), 0666); err != nil {
						fmt.Println(err.Error())
					}
				}
			}

			fmt.Println(re.Rows)

		case replication.INCIDENT_EVENT:
		case replication.HEARTBEAT_EVENT:
		case replication.IGNORABLE_EVENT:
		case replication.ROWS_QUERY_EVENT:
		case replication.GTID_EVENT:
		case replication.ANONYMOUS_GTID_EVENT:
		case replication.PREVIOUS_GTIDS_EVENT:
		default:
		}
	}
}

func TestLower(t *testing.T) {
	fmt.Println(fmt.Sprintf("%.2x", []byte("a")))
}

func TestString(t *testing.T) {
	str := make([]string, 2)
	str[0] = "abc"

	str[1] = "abccccc"

	fmt.Println(str)
}

func TestTime(t *testing.T) {
	stopTime, _ := time.Parse("2006-01-02 15:04:05", "2018-02-28 10:53:12")
	//fmt.Println(stopTime.Unix())

	stop := stopTime.Unix()

	start := int64(uint32(stop - 100284028482))

	fmt.Println(start)
	fmt.Println(stop)

}

const (
	Q_FLAGS2_CODE               = byte(0)
	Q_SQL_MODE_CODE             = byte(1)
	Q_CATALOG_NZ_CODE           = byte(6)
	Q_AUTO_INCREMENT            = byte(3)
	Q_CHARSET_CODE              = byte(4)
	Q_TIME_ZONE_CODE            = byte(5)
	Q_CATALOG_CODE              = byte(2)
	Q_LC_TIME_NAMES_CODE        = byte(7)
	Q_CHARSET_DATABASE_CODE     = byte(8)
	Q_TABLE_MAP_FOR_UPDATE_CODE = byte(9)
	Q_MASTER_DATA_WRITTEN_CODE  = byte(0x0a)
	Q_INVOKERS                  = byte(0x0b)
	Q_UPDATED_DB_NAMES          = byte(0x0c)
	Q_MICROSECONDS              = byte(0x0d)
)

func TestParseBinlog(t *testing.T) {
	parser := replication.NewBinlogParser()

	begin := 0
	commit := 0
	// true: means continue, false: means arrive the final time
	handler := func(ev *replication.BinlogEvent) error {
		switch ev.Header.EventType {
		case replication.UNKNOWN_EVENT:
		case replication.START_EVENT_V3:
		case replication.QUERY_EVENT:
			qe := ev.Event.(*replication.QueryEvent)
			switch strings.ToUpper(string(qe.Query)) {
			case "BEGIN":
				begin ++
			case "COMMIT":
				commit ++
			default:
				begin ++
				commit ++
				if begin != commit {
					log.Infof("QUERY_EVENT begin = %d, commit = %d", begin, commit)
				}
			}
		case replication.STOP_EVENT:
		case replication.ROTATE_EVENT:
		case replication.INTVAR_EVENT:
		case replication.LOAD_EVENT:
		case replication.SLAVE_EVENT:
		case replication.CREATE_FILE_EVENT:
		case replication.APPEND_BLOCK_EVENT:
		case replication.EXEC_LOAD_EVENT:
		case replication.DELETE_FILE_EVENT:
		case replication.NEW_LOAD_EVENT:
		case replication.RAND_EVENT:
		case replication.USER_VAR_EVENT:
		case replication.FORMAT_DESCRIPTION_EVENT:
			begin ++
			commit ++
			if begin != commit {
				log.Infof("FORMAT_DESCRIPTION_EVENT begin = %d, commit = %d", begin, commit)
			}
		case replication.XID_EVENT:
			commit ++
		case replication.BEGIN_LOAD_QUERY_EVENT:
		case replication.EXECUTE_LOAD_QUERY_EVENT:
		case replication.TABLE_MAP_EVENT:
		case replication.WRITE_ROWS_EVENTv0,
			replication.WRITE_ROWS_EVENTv1,
			replication.WRITE_ROWS_EVENTv2:
		case replication.DELETE_ROWS_EVENTv0,
			replication.DELETE_ROWS_EVENTv1,
			replication.DELETE_ROWS_EVENTv2:
		case replication.UPDATE_ROWS_EVENTv0,
			replication.UPDATE_ROWS_EVENTv1,
			replication.UPDATE_ROWS_EVENTv2:
		case replication.INCIDENT_EVENT:
		case replication.HEARTBEAT_EVENT:
		case replication.IGNORABLE_EVENT:
		case replication.ROWS_QUERY_EVENT:
		case replication.GTID_EVENT:
		case replication.ANONYMOUS_GTID_EVENT:
		case replication.PREVIOUS_GTIDS_EVENT:
		default:
		}
		return nil
	}

	// /home/pengan/1565581287.log /tmp/mysql-bin.000001
	if err := parser.ParseFile("/home/pengan//1565346400.log", int64(4), handler); err != nil {
		return
	}

	log.Infof("begin == %d, commit -= %d", begin, commit)
}
