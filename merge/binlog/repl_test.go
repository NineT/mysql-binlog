package binlog

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"reflect"
	"testing"
	"time"

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
		ev, _ := streamer.GetEvent(context.Background())
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

		case replication.DELETE_ROWS_EVENTv0,
			replication.DELETE_ROWS_EVENTv1,
			replication.DELETE_ROWS_EVENTv2:

		case replication.UPDATE_ROWS_EVENTv0,
			replication.UPDATE_ROWS_EVENTv1,
			replication.UPDATE_ROWS_EVENTv2:
			re, _ := ev.Event.(*replication.RowsEvent)

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
