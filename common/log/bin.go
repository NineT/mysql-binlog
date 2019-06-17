package log

import (
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"github.com/mysql-binlog/siddontang/go-mysql/replication"
	"github.com/zssky/log"

	"github.com/mysql-binlog/common/inter"
)

/***
@作用： 生成标准格式的binlog文件
*/

const (
	// CRC32Size crc32 大小
	CRC32Size = 4

	// BinlogBufferSize binlog 事件buffer 大小 默认采用 8k
	BinlogBufferSize = 8 * 1024

	// 	StmtEndFlag        = 1
	StmtEndFlag = 1
)

// DataEvent
type DataEvent struct {
	Header *replication.EventHeader // event header
	Data   []byte                   // data
	Gtid   []byte                   // gtid
	IsDDL  bool                     // is ddl
}

// Binlog 每个生成的binlog文件都对应一个结构
type BinlogWriter struct {
	Name       string                   // FileName file name
	Dir        string                   // table write path
	Desc       *DataEvent               // desc event
	lastHeader *replication.EventHeader // lastHeader
	f          *os.File                 // WriteEvent WriteEvent 生成binlog
	iw         io.Writer                // iw io writer
	compress   bool                     // compress
	xid        uint64                   // transaction id
	crc        uint32                   // crc 32 值
	logPos     uint32                   // logPos 文件位置
}

// NewBinlogWriter new binlog writer for binlog write
func NewBinlogWriter(path, table string, curr uint32, compress bool, desc *DataEvent) (*BinlogWriter, error) {
	w := &BinlogWriter{
		Name:     fmt.Sprintf("%d.log", curr),
		Dir:      fmt.Sprintf("%s%s", path, table),
		Desc:     desc,
		compress: compress,
	}

	f, err := inter.CreateFile(fmt.Sprintf("%s/%s", w.Dir, w.Name))
	if err != nil {
		log.Error(err)
		return nil, err
	}

	w.f = f  // set file
	w.iw = f // set file writer
	if compress { // 开启压缩
		// zlib 压缩数据
		iw := zlib.NewWriter(f)

		// set writer
		w.iw = iw
	}

	w.xid = 0
	w.crc = 0
	w.logPos = 0

	if err := w.writeMagic(); err != nil {
		log.Error(err)
		return nil, err
	}

	// write , write desc
	return w, w.writeDesc(curr)
}

// Binlog2Data: data event from binlog event
func Binlog2Data(ev *replication.BinlogEvent, checksumAlg byte, uuid []byte, ddl bool) *DataEvent {
	if checksumAlg == replication.BINLOG_CHECKSUM_ALG_CRC32 {
		return &DataEvent{
			Header: ev.Header,
			Data:   ev.RawData[replication.EventHeaderSize : len(ev.RawData)-CRC32Size],
			Gtid:   uuid,
			IsDDL:  ddl,
		}
	}

	return &DataEvent{
		Header: ev.Header,
		Data:   ev.RawData[replication.EventHeaderSize:],
		Gtid:   uuid,
		IsDDL:  ddl,
	}
}

// Clear 清除悬挂引用
func (b *BinlogWriter) Clear() {
	b.f = nil
	b.reset()
}

// reset binlog writer
func (b *BinlogWriter) reset() {
	b.xid = 0
	b.crc = 0
	b.logPos = 0
}

// flushLogs : for write rotate event, close the previous log, open the new log file, write file header, write desc event
func (b *BinlogWriter) flushLogs(curr uint32) error {
	// write rotate event
	r := &replication.RotateEvent{
		Position:    uint64(b.logPos),
		NextLogName: []byte(fmt.Sprintf("%d.log", curr)),
	}
	bts := r.Encode()

	b.lastHeader.LogPos = b.logPos
	b.lastHeader.EventSize = uint32(replication.EventHeaderSize) + uint32(len(bts)) + uint32(CRC32Size)

	if err := b.WriteEvent(&DataEvent{
		Header: b.lastHeader,
		Data:   bts,
	}); err != nil {
		return err
	}

	// flush zlib writer
	if b.compress && b.iw != nil {
		if err := b.iw.(*zlib.Writer).Flush(); err != nil {
			log.Error("flush zlib writer ", fmt.Sprintf("%s/%s", b.Dir, b.Name), " error")
		}
	}

	// close binlog file
	if err := b.f.Close(); err != nil {
		// do something if close file error
		log.Error("close binlog file ", fmt.Sprintf("%s/%s", b.Dir, b.Name), " error")
		return err
	}

	b.Name = fmt.Sprintf("%d.log", curr)

	f, err := inter.CreateFile(fmt.Sprintf("%s/%s", b.Dir, b.Name))
	if err != nil {
		log.Error(err)
		return err
	}
	b.reset()

	b.f = f

	if b.compress { // 开启压缩
		// zlib 压缩数据
		iw := zlib.NewWriter(f)

		// set writer
		b.iw = iw
	}

	// write file header
	if err := b.writeMagic(); err != nil {
		log.Error(err)
		return err
	}

	// write desc event
	return b.writeDesc(curr)
}

// WriteEvent common use WriteEvent method {header, body, footer}
func (b *BinlogWriter) WriteEvent(e *DataEvent) error {
	if e.Header.EventType == replication.QUERY_EVENT {
		// ddl take the ddl and promote the transaction number
		b.xid ++
	}

	// reset crc32 value
	b.crc = 0

	// WriteEvent event header:
	size := replication.EventHeaderSize + len(e.Data) + CRC32Size

	// calculate binlog position
	e.Header.LogPos = b.logPos
	ht := e.Header.Encode()
	b.crc = crc32.ChecksumIEEE(ht)

	// write header
	if _, err := b.write(ht); err != nil {
		return err
	}

	// set binlog offset
	b.logPos += uint32(size)

	// write data
	if _, err := b.write(e.Data); err != nil {
		return err
	}

	// calculate crc32  write crc32
	b.crc = crc32.Update(b.crc, crc32.IEEETable, e.Data)

	ct := make([]byte, CRC32Size)
	binary.LittleEndian.PutUint32(ct, b.crc)
	if _, err := b.write(ct); err != nil {
		return err
	}

	// using the last header
	b.lastHeader = e.Header

	return nil
}

// commit write commit event
func (b *BinlogWriter) Commit(h *replication.EventHeader) error {
	b.crc = 0

	// commit event
	c := replication.XIDEvent{
		XID: b.xid,
	}
	cts := c.Encode()

	// log position
	h.LogPos = b.logPos

	// event header size
	s := len(cts) + replication.EventHeaderSize + CRC32Size
	h.EventSize = uint32(s)

	// save new log position
	b.logPos += uint32(s)

	// header
	header := h.Encode()
	b.crc = crc32.ChecksumIEEE(header)

	// write header
	if _, err := b.write(header); err != nil {
		return err
	}

	// write commit event
	if _, err := b.write(cts); err != nil {
		return err
	}

	// write crc32
	b.crc = crc32.Update(b.crc, crc32.IEEETable, cts)

	ct := make([]byte, CRC32Size)
	binary.LittleEndian.PutUint32(ct, b.crc)
	if _, err := b.write(ct); err != nil {
		return err
	}

	return nil
}

// writeMagic write magic-number, desc event
func (b *BinlogWriter) writeMagic() error {
	b.logPos = 4
	if _, err := b.write(replication.BinLogFileHeader); err != nil {
		return err
	}

	return nil
}

func (b *BinlogWriter) writeDesc(curr uint32) error {
	// update timestamp
	b.Desc.Header.Timestamp = curr
	return b.WriteEvent(b.Desc)
}

// CheckFlush check binlog file whether is full then flush logs for writer
func (b *BinlogWriter) CheckFlush(s int, curr uint32) error {
	if b.logPos+uint32(s) < inter.FileLimitSize {
		return nil
	}

	// have to flush logs
	if err := b.flushLogs(curr); err != nil {
		log.Error(err)
		return err
	}

	return nil
}

// WriteBytes write bytes in case of compress
func (b *BinlogWriter) write(bt []byte) (int, error) {
	if len(bt) == 0 {
		return 0, nil
	}

	if b.compress {
		return b.iw.Write(bt)
	}

	return b.f.Write(bt)
}
