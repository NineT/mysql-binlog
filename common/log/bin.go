package log

import (
	"encoding/binary"
	"hash/crc32"
	"io"

	"github.com/mysql-binlog/siddontang/go-mysql/replication"
	"github.com/zssky/log"
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

// Binlog 每个生成的binlog文件都对应一个结构
type BinlogWriter struct {
	FormatDesc    *replication.FormatDescriptionEvent // FormatDesc 事件
	EventHeader   *replication.EventHeader            // binlog 公共事件头
	QueryEvent    *replication.QueryEvent             // query event 事件 包含commit/ begin ddl etc
	TableMapEvent []byte                              // table map event 表map 事件 不包含event header, 不包含crc32
	RowsHeader    *replication.RowsEventHeader        // InsertHeader 写入事件 body头字节
	W             io.Writer                           // write write 生成binlog
	crc           uint32                              // crc 32 值
	logPos        uint32                              // logPos 文件位置
	XID           uint64                              // XID 事务ID
	NextPrefix    []byte                              // Next 下一个文件起始时间前缀 day_1549847236_{1549961464}.base64 前缀为： day_1549847236
}

// Clear 清除悬挂引用
func (b *BinlogWriter) Clear() {
	b.FormatDesc = nil
	b.EventHeader = nil
	b.QueryEvent = nil
	b.TableMapEvent = nil
	b.RowsHeader = nil
	b.NextPrefix = nil
	b.W = nil
	b.crc = 0
	b.logPos = 0
	b.XID = 0
}

// WriteEventHeader binlog公共事件头
func (b *BinlogWriter) WriteEventHeader(size int, t replication.EventType) error {
	// 整个数据包长度 包括 header + row + crc32 {可能需要 + 1 flag}
	b.EventHeader.EventSize = uint32(size) + CRC32Size
	b.EventHeader.LogPos = b.logPos
	b.EventHeader.EventType = t

	// 计算crc32
	h := b.EventHeader.Encode()
	b.crc = crc32.ChecksumIEEE(h)

	// 数据写入 writer
	if _, err := b.W.Write(h); err != nil {
		return err
	}

	// 重新计算位置
	b.logPos += uint32(size)
	return nil
}

// WriteEventFooter binlog 事件 footer
func (b *BinlogWriter) WriteEventFooter() error {
	log.Debug("WriteEventFooter crc32 ", b.crc)
	ct := make([]byte, CRC32Size)
	binary.LittleEndian.PutUint32(ct, b.crc)
	if _, err := b.W.Write(ct); err != nil {
		return err
	}
	return nil
}

// WriteRowsHeader 写入binlog 行数据头
func (b *BinlogWriter) WriteRowsHeader(flag int, t replication.EventType) error {
	h := b.RowsHeader

	nh := make([]byte, len(h.Header))
	copy(nh, h.Header)

	// bd.DeleteRowsHeader.FlagsPos:
	binary.LittleEndian.PutUint16(nh[h.FlagsPos:], uint16(flag))

	return b.WriteRowsEvent(nh)
}

// WriteRowsEvent 写binlog 事件 仅仅写入数据 不包括event header
func (b *BinlogWriter) WriteRowsEvent(data []byte) error {
	// 写入数据
	if _, err := b.W.Write(data); err != nil {
		return err
	}

	// 计算crc32 值
	// 是否开启 checksum
	b.crc = crc32.Update(b.crc, crc32.IEEETable, data)
	return nil
}

// WriteBinlogFileHeader 开始写入binlog文件header 魔数
func (b *BinlogWriter) WriteBinlogFileHeader() error {
	b.logPos = 4
	if _, err := b.W.Write(replication.BinLogFileHeader); err != nil {
		return err
	}

	return nil
}

func (b *BinlogWriter) WriteDDL(ddl []byte) error {
	b.QueryEvent.Query = ddl

	return b.write(b.QueryEvent, replication.QUERY_EVENT)
}

// WriteQueryBegin write query being event
func (b *BinlogWriter) WriteQueryBegin() error {
	b.QueryEvent.Query = []byte("BEGIN")

	return b.write(b.QueryEvent, replication.QUERY_EVENT)
}

// WriteQueryCommit write query commit event
func (b *BinlogWriter) WriteQueryCommit() error {
	b.XID += 1 // 增加事务号

	b.QueryEvent.Query = []byte("COMMIT")

	return b.write(b.QueryEvent, replication.QUERY_EVENT)
}

// WriteXIDEvent write innodb xid event
func (b *BinlogWriter) WriteXIDEvent() error {
	b.XID += 1 // 增加事务号

	e := &replication.XIDEvent{
		XID: b.XID,
	}

	return b.write(e, replication.XID_EVENT)
}

// WriteRotateEvent write rotate event
func (b *BinlogWriter) WriteRotateEvent() error {
	e := &replication.RotateEvent{
		Position:    uint64(b.logPos),
		NextLogName: b.NextPrefix,
	}

	return b.write(e, replication.ROTATE_EVENT)
}

// WriteTableMapEvent write table map event 写入table map event
func (b *BinlogWriter) WriteTableMapEvent() error {
	// 计算长度
	size := len(b.TableMapEvent) + replication.EventHeaderSize
	if err := b.WriteEventHeader(size, replication.TABLE_MAP_EVENT); err != nil {
		return err
	}

	// 写入数据
	if _, err := b.W.Write(b.TableMapEvent); err != nil {
		return err
	}

	// 写入 footer
	return b.WriteEventFooter()
}

// Write DescEvent write format desc event
func (b *BinlogWriter) WriteDescEvent() error {
	return b.write(b.FormatDesc, replication.FORMAT_DESCRIPTION_EVENT)
}

// write common use write method {header, body, footer}
func (b *BinlogWriter) write(e replication.EncodeEvent, t replication.EventType) error {
	// 數據內容
	bt := e.Encode()

	// write event header
	size := len(bt) + replication.EventHeaderSize
	b.WriteEventHeader(size, t)

	// write data
	if _, err := b.W.Write(bt); err != nil {
		return err
	}

	// calculate crc32
	b.crc = crc32.Update(b.crc, crc32.IEEETable, bt)

	// write event footer
	return b.WriteEventFooter()
}
