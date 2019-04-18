package replication

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
)

type RowsEventHeader struct {
	// row header data: total data size
	Header []byte

	// flag position
	FlagsPos int
}

type EncodeEvent interface {
	Event
	Encode() []byte
}

type CopyEvent interface {
	EncodeEvent
	Copy() Event
}

func (h *RowsEventHeader) Copy() *RowsEventHeader {
	dst := make([]byte, len(h.Header))
	copy(dst, h.Header)
	return &RowsEventHeader{
		Header:   dst,
		FlagsPos: h.FlagsPos,
	}
}

func (h *EventHeader) Copy() *EventHeader {
	return &EventHeader{
		Timestamp: h.Timestamp,
		EventType: h.EventType,
		ServerID:  h.ServerID,
		EventSize: h.EventSize,
		LogPos:    h.LogPos,
		Flags:     h.Flags,
	}
}

func (e *QueryEvent) Encode() []byte {
	buf := bytes.NewBuffer(nil)
	bts := make([]byte, 4)

	// 4 byte slave proxy id
	binary.LittleEndian.PutUint32(bts, e.SlaveProxyID)
	buf.Write(bts)

	// 4 byte execute time
	binary.LittleEndian.PutUint32(bts, e.ExecutionTime)
	buf.Write(bts)

	// 1 byte schema length
	buf.Write([]byte{byte(len(e.Schema))})

	// 2 byte error code
	binary.LittleEndian.PutUint16(bts, e.ErrorCode)
	buf.Write(bts[:2])

	// 2 bytes status var length
	binary.LittleEndian.PutUint16(bts, uint16(len(e.StatusVars)))
	buf.Write(bts[:2])

	// status vars
	buf.Write(e.StatusVars)

	// write schema
	buf.Write(e.Schema)

	//skip 0x00
	buf.Write([]byte{byte(0x00)})

	// write query
	buf.Write(e.Query)

	return buf.Bytes()
}

func (e *XIDEvent) Encode() []byte {
	buf := bytes.NewBuffer(nil)

	// 8 byte xid
	bts := make([]byte, 8)
	binary.LittleEndian.PutUint64(bts, e.XID)

	buf.Write(bts)

	return buf.Bytes()
}

func (e *RotateEvent) Encode() []byte {
	buf := bytes.NewBuffer(nil)

	// 8 byte position
	bts := make([]byte, 8)
	binary.LittleEndian.PutUint64(bts, e.Position)
	buf.Write(bts)

	// left bytes next log name
	buf.Write(e.NextLogName)

	return buf.Bytes()
}

func (e *FormatDescriptionEvent) Encode() []byte {
	buf := bytes.NewBuffer(nil)

	// 2 byte version
	bts := make([]byte, 4)
	binary.LittleEndian.PutUint16(bts, e.Version)
	buf.Write(bts[:2])

	// 50 bytes server version
	buf.Write(e.ServerVersion)

	// 4 byte timestamp
	binary.LittleEndian.PutUint32(bts, e.CreateTimestamp)
	buf.Write(bts)

	// 1 byte event header length 
	buf.Write([]byte{byte(e.EventHeaderLength)})

	// write event header length
	buf.Write(e.EventTypeHeaderLengths)

	// 1 byte checksum
	buf.Write([]byte{BINLOG_CHECKSUM_ALG_CRC32})

	return buf.Bytes()
}

func (e *FormatDescriptionEvent) Copy() Event {
	buf := bytes.NewBuffer(nil)

	bts := e.Encode()
	buf.Write(bts)
	crbt := make([]byte, 4)
	binary.LittleEndian.PutUint32(crbt, crc32.ChecksumIEEE(bts))
	buf.Write(crbt)

	fd := &FormatDescriptionEvent{}

	if err := fd.Decode(buf.Bytes()); err != nil {
		return nil
	}
	return fd
}

func (e *QueryEvent) Copy() Event {
	buf := bytes.NewBuffer(nil)

	bts := e.Encode()
	buf.Write(bts)
	qe := &QueryEvent{}

	if err := qe.Decode(buf.Bytes()); err != nil {
		return nil
	}
	return qe
}
