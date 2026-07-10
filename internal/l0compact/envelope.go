package l0compact

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
)

const v1Magic int32 = 0xfffabc

const (
	eventDescriptor int8 = 0
	eventInsert     int8 = 1
	eventDelete     int8 = 2
)

var endian = binary.LittleEndian

type eventHeader struct {
	Timestamp    uint64
	TypeCode     int8
	EventLength  int32
	NextPosition int32
}

const eventHeaderSize = 17
const descriptorFixPartSize = 52
const dataEventFixPartSize = 16 // {StartTimestamp, EndTimestamp uint64}

type descriptor struct {
	CollectionID, PartitionID, SegmentID, FieldID int64
	PayloadDataType                               int32
	Extras                                        map[string]string
}

type v1Event struct {
	TypeCode int8
	Payload  []byte // the parquet blob
}

func parseV1Envelope(blob []byte) (descriptor, []v1Event, error) {
	r := bytes.NewReader(blob)
	var magic int32
	if err := binary.Read(r, endian, &magic); err != nil {
		return descriptor{}, nil, fmt.Errorf("l0compact: read magic: %w", err)
	}
	if magic != v1Magic {
		return descriptor{}, nil, fmt.Errorf("l0compact: bad magic %x (want %x)", magic, v1Magic)
	}

	// descriptor event: header + fixpart + postHeaderLengths(8) + extraLen + extras
	var dh eventHeader
	if err := binary.Read(r, endian, &dh); err != nil {
		return descriptor{}, nil, fmt.Errorf("l0compact: read desc header: %w", err)
	}
	var d descriptor
	fix := struct {
		CollectionID, PartitionID, SegmentID, FieldID int64
		StartTs, EndTs                                uint64
		PayloadDataType                               int32
	}{}
	if err := binary.Read(r, endian, &fix); err != nil {
		return descriptor{}, nil, fmt.Errorf("l0compact: read desc fixpart: %w", err)
	}
	d.CollectionID, d.PartitionID, d.SegmentID, d.FieldID = fix.CollectionID, fix.PartitionID, fix.SegmentID, fix.FieldID
	d.PayloadDataType = fix.PayloadDataType
	var postHeader [8]uint8
	if err := binary.Read(r, endian, &postHeader); err != nil {
		return descriptor{}, nil, fmt.Errorf("l0compact: read post-header lengths: %w", err)
	}
	var extraLen int32
	if err := binary.Read(r, endian, &extraLen); err != nil {
		return descriptor{}, nil, fmt.Errorf("l0compact: read extra len: %w", err)
	}
	extraBytes := make([]byte, extraLen)
	if _, err := readFull(r, extraBytes); err != nil {
		return descriptor{}, nil, fmt.Errorf("l0compact: read extras: %w", err)
	}
	d.Extras = map[string]string{}
	if extraLen > 0 {
		if err := json.Unmarshal(extraBytes, &d.Extras); err != nil {
			return descriptor{}, nil, fmt.Errorf("l0compact: unmarshal extras: %w", err)
		}
	}

	// remaining data events
	var events []v1Event
	for {
		var h eventHeader
		if err := binary.Read(r, endian, &h); err != nil {
			break // EOF -> done
		}
		payloadLen := int(h.EventLength) - eventHeaderSize - dataEventFixPartSize
		if payloadLen < 0 {
			return descriptor{}, nil, fmt.Errorf("l0compact: negative payload len %d", payloadLen)
		}
		// skip 16-byte data-event fixpart
		if _, err := r.Seek(dataEventFixPartSize, 1); err != nil {
			return descriptor{}, nil, err
		}
		payload := make([]byte, payloadLen)
		if _, err := readFull(r, payload); err != nil {
			return descriptor{}, nil, fmt.Errorf("l0compact: read event payload: %w", err)
		}
		events = append(events, v1Event{TypeCode: h.TypeCode, Payload: payload})
	}
	return d, events, nil
}

// BuildV1Envelope wraps a single parquet payload as a v1 binlog file with one data event.
func BuildV1Envelope(typeCode int8, payloadType int32, fieldID int64, extras map[string]string, payload []byte) ([]byte, error) {
	var buf bytes.Buffer
	_ = binary.Write(&buf, endian, v1Magic)

	extraJSON, _ := json.Marshal(extras)
	descDataLen := descriptorFixPartSize + 8 /*postheader*/ + 4 /*extralen*/ + len(extraJSON)
	descEventLen := int32(eventHeaderSize + descDataLen)

	// descriptor header
	_ = binary.Write(&buf, endian, eventHeader{Timestamp: 0, TypeCode: eventDescriptor, EventLength: descEventLen, NextPosition: 4 + descEventLen})
	// descriptor fixpart
	_ = binary.Write(&buf, endian, struct {
		CollectionID, PartitionID, SegmentID, FieldID int64
		StartTs, EndTs                                uint64
		PayloadDataType                               int32
	}{FieldID: fieldID, PayloadDataType: payloadType})
	_ = binary.Write(&buf, endian, [8]uint8{52, 16, 16, 16, 16, 16, 16, 16})
	_ = binary.Write(&buf, endian, int32(len(extraJSON)))
	buf.Write(extraJSON)

	// data event
	dataEventLen := int32(eventHeaderSize + dataEventFixPartSize + len(payload))
	pos := 4 + descEventLen
	_ = binary.Write(&buf, endian, eventHeader{Timestamp: 0, TypeCode: typeCode, EventLength: dataEventLen, NextPosition: pos + dataEventLen})
	_ = binary.Write(&buf, endian, struct{ StartTs, EndTs uint64 }{})
	buf.Write(payload)
	return buf.Bytes(), nil
}

func readFull(r *bytes.Reader, p []byte) (int, error) {
	n := 0
	for n < len(p) {
		m, err := r.Read(p[n:])
		n += m
		if err != nil {
			return n, err
		}
	}
	return n, nil
}
