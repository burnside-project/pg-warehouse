package postgres

import (
	"fmt"
	"time"

	"github.com/burnside-project/pg-warehouse/internal/ports"
	"github.com/jackc/pglogrepl"
)

// relationInfo caches table OID to column mappings from RelationMessages.
type relationInfo struct {
	Schema    string
	Name      string
	Columns   []columnInfo
}

type columnInfo struct {
	Name     string
	DataType uint32
	TypeName string
}

// decoder handles pgoutput protocol message decoding.
type decoder struct {
	relations map[uint32]*relationInfo // OID -> relation info
}

func newDecoder() *decoder {
	return &decoder{
		relations: make(map[uint32]*relationInfo),
	}
}

// handleRelation processes a RelationMessage and caches the column mapping.
func (d *decoder) handleRelation(msg *pglogrepl.RelationMessageV2) {
	cols := make([]columnInfo, len(msg.Columns))
	for i, col := range msg.Columns {
		cols[i] = columnInfo{
			Name:     col.Name,
			DataType: col.DataType,
		}
	}
	d.relations[msg.RelationID] = &relationInfo{
		Schema:  msg.Namespace,
		Name:    msg.RelationName,
		Columns: cols,
	}
}

// decodeInsert converts an InsertMessageV2 to a CDCEvent.
func (d *decoder) decodeInsert(msg *pglogrepl.InsertMessageV2, lsn string, ts time.Time) (*ports.CDCEvent, error) {
	rel, ok := d.relations[msg.RelationID]
	if !ok {
		return nil, fmt.Errorf("unknown relation ID %d", msg.RelationID)
	}

	newTuple, err := d.decodeTuple(msg.Tuple, rel)
	if err != nil {
		return nil, fmt.Errorf("failed to decode insert tuple: %w", err)
	}

	return &ports.CDCEvent{
		Table:     rel.Schema + "." + rel.Name,
		Operation: "INSERT",
		LSN:       lsn,
		NewTuple:  newTuple,
		Timestamp: ts,
	}, nil
}

// decodeUpdate converts an UpdateMessageV2 to a CDCEvent.
func (d *decoder) decodeUpdate(msg *pglogrepl.UpdateMessageV2, lsn string, ts time.Time) (*ports.CDCEvent, error) {
	rel, ok := d.relations[msg.RelationID]
	if !ok {
		return nil, fmt.Errorf("unknown relation ID %d", msg.RelationID)
	}

	var oldTuple map[string]any
	if msg.OldTuple != nil {
		var err error
		oldTuple, err = d.decodeTuple(msg.OldTuple, rel)
		if err != nil {
			return nil, fmt.Errorf("failed to decode old tuple: %w", err)
		}
	}

	newTuple, err := d.decodeTuple(msg.NewTuple, rel)
	if err != nil {
		return nil, fmt.Errorf("failed to decode new tuple: %w", err)
	}

	return &ports.CDCEvent{
		Table:     rel.Schema + "." + rel.Name,
		Operation: "UPDATE",
		LSN:       lsn,
		OldTuple:  oldTuple,
		NewTuple:  newTuple,
		Timestamp: ts,
	}, nil
}

// decodeDelete converts a DeleteMessageV2 to a CDCEvent.
func (d *decoder) decodeDelete(msg *pglogrepl.DeleteMessageV2, lsn string, ts time.Time) (*ports.CDCEvent, error) {
	rel, ok := d.relations[msg.RelationID]
	if !ok {
		return nil, fmt.Errorf("unknown relation ID %d", msg.RelationID)
	}

	var oldTuple map[string]any
	if msg.OldTuple != nil {
		var err error
		oldTuple, err = d.decodeTuple(msg.OldTuple, rel)
		if err != nil {
			return nil, fmt.Errorf("failed to decode delete tuple: %w", err)
		}
	}

	return &ports.CDCEvent{
		Table:     rel.Schema + "." + rel.Name,
		Operation: "DELETE",
		LSN:       lsn,
		OldTuple:  oldTuple,
		Timestamp: ts,
	}, nil
}

// decodeTuple converts a TupleData into a map using the relation's column info.
func (d *decoder) decodeTuple(tuple *pglogrepl.TupleData, rel *relationInfo) (map[string]any, error) {
	if tuple == nil {
		return nil, nil
	}

	row := make(map[string]any, len(tuple.Columns))
	for i, col := range tuple.Columns {
		if i >= len(rel.Columns) {
			break
		}
		colName := rel.Columns[i].Name

		switch col.DataType {
		case 'n': // null
			row[colName] = nil
		case 'u': // unchanged toast
			// Skip unchanged TOAST values
		case 't': // text
			row[colName] = string(col.Data)
		default:
			row[colName] = string(col.Data)
		}
	}
	return row, nil
}
