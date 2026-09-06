// Package store contains the Pebble-specific persistence logic.
package store

import (
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"
	"gosmninfo.rasc.ch/internal/data"
)

// Query selects records during database traversal. A zero limit means no limit.
type Query struct {
	Station string
	Limit   int
}

// Put validates and commits all records in a single durable batch.
func Put(db *pebble.DB, records []data.StationData) (err error) {
	if len(records) == 0 {
		return errors.New("no measurements to store")
	}

	batch := db.NewBatch()
	defer func() {
		err = errors.Join(err, batch.Close())
	}()

	for i := range records {
		if err := records[i].Validate(); err != nil {
			return fmt.Errorf("record %d: %w", i+1, err)
		}
		value, err := records[i].Serialize()
		if err != nil {
			return fmt.Errorf("serialize record %d: %w", i+1, err)
		}
		if err := batch.Set(records[i].Key(), value, pebble.NoSync); err != nil {
			return fmt.Errorf("stage record %d: %w", i+1, err)
		}
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("commit measurements: %w", err)
	}
	return nil
}

// Iterate visits matching records in Pebble key order.
func Iterate(db *pebble.DB, query Query, visit func(data.StationData) error) (err error) {
	if query.Limit < 0 {
		return errors.New("limit cannot be negative")
	}
	if visit == nil {
		return errors.New("visit function is nil")
	}

	options := &pebble.IterOptions{}
	if query.Station != "" {
		prefix := append([]byte(query.Station), '-')
		options.LowerBound = prefix
		options.UpperBound = append(append([]byte(nil), prefix...), 0xff)
	}

	iterator, err := db.NewIter(options)
	if err != nil {
		return fmt.Errorf("create database iterator: %w", err)
	}
	defer func() {
		err = errors.Join(err, iterator.Close())
	}()

	count := 0
	for iterator.First(); iterator.Valid(); iterator.Next() {
		key := iterator.Key()
		station, epochSeconds, err := data.ParseKey(key)
		if err != nil {
			return fmt.Errorf("decode key %q: %w", key, err)
		}
		if query.Station != "" && station != query.Station {
			continue
		}

		value, err := iterator.ValueAndErr()
		if err != nil {
			return fmt.Errorf("read value for key %q: %w", key, err)
		}
		record, err := decodeRecord(key, station, epochSeconds, value)
		if err != nil {
			return err
		}
		if err := visit(record); err != nil {
			return fmt.Errorf("visit record %q: %w", key, err)
		}

		count++
		if query.Limit > 0 && count >= query.Limit {
			break
		}
	}
	if err := iterator.Error(); err != nil {
		return fmt.Errorf("iterate database: %w", err)
	}
	return nil
}

func decodeRecord(key []byte, station string, epochSeconds int64, value []byte) (data.StationData, error) {
	var record data.StationData
	if err := record.Deserialize(value); err != nil {
		return data.StationData{}, fmt.Errorf("decode value for key %q: %w", key, err)
	}
	record.Station = station
	record.DateTime.EpochSeconds = epochSeconds
	if err := record.Validate(); err != nil {
		return data.StationData{}, fmt.Errorf("validate record for key %q: %w", key, err)
	}
	return record, nil
}
