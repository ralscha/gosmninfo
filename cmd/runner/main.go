package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"os"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-resty/resty/v2"
	"github.com/gocarina/gocsv"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gosmninfo.rasc.ch/internal/data"
)

const SwissMetNetURL = "https://data.geo.admin.ch/ch.meteoschweiz.messwerte-aktuell/VQHA80.csv"

func main() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

	httpClient := resty.New()
	httpClient.SetRetryCount(3).SetRetryWaitTime(30 * time.Second).SetTimeout(1 * time.Minute)

	resp, err := httpClient.R().Get(SwissMetNetURL)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to get data from SwissMetNet")
	} else {
		if resp.IsSuccess() {
			if outputFile, err := os.Create("data.csv"); err == nil {
				defer outputFile.Close()
				body := resp.Body()
				_, err = outputFile.Write(body)
				if err != nil {
					log.Warn().Err(err).Msg("Failed to write data to file")
				}
				err := processData(body)
				if err != nil {
					log.Warn().Err(err).Msg("Failed to process data")
				}
			} else {
				log.Warn().Err(err).Msg("Failed to write response to file")
			}
		}
	}

}

func processData(da []byte) error {
	var stationDatas []*data.StationData
	reader := csv.NewReader(bytes.NewReader(da))
	reader.Comma = ';'
	err := gocsv.UnmarshalCSV(reader, &stationDatas)
	if err != nil {
		return fmt.Errorf("failed to unmarshal csv data: %w", err)
	}

	db, err := pebble.Open("smninfo", &pebble.Options{})
	if err != nil {
		return fmt.Errorf("failed to open pebble db: %w", err)
	}
	defer db.Close()

	batch := db.NewBatch()
	defer batch.Close()

	for _, d := range stationDatas {
		key := d.Key()
		value, err := d.Serialize()
		if err != nil {
			return fmt.Errorf("failed to serialize data: %w", err)
		}
		if err := batch.Set(key, value, pebble.NoSync); err != nil {
			return fmt.Errorf("failed to write to pebble db: %w", err)
		}
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("failed to commit pebble batch: %w", err)
	}

	return nil
}
