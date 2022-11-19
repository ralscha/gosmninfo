package main

import (
	"encoding/csv"
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/go-resty/resty/v2"
	"github.com/gocarina/gocsv"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gosmninfo.rasc.ch/internal/data"
	"io"
	"os"
	"time"
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
				_, err = io.WriteString(outputFile, resp.String())
				if err != nil {
					log.Warn().Err(err).Msg("Failed to write data to file")
				}
				err := processData(resp.String())
				if err != nil {
					log.Warn().Err(err).Msg("Failed to process data")
				}
			} else {
				log.Warn().Err(err).Msg("Failed to write response to file")
			}
		}
	}

}

func processData(da string) error {
	var stationDatas []*data.StationData
	gocsv.SetCSVReader(func(in io.Reader) gocsv.CSVReader {
		r := csv.NewReader(in)
		r.Comma = ';'
		return r
	})
	if err := gocsv.UnmarshalString(da, &stationDatas); err != nil {
		return fmt.Errorf("failed to unmarshal csv data: %w", err)
	}

	db, err := pebble.Open("smninfo", &pebble.Options{})
	if err != nil {
		return fmt.Errorf("failed to open pebble db: %w", err)
	}
	defer db.Close()

	for _, d := range stationDatas {
		key := d.Key()
		value, err := d.Serialize()
		if err != nil {
			return fmt.Errorf("failed to serialize data: %w", err)
		}
		if err := db.Set(key, value, pebble.Sync); err != nil {
			return fmt.Errorf("failed to write to pebble db: %w", err)
		}
	}

	return nil
}
