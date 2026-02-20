package main

import (
	"os"

	"github.com/cockroachdb/pebble"
	"github.com/gocarina/gocsv"
	"gosmninfo.rasc.ch/internal/data"
)

func main() {
	// open database and export everything in the form of a CSV file
	db, err := pebble.Open("smninfo", &pebble.Options{})
	if err != nil {
		panic(err)
	}
	defer db.Close()

	csvFile := "smninfo.csv"
	f, err := os.Create(csvFile)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	sds := make([]*data.StationData, 0)

	it, err := db.NewIter(&pebble.IterOptions{LowerBound: []byte("AAA-0"), UpperBound: []byte("ZZZ-999999999999")})
	if err != nil {
		panic(err)
	}
	defer it.Close()

	for it.First(); it.Valid(); it.Next() {
		key := it.Key()
		value, err := it.ValueAndErr()
		if err != nil {
			panic(err)
		}
		sd := data.StationData{}
		if err := sd.Deserialize(value); err != nil {
			panic(err)
		}

		station, epochSeconds, err := data.ParseKey(key)
		if err != nil {
			panic(err)
		}
		sd.Station = station
		sd.DateTime.EpochSeconds = epochSeconds

		sds = append(sds, &sd)
	}

	if err := it.Error(); err != nil {
		panic(err)
	}

	err = gocsv.MarshalFile(sds, f)
	if err != nil {
		panic(err)
	}

}
