package main

import (
	"github.com/cockroachdb/pebble"
	"github.com/gocarina/gocsv"
	"gosmninfo.rasc.ch/internal/data"
	"os"
	"strconv"
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
	for it.First(); it.Valid(); it.Next() {
		key := it.Key()
		value, err := it.ValueAndErr()
		if err != nil {
			panic(err)
		}
		sd := data.StationData{}
		sd.Deserialize(value)

		sd.Station = string(key[0:3])
		es := string(key[4:])
		pi, err := strconv.ParseInt(es, 10, 64)
		if err != nil {
			panic(err)
		}
		sd.DateTime.EpochSeconds = pi

		sds = append(sds, &sd)
	}

	err = gocsv.MarshalFile(sds, f)
	if err != nil {
		panic(err)
	}

}
