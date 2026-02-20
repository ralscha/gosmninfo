package main

import (
	"fmt"

	"github.com/cockroachdb/pebble"
	"gosmninfo.rasc.ch/internal/data"
)

func main() {
	db, err := pebble.Open("smninfo", &pebble.Options{})
	if err != nil {
		panic(err)
	}
	defer db.Close()

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

		fmt.Println(sd.Station)
		fmt.Printf("%s: %s\n", key, value)
	}

	if err := it.Error(); err != nil {
		panic(err)
	}

}
