package main

import (
	"fmt"
	"github.com/cockroachdb/pebble"
	"gosmninfo.rasc.ch/internal/data"
	"strconv"
)

func main() {
	db, err := pebble.Open("smninfo", &pebble.Options{})
	if err != nil {
		panic(err)
	}
	defer db.Close()

	it := db.NewIter(&pebble.IterOptions{LowerBound: []byte("AAA-0"), UpperBound: []byte("ZZZ-999999999999")})
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

		fmt.Println(sd.Station)
		fmt.Printf("%s: %s\n", key, value)
	}

}
