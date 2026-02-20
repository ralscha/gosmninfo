package data

import "testing"

func TestStationDataSerializeDeserializeRoundTrip(t *testing.T) {
	src := StationData{
		AirTemperature:           NullFloat64{Float64: 12.34, Valid: true},
		Precipitation:            NullFloat64{Float64: 0.12, Valid: true},
		SunshineDuration:         NullFloat64{Float64: 5, Valid: true},
		GlobalRadiation:          NullFloat64{Float64: 123.45, Valid: true},
		RelativeAirHumidity:      NullFloat64{Float64: 56.78, Valid: true},
		DewPointTemperature:      NullFloat64{Float64: 1.23, Valid: true},
		WindDirection:            NullFloat64{Float64: 270, Valid: true},
		WindSpeed:                NullFloat64{Float64: 18.9, Valid: true},
		GustPeak:                 NullFloat64{Float64: 31.2, Valid: true},
		PressureQFE:              NullFloat64{Float64: 990.1, Valid: true},
		PressureQFF:              NullFloat64{Float64: 1015.2, Valid: true},
		PressureQNH:              NullFloat64{Float64: 1014.8, Valid: true},
		GeopotentialHeight850:    NullFloat64{Float64: 1460.5, Valid: true},
		GeopotentialHeight700:    NullFloat64{Float64: 3021.7, Valid: true},
		WindDirectionVectorial:   NullFloat64{Float64: 255, Valid: true},
		WindSpeedTower:           NullFloat64{Float64: 22.4, Valid: true},
		GustPeakTower:            NullFloat64{Float64: 35.6, Valid: true},
		AirTemperatureTool:       NullFloat64{Float64: 11.11, Valid: true},
		RelativeAirHumidityTower: NullFloat64{Float64: 44.44, Valid: true},
		DewPointTower:            NullFloat64{Float64: -1.5, Valid: true},
	}

	buf, err := src.Serialize()
	if err != nil {
		t.Fatalf("Serialize() error = %v", err)
	}
	if len(buf) != serializedStationDataSize {
		t.Fatalf("Serialize() len = %d, want %d", len(buf), serializedStationDataSize)
	}

	var dst StationData
	if err := dst.Deserialize(buf); err != nil {
		t.Fatalf("Deserialize() error = %v", err)
	}

	if dst != src {
		t.Fatalf("round-trip mismatch\n got: %+v\nwant: %+v", dst, src)
	}
}

func TestDeserializeRejectsShortPayload(t *testing.T) {
	var sd StationData
	if err := sd.Deserialize(make([]byte, serializedStationDataSize-1)); err == nil {
		t.Fatalf("Deserialize() expected error for short payload")
	}
}

func TestParseKey(t *testing.T) {
	station := "AB-C"
	epochSeconds := int64(1700000000)

	sd := StationData{Station: station}
	sd.DateTime.EpochSeconds = epochSeconds

	parsedStation, parsedEpoch, err := ParseKey(sd.Key())
	if err != nil {
		t.Fatalf("ParseKey() error = %v", err)
	}
	if parsedStation != station {
		t.Fatalf("station = %q, want %q", parsedStation, station)
	}
	if parsedEpoch != epochSeconds {
		t.Fatalf("epoch = %d, want %d", parsedEpoch, epochSeconds)
	}
}

func TestDateTimeUnmarshalCSVDoesNotOverwriteOnError(t *testing.T) {
	date := DateTime{EpochSeconds: 12345}
	if err := date.UnmarshalCSV("invalid"); err == nil {
		t.Fatalf("UnmarshalCSV() expected parse error")
	}
	if date.EpochSeconds != 12345 {
		t.Fatalf("EpochSeconds overwritten on error: got %d, want %d", date.EpochSeconds, 12345)
	}
}
