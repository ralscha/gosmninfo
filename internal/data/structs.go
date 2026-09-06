package data

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

const (
	measurementCount          = 20
	serializedStationDataSize = measurementCount * 9
	sourceDateLayout          = "200601021504"
)

var csvHeader = []string{
	"Station/Location", "Date", "tre200s0", "rre150z0", "sre000z0", "gre000z0",
	"ure200s0", "tde200s0", "dkl010z0", "fu3010z0", "fu3010z1", "prestas0",
	"pp0qffs0", "pp0qnhs0", "ppz850s0", "ppz700s0", "dv1towz0", "fu3towz0",
	"fu3towz1", "ta1tows0", "uretows0", "tdetows0",
}

type DateTime struct {
	EpochSeconds int64
}

func (date *DateTime) UnmarshalCSV(csv string) (err error) {
	csv = strings.TrimSpace(csv)
	for _, layout := range []string{sourceDateLayout, time.RFC3339Nano} {
		ti, parseErr := time.Parse(layout, csv)
		if parseErr == nil {
			date.EpochSeconds = ti.Unix()
			return nil
		}
	}
	return fmt.Errorf("invalid date %q (expected %s or RFC3339)", csv, sourceDateLayout)
}

func (date *DateTime) MarshalCSV() (string, error) {
	return time.Unix(date.EpochSeconds, 0).UTC().Format("2006-01-02T15:04:05.000Z"), nil
}

type NullFloat64 struct {
	Float64 float64
	Valid   bool
}

func (value *NullFloat64) UnmarshalCSV(csv string) (err error) {
	csv = strings.TrimSpace(csv)
	if csv == "" || csv == "-" {
		value.Valid = false
		value.Float64 = 0
		return nil
	}

	v, err := strconv.ParseFloat(csv, 64)
	if err != nil {
		return err
	}
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return fmt.Errorf("measurement must be finite: %q", csv)
	}
	value.Valid = true
	value.Float64 = v
	return nil
}

func (value *NullFloat64) MarshalCSV() (string, error) {
	if value.Valid {
		return strconv.FormatFloat(value.Float64, 'f', 2, 64), nil
	}
	return "-", nil
}

type StationData struct {
	Station                  string
	DateTime                 DateTime
	AirTemperature           NullFloat64 // deg C: Air temperature 2 m above ground; current value
	Precipitation            NullFloat64 // mm: Precipitation; current value
	SunshineDuration         NullFloat64 // min: Sunshine duration; ten minutes total
	GlobalRadiation          NullFloat64 // W/m2: Global radiation; ten minutes mean
	RelativeAirHumidity      NullFloat64 // %: Relative air humidity 2 m above ground; current value
	DewPointTemperature      NullFloat64 // deg C: Dew point temperature 2 m above ground; current value
	WindDirection            NullFloat64 // degrees: wind direction; ten minutes mean
	WindSpeed                NullFloat64 // km/h: Wind speed; ten minutes mean
	GustPeak                 NullFloat64 // km/h: Gust peak (one second); maximum
	PressureQFE              NullFloat64 // hPa: Pressure at station level (QFE); current value
	PressureQFF              NullFloat64 // hPa: Pressure reduced to sea level (QFF); current value
	PressureQNH              NullFloat64 // hPa: Pressure reduced to sea level according to standard atmosphere (QNH); current value
	GeopotentialHeight850    NullFloat64 // gpm: geopotential height of the 850 hPa-surface; current value
	GeopotentialHeight700    NullFloat64 // gpm: geopotential height of the 700 hPa-surface; current value
	WindDirectionVectorial   NullFloat64 // degrees: wind direction vectorial, average of 10 min; instrument 1
	WindSpeedTower           NullFloat64 // km/h: Wind speed tower; ten minutes mean
	GustPeakTower            NullFloat64 // km/h: Gust peak (one second) tower; maximum
	AirTemperatureTool       NullFloat64 // deg C: Air temperature tool 1
	RelativeAirHumidityTower NullFloat64 // %: Relative air humidity tower; current value
	DewPointTower            NullFloat64 // deg C: Dew point tower
}

// CSVHeader returns the column names used by the MeteoSwiss feed and exports.
func CSVHeader() []string {
	return append([]string(nil), csvHeader...)
}

// CSVRecord returns a complete, human-readable representation of a measurement.
func (d *StationData) CSVRecord() ([]string, error) {
	date, err := d.DateTime.MarshalCSV()
	if err != nil {
		return nil, err
	}

	record := make([]string, 0, len(csvHeader))
	record = append(record, d.Station, date)
	for _, field := range d.floatFields() {
		value, err := field.MarshalCSV()
		if err != nil {
			return nil, err
		}
		record = append(record, value)
	}
	return record, nil
}

// Validate checks the fields required to safely identify and store a record.
func (d *StationData) Validate() error {
	if strings.TrimSpace(d.Station) == "" {
		return errors.New("station is empty")
	}
	if d.DateTime.EpochSeconds == 0 {
		return errors.New("date is missing")
	}
	return d.validateMeasurements()
}

func (d *StationData) validateMeasurements() error {
	for i, field := range d.floatFields() {
		if field.Valid && (math.IsNaN(field.Float64) || math.IsInf(field.Float64, 0)) {
			return fmt.Errorf("measurement %q must be finite", csvHeader[i+2])
		}
	}
	return nil
}

func (d *StationData) Key() []byte {
	key := make([]byte, 0, len(d.Station)+1+20)
	key = append(key, d.Station...)
	key = append(key, '-')
	key = strconv.AppendInt(key, d.DateTime.EpochSeconds, 10)
	return key
}

func (d *StationData) Serialize() ([]byte, error) {
	if err := d.validateMeasurements(); err != nil {
		return nil, err
	}
	data := make([]byte, 0, serializedStationDataSize)

	for _, field := range d.floatFields() {
		data = append(data, boolToByte(field.Valid))
		data = binary.BigEndian.AppendUint64(data, math.Float64bits(field.Float64))
	}

	return data, nil
}

func (d *StationData) Deserialize(data []byte) error {
	if len(data) != serializedStationDataSize {
		return fmt.Errorf("invalid station data payload size: got %d, want %d", len(data), serializedStationDataSize)
	}

	var decoded StationData
	decodedFields := decoded.floatFields()
	for i, field := range decodedFields {
		offset := i * 9
		if data[offset] > 1 {
			return fmt.Errorf("invalid validity marker %d for measurement %q", data[offset], csvHeader[i+2])
		}
		field.Valid = data[offset] == 1
		field.Float64 = math.Float64frombits(binary.BigEndian.Uint64(data[offset+1 : offset+9]))
	}
	if err := decoded.validateMeasurements(); err != nil {
		return fmt.Errorf("invalid station data payload: %w", err)
	}
	for i, field := range d.floatFields() {
		*field = *decodedFields[i]
	}

	return nil
}

func (d *StationData) floatFields() []*NullFloat64 {
	return []*NullFloat64{
		&d.AirTemperature,
		&d.Precipitation,
		&d.SunshineDuration,
		&d.GlobalRadiation,
		&d.RelativeAirHumidity,
		&d.DewPointTemperature,
		&d.WindDirection,
		&d.WindSpeed,
		&d.GustPeak,
		&d.PressureQFE,
		&d.PressureQFF,
		&d.PressureQNH,
		&d.GeopotentialHeight850,
		&d.GeopotentialHeight700,
		&d.WindDirectionVectorial,
		&d.WindSpeedTower,
		&d.GustPeakTower,
		&d.AirTemperatureTool,
		&d.RelativeAirHumidityTower,
		&d.DewPointTower,
	}
}

func ParseKey(key []byte) (string, int64, error) {
	idx := bytes.LastIndexByte(key, '-')
	if idx <= 0 || idx >= len(key)-1 {
		return "", 0, errors.New("invalid key format")
	}

	epochSeconds, err := strconv.ParseInt(string(key[idx+1:]), 10, 64)
	if err != nil {
		return "", 0, err
	}

	return string(key[:idx]), epochSeconds, nil
}

func boolToByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}
