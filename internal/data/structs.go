package data

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"strconv"
	"time"
)

const serializedStationDataSize = 180

type DateTime struct {
	EpochSeconds int64
}

func (date *DateTime) UnmarshalCSV(csv string) (err error) {
	ti, err := time.Parse("200601021504", csv)
	if err == nil {
		date.EpochSeconds = ti.Unix()
	}
	return err
}

func (date *DateTime) MarshalCSV() (string, error) {
	return time.Unix(date.EpochSeconds, 0).UTC().Format("2006-01-02T15:04:05.000Z"), nil
}

type NullFloat64 struct {
	Float64 float64
	Valid   bool
}

func (value *NullFloat64) UnmarshalCSV(csv string) (err error) {
	if csv == "" || csv == "-" {
		value.Valid = false
		value.Float64 = 0
		return nil
	}

	v, err := strconv.ParseFloat(csv, 64)
	if err != nil {
		return err
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
	Station                  string      `csv:"Station/Location"`
	DateTime                 DateTime    `csv:"Date"`
	AirTemperature           NullFloat64 `csv:"tre200s0"` // deg C: Air temperature 2 m above ground; current value
	Precipitation            NullFloat64 `csv:"rre150z0"` // mm: Precipitation; current value
	SunshineDuration         NullFloat64 `csv:"sre000z0"` // min: Sunshine duration; ten minutes total
	GlobalRadiation          NullFloat64 `csv:"gre000z0"` // W/m2: Global radiation; ten minutes mean
	RelativeAirHumidity      NullFloat64 `csv:"ure200s0"` // %: Relative air humidity 2 m above ground; current value
	DewPointTemperature      NullFloat64 `csv:"tde200s0"` // deg C: Dew point temperature 2 m above ground; current value
	WindDirection            NullFloat64 `csv:"dkl010z0"` // degrees: wind direction; ten minutes mean
	WindSpeed                NullFloat64 `csv:"fu3010z0"` // km/h: Wind speed; ten minutes mean
	GustPeak                 NullFloat64 `csv:"fu3010z1"` // km/h: Gust peak (one second); maximum
	PressureQFE              NullFloat64 `csv:"prestas0"` // hPa: Pressure at station level (QFE); current value
	PressureQFF              NullFloat64 `csv:"pp0qffs0"` // hPa: Pressure reduced to sea level (QFF); current value
	PressureQNH              NullFloat64 `csv:"pp0qnhs0"` // hPa: Pressure reduced to sea level according to standard atmosphere (QNH); current value
	GeopotentialHeight850    NullFloat64 `csv:"ppz850s0"` // gpm: geopotential height of the 850 hPa-surface; current value
	GeopotentialHeight700    NullFloat64 `csv:"ppz700s0"` // gpm: geopotential height of the 700 hPa-surface; current value
	WindDirectionVectorial   NullFloat64 `csv:"dv1towz0"` // degrees: wind direction vectorial, average of 10 min; instrument 1
	WindSpeedTower           NullFloat64 `csv:"fu3towz0"` // km/h: Wind speed tower; ten minutes mean
	GustPeakTower            NullFloat64 `csv:"fu3towz1"` // km/h: Gust peak (one second) tower; maximum
	AirTemperatureTool       NullFloat64 `csv:"ta1tows0"` // deg C: Air temperature tool 1
	RelativeAirHumidityTower NullFloat64 `csv:"uretows0"` // %: Relative air humidity tower; current value
	DewPointTower            NullFloat64 `csv:"tdetows0"` // deg C: Dew point tower
}

func (d *StationData) Key() []byte {
	key := make([]byte, 0, len(d.Station)+1+20)
	key = append(key, d.Station...)
	key = append(key, '-')
	key = strconv.AppendInt(key, d.DateTime.EpochSeconds, 10)
	return key
}

func (d *StationData) Serialize() ([]byte, error) {
	data := make([]byte, 0, serializedStationDataSize)

	for _, field := range d.floatFields() {
		data = append(data, boolToByte(field.Valid))
		data = append(data, float64ToByte(field.Float64)...)
	}

	return data, nil
}

func (d *StationData) Deserialize(data []byte) error {
	if len(data) != serializedStationDataSize {
		return errors.New("invalid station data payload")
	}

	for i, field := range d.floatFields() {
		offset := i * 9
		field.Valid = byteToBool(data[offset])
		field.Float64 = byteToFloat64(data[offset+1 : offset+9])
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

func byteToBool(b byte) bool {
	return b == 1
}

func float64ToByte(f float64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], math.Float64bits(f))
	return buf[:]
}

func byteToFloat64(b []byte) float64 {
	return math.Float64frombits(binary.BigEndian.Uint64(b))
}
