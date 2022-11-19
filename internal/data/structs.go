package data

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"time"
)

type DateTime struct {
	EpochSeconds int64
}

func (date *DateTime) UnmarshalCSV(csv string) (err error) {
	ti, err := time.Parse("200601021504", csv)
	date.EpochSeconds = ti.Unix()
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
	if csv != "-" {
		v, err := strconv.ParseFloat(csv, 32)
		if err != nil {
			return err
		}
		value.Valid = true
		value.Float64 = v
	} else {
		value.Valid = false
	}
	return nil
}

func (value *NullFloat64) MarshalCSV() (string, error) {
	if value.Valid {
		return strconv.FormatFloat(value.Float64, 'f', 2, 64), nil
	}
	return "", nil
}

type StationData struct {
	Station                  string      `csv:"Station/Location"`
	DateTime                 DateTime    `csv:"Date"`
	AirTemperature           NullFloat64 `csv:"tre200s0"` // °C: Air temperature 2 m above ground; current value
	Precipitation            NullFloat64 `csv:"rre150z0"` // mm: Precipitation; current value
	SunshineDuration         NullFloat64 `csv:"sre000z0"` //min: Sunshine duration; ten minutes total
	GlobalRadiation          NullFloat64 `csv:"gre000z0"` // W/m²: Global radiation; ten minutes mean
	RelativeAirHumidity      NullFloat64 `csv:"ure200s0"` // %: Relative air humidity 2 m above ground; current value
	DewPointTemperature      NullFloat64 `csv:"tde200s0"` // °C: Dew point temperature 2 m above ground; current value
	WindDirection            NullFloat64 `csv:"dkl010z0"` // °: wind direction; ten minutes mean
	WindSpeed                NullFloat64 `csv:"fu3010z0"` // km/h: Wind speed; ten minutes mean
	GustPeak                 NullFloat64 `csv:"fu3010z1"` // km/h: Gust peak (one second); maximum
	PressureQFE              NullFloat64 `csv:"prestas0"` // hPa: Pressure at station level (QFE); current value
	PressureQFF              NullFloat64 `csv:"pp0qffs0"` // hPa: Pressure reduced to sea level (QFF); current value
	PressureQNH              NullFloat64 `csv:"pp0qnhs0"` // hPa: Pressure reduced to sea level according to standard atmosphere (QNH); current value
	GeopotentialHeight850    NullFloat64 `csv:"ppz850s0"` // gpm: geopotential height of the 850 hPa-surface; current value
	GeopotentialHeight700    NullFloat64 `csv:"ppz700s0"` // gpm: geopotential height of the 700 hPa-surface; current value
	WindDirectionVectorial   NullFloat64 `csv:"dv1towz0"` // °: wind direction vectorial, average of 10 min; instrument 1
	WindSpeedTower           NullFloat64 `csv:"fu3towz0"` // km/h: Wind speed tower; ten minutes mean
	GustPeakTower            NullFloat64 `csv:"fu3towz1"` // km/h: Gust peak (one second) tower; maximum
	AirTemperatureTool       NullFloat64 `csv:"ta1tows0"` // °C: Air temperature tool 1
	RelativeAirHumidityTower NullFloat64 `csv:"uretows0"` // %: Relative air humidity tower; current value
	DewPointTower            NullFloat64 `csv:"tdetows0"` // °C: Dew point tower
}

func (d *StationData) Key() []byte {
	return []byte(fmt.Sprintf("%s-%d", d.Station, d.DateTime.EpochSeconds))
}

func (d *StationData) Serialize() ([]byte, error) {
	var data []byte

	data = append(data, boolToByte(d.AirTemperature.Valid))
	data = append(data, float64ToByte(d.AirTemperature.Float64)...)

	data = append(data, boolToByte(d.Precipitation.Valid))
	data = append(data, float64ToByte(d.Precipitation.Float64)...)

	data = append(data, boolToByte(d.SunshineDuration.Valid))
	data = append(data, float64ToByte(d.SunshineDuration.Float64)...)

	data = append(data, boolToByte(d.GlobalRadiation.Valid))
	data = append(data, float64ToByte(d.GlobalRadiation.Float64)...)

	data = append(data, boolToByte(d.RelativeAirHumidity.Valid))
	data = append(data, float64ToByte(d.RelativeAirHumidity.Float64)...)

	data = append(data, boolToByte(d.DewPointTemperature.Valid))
	data = append(data, float64ToByte(d.DewPointTemperature.Float64)...)

	data = append(data, boolToByte(d.WindDirection.Valid))
	data = append(data, float64ToByte(d.WindDirection.Float64)...)

	data = append(data, boolToByte(d.WindSpeed.Valid))
	data = append(data, float64ToByte(d.WindSpeed.Float64)...)

	data = append(data, boolToByte(d.GustPeak.Valid))
	data = append(data, float64ToByte(d.GustPeak.Float64)...)

	data = append(data, boolToByte(d.PressureQFE.Valid))
	data = append(data, float64ToByte(d.PressureQFE.Float64)...)

	data = append(data, boolToByte(d.PressureQFF.Valid))
	data = append(data, float64ToByte(d.PressureQFF.Float64)...)

	data = append(data, boolToByte(d.PressureQNH.Valid))
	data = append(data, float64ToByte(d.PressureQNH.Float64)...)

	data = append(data, boolToByte(d.GeopotentialHeight850.Valid))
	data = append(data, float64ToByte(d.GeopotentialHeight850.Float64)...)

	data = append(data, boolToByte(d.GeopotentialHeight700.Valid))
	data = append(data, float64ToByte(d.GeopotentialHeight700.Float64)...)

	data = append(data, boolToByte(d.WindDirectionVectorial.Valid))
	data = append(data, float64ToByte(d.WindDirectionVectorial.Float64)...)

	data = append(data, boolToByte(d.WindSpeedTower.Valid))
	data = append(data, float64ToByte(d.WindSpeedTower.Float64)...)

	data = append(data, boolToByte(d.GustPeakTower.Valid))
	data = append(data, float64ToByte(d.GustPeakTower.Float64)...)

	data = append(data, boolToByte(d.AirTemperatureTool.Valid))
	data = append(data, float64ToByte(d.AirTemperatureTool.Float64)...)

	data = append(data, boolToByte(d.RelativeAirHumidityTower.Valid))
	data = append(data, float64ToByte(d.RelativeAirHumidityTower.Float64)...)

	data = append(data, boolToByte(d.DewPointTower.Valid))
	data = append(data, float64ToByte(d.DewPointTower.Float64)...)

	return data, nil
}

func (d *StationData) Deserialize(data []byte) {
	d.AirTemperature.Valid = byteToBool(data[0])
	d.AirTemperature.Float64 = byteToFloat64(data[1:9])

	d.Precipitation.Valid = byteToBool(data[9])
	d.Precipitation.Float64 = byteToFloat64(data[10:18])

	d.SunshineDuration.Valid = byteToBool(data[18])
	d.SunshineDuration.Float64 = byteToFloat64(data[19:27])

	d.GlobalRadiation.Valid = byteToBool(data[27])
	d.GlobalRadiation.Float64 = byteToFloat64(data[28:36])

	d.RelativeAirHumidity.Valid = byteToBool(data[36])
	d.RelativeAirHumidity.Float64 = byteToFloat64(data[37:45])

	d.DewPointTemperature.Valid = byteToBool(data[45])
	d.DewPointTemperature.Float64 = byteToFloat64(data[46:54])

	d.WindDirection.Valid = byteToBool(data[54])
	d.WindDirection.Float64 = byteToFloat64(data[55:63])

	d.WindSpeed.Valid = byteToBool(data[63])
	d.WindSpeed.Float64 = byteToFloat64(data[64:72])

	d.GustPeak.Valid = byteToBool(data[72])
	d.GustPeak.Float64 = byteToFloat64(data[73:81])

	d.PressureQFE.Valid = byteToBool(data[81])
	d.PressureQFE.Float64 = byteToFloat64(data[82:90])

	d.PressureQFF.Valid = byteToBool(data[90])
	d.PressureQFF.Float64 = byteToFloat64(data[91:99])

	d.PressureQNH.Valid = byteToBool(data[99])
	d.PressureQNH.Float64 = byteToFloat64(data[100:108])

	d.GeopotentialHeight850.Valid = byteToBool(data[108])
	d.GeopotentialHeight850.Float64 = byteToFloat64(data[109:117])

	d.GeopotentialHeight700.Valid = byteToBool(data[117])
	d.GeopotentialHeight700.Float64 = byteToFloat64(data[118:126])

	d.WindDirectionVectorial.Valid = byteToBool(data[126])
	d.WindDirectionVectorial.Float64 = byteToFloat64(data[127:135])

	d.WindSpeedTower.Valid = byteToBool(data[135])
	d.WindSpeedTower.Float64 = byteToFloat64(data[136:144])

	d.GustPeakTower.Valid = byteToBool(data[144])
	d.GustPeakTower.Float64 = byteToFloat64(data[145:153])

	d.AirTemperatureTool.Valid = byteToBool(data[153])
	d.AirTemperatureTool.Float64 = byteToFloat64(data[154:162])

	d.RelativeAirHumidityTower.Valid = byteToBool(data[162])
	d.RelativeAirHumidityTower.Float64 = byteToFloat64(data[163:171])

	d.DewPointTower.Valid = byteToBool(data[171])
	d.DewPointTower.Float64 = byteToFloat64(data[172:180])
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
