package data

import (
	"strings"
	"testing"
)

const validCSV = "Station/Location;Date;tre200s0;rre150z0;sre000z0;gre000z0;ure200s0;tde200s0;dkl010z0;fu3010z0;fu3010z1;prestas0;pp0qffs0;pp0qnhs0;ppz850s0;ppz700s0;dv1towz0;fu3towz0;fu3towz1;ta1tows0;uretows0;tdetows0\n" +
	" KLO ;202609061230;12.34;0.00;-;100.00;65.00;6.10;180.00;4.30;7.20;950.00;1010.00;1011.00;-;-;-;-;-;-;-;-\n"

func TestParseCSV(t *testing.T) {
	records, err := ParseCSV(append([]byte{0xef, 0xbb, 0xbf}, []byte(validCSV)...))
	if err != nil {
		t.Fatalf("ParseCSV() error = %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("ParseCSV() returned %d records", len(records))
	}
	if records[0].Station != "KLO" {
		t.Errorf("station = %q, want KLO", records[0].Station)
	}
	if !records[0].AirTemperature.Valid || records[0].AirTemperature.Float64 != 12.34 {
		t.Errorf("air temperature = %+v", records[0].AirTemperature)
	}
	if records[0].SunshineDuration.Valid {
		t.Errorf("sunshine duration = %+v, want null", records[0].SunshineDuration)
	}
}

func TestParseCSVRejectsMissingHeader(t *testing.T) {
	input := strings.Replace(validCSV, ";tdetows0", "", 1)
	if _, err := ParseCSV([]byte(input)); err == nil || !strings.Contains(err.Error(), "tdetows0") {
		t.Fatalf("ParseCSV() error = %v, want missing-header error", err)
	}
}

func TestParseCSVRejectsDuplicateHeader(t *testing.T) {
	input := strings.Replace(validCSV, "tdetows0", "tre200s0", 1)
	if _, err := ParseCSV([]byte(input)); err == nil || !strings.Contains(err.Error(), "more than once") {
		t.Fatalf("ParseCSV() error = %v, want duplicate-header error", err)
	}
}

func TestParseCSVRejectsNoMeasurements(t *testing.T) {
	input := validCSV[:strings.IndexByte(validCSV, '\n')+1]
	if _, err := ParseCSV([]byte(input)); err == nil || !strings.Contains(err.Error(), "no measurements") {
		t.Fatalf("ParseCSV() error = %v, want no-measurements error", err)
	}
}

func TestParseCSVRejectsInvalidMeasurement(t *testing.T) {
	input := strings.Replace(validCSV, "12.34", "NaN", 1)
	if _, err := ParseCSV([]byte(input)); err == nil || !strings.Contains(err.Error(), "finite") {
		t.Fatalf("ParseCSV() error = %v, want finite-value error", err)
	}
}

func TestParseCSVRejectsDuplicateMeasurement(t *testing.T) {
	row := validCSV[strings.IndexByte(validCSV, '\n')+1:]
	if _, err := ParseCSV([]byte(validCSV + row)); err == nil || !strings.Contains(err.Error(), "duplicates") {
		t.Fatalf("ParseCSV() error = %v, want duplicate-row error", err)
	}
}

func TestParseCSVAllowsAdditionalColumn(t *testing.T) {
	input := "quality;" + strings.Replace(validCSV, "\n", "\nok;", 1)
	records, err := ParseCSV([]byte(input))
	if err != nil {
		t.Fatalf("ParseCSV() error = %v", err)
	}
	if len(records) != 1 || records[0].Station != "KLO" {
		t.Fatalf("ParseCSV() records = %+v", records)
	}
}
