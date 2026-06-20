# SwissMetNet Importer

Downloads current SwissMetNet measurements from MeteoSwiss and stores them in a local Pebble database (`smninfo`). The importer also keeps the latest downloaded CSV as `data.csv`.

Source data: https://data.geo.admin.ch/ch.meteoschweiz.messwerte-aktuell/VQHA80.csv

## Commands

- `go run ./cmd/runner`: download the latest measurements and import them into Pebble.
- `go run ./cmd/export`: export all Pebble records to `smninfo.csv`.
- `go run ./cmd/read`: print records from the local Pebble database.
- `go test ./...`: run the test suite.

If you use Task, the same workflows are available as `task run`, `task export`, `task read`, `task tidy`, `task audit`, and `task build`.

## Data Format

Pebble keys are stored as `<station>-<epoch-seconds>`. Values contain the fixed-width binary encoding of the twenty numeric measurement fields from `internal/data.StationData`.
