# SwissMetNet Importer

Downloads current SwissMetNet measurements from MeteoSwiss, validates them, and stores them in a local Pebble database (`smninfo`). The importer also keeps the latest successfully imported CSV as `data.csv`.

Source: MeteoSwiss — https://data.geo.admin.ch/ch.meteoschweiz.messwerte-aktuell/VQHA80.csv

## Quick start

- `go run ./cmd/runner`: download the latest measurements and import them into Pebble.
- `go run ./cmd/read`: print all records as CSV.
- `go run ./cmd/read -station KLO -limit 10`: print up to ten records for one station.
- `go run ./cmd/export`: atomically export all records to `smninfo.csv`.
- `go run ./cmd/export -station KLO -out klo.csv`: export one station.
- `go test ./...`: run the test suite.

All commands accept `-h`. In particular, `-db` selects another database path; the runner also supports `-url`, `-csv`, `-timeout`, `-retries`, and `-retry-wait` for deployments and testing. Set `-csv ""` to disable the downloaded-file snapshot.

If you use Task, the same workflows are available as `task run`, `task export`, `task read`, `task test`, `task tidy`, `task audit`, and `task build`.

## Data Format

Pebble keys are stored as `<station>-<epoch-seconds>`. Values contain the fixed-width binary encoding of the twenty numeric measurement fields from `internal/data.StationData`. Imports are committed as one durable batch; malformed or empty downloads do not modify the database or replace the last good CSV snapshot.
