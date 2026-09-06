package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/cockroachdb/pebble"
	"gosmninfo.rasc.ch/internal/data"
	"gosmninfo.rasc.ch/internal/fileutil"
	"gosmninfo.rasc.ch/internal/store"
)

const (
	defaultSwissMetNetURL = "https://data.geo.admin.ch/ch.meteoschweiz.messwerte-aktuell/VQHA80.csv"
	maxDownloadSize       = 10 << 20
)

type config struct {
	URL       string
	DBPath    string
	CSVPath   string
	Retries   int
	RetryWait time.Duration
}

func main() {
	var cfg config
	var timeout time.Duration
	flag.StringVar(&cfg.URL, "url", defaultSwissMetNetURL, "MeteoSwiss CSV URL")
	flag.StringVar(&cfg.DBPath, "db", "smninfo", "Pebble database path")
	flag.StringVar(&cfg.CSVPath, "csv", "data.csv", "download snapshot path (empty disables it)")
	flag.IntVar(&cfg.Retries, "retries", 3, "retries after a transient download failure")
	flag.DurationVar(&cfg.RetryWait, "retry-wait", time.Second, "delay between download attempts")
	flag.DurationVar(&timeout, "timeout", time.Minute, "HTTP request timeout")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	count, err := run(ctx, &http.Client{Timeout: timeout}, cfg)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("imported %d measurements", count)
}

func run(ctx context.Context, client *http.Client, cfg config) (count int, err error) {
	if cfg.URL == "" {
		return 0, errors.New("URL is empty")
	}
	if cfg.DBPath == "" {
		return 0, errors.New("database path is empty")
	}
	if cfg.Retries < 0 {
		return 0, errors.New("retries cannot be negative")
	}
	if cfg.RetryWait < 0 {
		return 0, errors.New("retry wait cannot be negative")
	}

	body, err := downloadWithRetry(ctx, client, cfg)
	if err != nil {
		return 0, err
	}
	records, err := data.ParseCSV(body)
	if err != nil {
		return 0, fmt.Errorf("parse downloaded data: %w", err)
	}

	db, err := pebble.Open(cfg.DBPath, &pebble.Options{})
	if err != nil {
		return 0, fmt.Errorf("open database: %w", err)
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close database: %w", closeErr))
		}
	}()
	if err := store.Put(db, records); err != nil {
		return 0, err
	}

	if cfg.CSVPath != "" {
		if err := fileutil.WriteAtomically(cfg.CSVPath, 0o644, func(output io.Writer) error {
			_, err := output.Write(body)
			return err
		}); err != nil {
			return 0, fmt.Errorf("save download snapshot: %w", err)
		}
	}
	return len(records), nil
}

func downloadWithRetry(ctx context.Context, client *http.Client, cfg config) ([]byte, error) {
	if client == nil {
		return nil, errors.New("HTTP client is nil")
	}

	var lastErr error
	for attempt := 0; attempt <= cfg.Retries; attempt++ {
		body, retry, err := download(ctx, client, cfg.URL)
		if err == nil {
			return body, nil
		}
		lastErr = err
		if !retry || attempt == cfg.Retries {
			break
		}

		timer := time.NewTimer(cfg.RetryWait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	return nil, fmt.Errorf("download MeteoSwiss data: %w", lastErr)
}

func download(ctx context.Context, client *http.Client, url string) (body []byte, retry bool, err error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, false, fmt.Errorf("create request: %w", err)
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, true, err
	}
	defer func() {
		if closeErr := response.Body.Close(); closeErr != nil {
			if err == nil {
				retry = true
			}
			err = errors.Join(err, fmt.Errorf("close response body: %w", closeErr))
		}
	}()

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		retry := response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= http.StatusInternalServerError
		return nil, retry, fmt.Errorf("unexpected HTTP status %s", response.Status)
	}

	body, err = io.ReadAll(io.LimitReader(response.Body, maxDownloadSize+1))
	if err != nil {
		return nil, true, fmt.Errorf("read response body: %w", err)
	}
	if len(body) > maxDownloadSize {
		return nil, false, fmt.Errorf("response exceeds %d bytes", maxDownloadSize)
	}
	return body, false, nil
}
