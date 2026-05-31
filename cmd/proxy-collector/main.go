package main

import (
	"context"
	"time"

	"github.com/proxy-collector/internal/config"
	"github.com/proxy-collector/internal/logger"
	"github.com/proxy-collector/internal/output"
	"github.com/proxy-collector/internal/sources"
	"github.com/proxy-collector/internal/validator"
)

func main() {
	startTimer := time.Now()

	// Create a master context preventing infinite hangs on GitHub Actions
	ctx, cancel := context.WithTimeout(context.Background(), config.GlobalTimeout)
	defer cancel()

	logger.Info("Initializing Proxy Collector...")

	// 1. Fetch & Deduplicate
	dedupedProxies, duplicates := sources.FetchAndDeduplicate(ctx)
	if len(dedupedProxies) == 0 {
		logger.Error("No proxies collected. Exiting.")
		return
	}

	// 2. Validate, Test Speed & Filter
	validProxies, stats := validator.RunWorkerPool(ctx, dedupedProxies)

	// 3. Process Bounds, Save Results, Generate Stats
	elapsed := time.Since(startTimer).Seconds()
	output.ProcessAndSave(validProxies, stats, duplicates, elapsed)
}
