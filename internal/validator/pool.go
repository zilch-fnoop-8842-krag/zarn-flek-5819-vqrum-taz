package validator

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/proxy-collector/internal/config"
	"github.com/proxy-collector/internal/logger"
	"github.com/proxy-collector/internal/models"
)

// PoolStats tracks validation metrics safely
type PoolStats struct {
	Tested int32
	Passed int32
	Failed int32
}

// RunWorkerPool consumes the deduplicated proxies, tests them concurrently, and gathers valid proxies
func RunWorkerPool(ctx context.Context, proxies []string) ([]models.Proxy, *PoolStats) {
	jobs := make(chan string, len(proxies))
	results := make(chan models.Proxy, len(proxies))
	var wg sync.WaitGroup

	stats := &PoolStats{}
	logger.Info("Starting validation pool with %d concurrent workers...", config.MaxWorkers)

	// Spin up workers
	for i := 0; i < config.MaxWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for proxyAddr := range jobs {
				// Stop processing if context gets cancelled
				if ctx.Err() != nil {
					return
				}

				atomic.AddInt32(&stats.Tested, 1)

				// Give each test isolation with a quick timeout block
				testCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
				validProxy, err := ValidateProxy(testCtx, proxyAddr)
				cancel()

				if err == nil {
					atomic.AddInt32(&stats.Passed, 1)
					logger.Success("Proxy %s Passed! Speed: %.2f KB/s, Latency: %dms",
						validProxy.Address, validProxy.Speed, validProxy.Latency.Milliseconds())
					results <- validProxy
				} else {
					atomic.AddInt32(&stats.Failed, 1)
				}

				// Periodic progress update
				tested := atomic.LoadInt32(&stats.Tested)
				if tested%5000 == 0 {
					logger.Info("Progress: Tested %d / %d proxies. Valid so far: %d", tested, len(proxies), atomic.LoadInt32(&stats.Passed))
				}
			}
		}()
	}

	// Feed jobs
	for _, p := range proxies {
		jobs <- p
	}
	close(jobs)

	// Wait for workers in background and close results
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect output
	var validProxies []models.Proxy
	for rp := range results {
		validProxies = append(validProxies, rp)
	}

	return validProxies, stats
}
