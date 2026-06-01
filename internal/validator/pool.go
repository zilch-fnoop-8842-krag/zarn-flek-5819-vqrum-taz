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
	// Create a Cancellable context to trigger an early exit
	poolCtx, poolCancel := context.WithCancel(ctx)
	defer poolCancel() // Ensure cleanup

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
				// Stop processing immediately if context gets cancelled (e.g., target reached)
				if poolCtx.Err() != nil {
					return
				}

				atomic.AddInt32(&stats.Tested, 1)

				// Give each test isolation, tied to the parent poolCtx so it instantly aborts on Early Exit
				testCtx, cancelTest := context.WithTimeout(poolCtx, 15*time.Second)
				validProxy, err := ValidateProxy(testCtx, proxyAddr)
				cancelTest()

				if err == nil {
					passedCount := atomic.AddInt32(&stats.Passed, 1)
					// Log KB/s alongside MB/s and indicate the detected protocol type
					logger.Success("Proxy %s [%s] Passed! Speed: %.2f KB/s (%.2f MB/s), Latency: %dms",
						validProxy.Address, validProxy.Protocol, validProxy.Speed, validProxy.Speed/1024.0, validProxy.Latency.Milliseconds())

					results <- validProxy

					// EARLY EXIT TRIGGER: Stop as soon as we hit the max requested amount
					if passedCount >= int32(config.MaxOutputCount) {
						logger.Info("Reached target of %d valid proxies! Stopping workers early...", config.MaxOutputCount)
						poolCancel() // This instantly halts all other running and pending tests
					}
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

	// Feed jobs in a separate goroutine so we can abort feeding if we reach our target early
	go func() {
		for _, p := range proxies {
			if poolCtx.Err() != nil {
				break // Stop sending jobs to the channel if we cancelled early
			}
			jobs <- p
		}
		close(jobs)
	}()

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
