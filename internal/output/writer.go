package output

import (
	"fmt"
	"os"
	"sort"

	"github.com/proxy-collector/internal/config"
	"github.com/proxy-collector/internal/logger"
	"github.com/proxy-collector/internal/models"
	"github.com/proxy-collector/internal/validator"
)

// ProcessAndSave applies sorting, filters to top N, and saves output to root txt
func ProcessAndSave(valid []models.Proxy, stats *validator.PoolStats, duplicates int, elapsed float64) {
	// 1. Sort by Speed (Descending) then Latency (Ascending)
	sort.Slice(valid, func(i, j int) bool {
		if valid[i].Speed == valid[j].Speed {
			return valid[i].Latency < valid[j].Latency
		}
		return valid[i].Speed > valid[j].Speed
	})

	// 2. Limit bounds
	finalCount := len(valid)
	if finalCount > config.MaxOutputCount {
		valid = valid[:config.MaxOutputCount]
		logger.Warn("More than %d valid proxies. Keeping the fastest %d.", config.MaxOutputCount, config.MaxOutputCount)
	} else if finalCount < config.MinOutputCount {
		logger.Warn("Warning: Only found %d valid proxies, which is below the minimum target of %d.", finalCount, config.MinOutputCount)
	}

	// 3. Extract Statistics
	var bestSpeed, avgSpeed float64
	var avgLatency float64

	if len(valid) > 0 {
		bestSpeed = valid[0].Speed
		var totalSpeed float64
		var totalLatencyMs float64
		for _, p := range valid {
			totalSpeed += p.Speed
			totalLatencyMs += float64(p.Latency.Milliseconds())
		}
		avgSpeed = totalSpeed / float64(len(valid))
		avgLatency = totalLatencyMs / float64(len(valid))
	}

	// 4. Save to valid_proxies.txt
	file, err := os.Create("valid_proxies.txt")
	if err != nil {
		logger.Error("Failed to create output file: %v", err)
		return
	}
	defer file.Close()

	for _, p := range valid {
		file.WriteString(fmt.Sprintf("%s\n", p.Address))
	}

	// 5. Print Final Statistics Block
	fmt.Println("\n===========================================")
	fmt.Println("       PROXY COLLECTOR STATISTICS")
	fmt.Println("===========================================")
	fmt.Printf("Total Collected    : %d (Duplicates removed: %d)\n", stats.Tested+int32(duplicates), duplicates)
	fmt.Printf("Total Tested       : %d\n", stats.Tested)
	fmt.Printf("Failed Validation  : %d\n", stats.Failed)
	fmt.Printf("Passed Validation  : %d\n", stats.Passed)
	fmt.Printf("Saved to File      : %d\n", len(valid))
	fmt.Println("-------------------------------------------")
	fmt.Printf("Best Speed         : %.2f KB/s\n", bestSpeed)
	fmt.Printf("Average Speed      : %.2f KB/s\n", avgSpeed)
	fmt.Printf("Average Latency    : %.2f ms\n", avgLatency)
	fmt.Printf("Total Elapsed Time : %.2f seconds\n", elapsed)
	fmt.Println("===========================================")
}
