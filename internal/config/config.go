package config

import "time"

const (
	// WorkerPool settings
	MaxWorkers = 150

	// Validation Thresholds
	MinSpeedKBps   = 500.0
	MaxOutputCount = 50
	MinOutputCount = 10

	// Timeouts
	ConnectTimeout = 4 * time.Second
	TestTimeout    = 8 * time.Second // Fail-fast: prevents hanging on slow downloads

	// Global execution limit for GitHub actions
	GlobalTimeout = 15 * time.Minute
)

// Fallback targets for tests
var (
	HTTPTargets = []string{
		"http://httpbin.org/get",
		"http://connectivitycheck.gstatic.com/generate_204",
		"http://1.1.1.1",
	}

	HTTPSTargets = []string{
		"https://cloudflare.com/cdn-cgi/trace",
		"https://www.google.com/generate_204",
		"https://1.1.1.1",
	}

	SpeedTargets = []string{
		// 2MB file. To download this within 8 seconds TestTimeout, it must be >250 KB/s minimum.
		// Enforcing our 500 KB/s check in the code guarantees quality.
		"https://speed.cloudflare.com/__down?bytes=2097152",
		"http://ipv4.download.thinkbroadband.com/2MB.zip",
	}
)