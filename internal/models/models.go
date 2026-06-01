package models

import "time"

// Proxy represents a tested and validated proxy
type Proxy struct {
	Address  string        // IP:PORT format
	Protocol string        // Working proxy scheme: http, https, socks5
	Speed    float64       // Throughput in KB/s
	Latency  time.Duration // TCP Connection Latency
}
