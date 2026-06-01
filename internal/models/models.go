package models

import "time"

// Proxy represents a tested and validated proxy
type Proxy struct {
	Address string        // IP:PORT format
	Type    string        // Protocol (http or socks5)
	Country string        // Country Location Code
	Speed   float64       // Throughput in KB/s
	Latency time.Duration // TCP Connection Latency
}
