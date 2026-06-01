package validator

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/proxy-collector/internal/config"
	"github.com/proxy-collector/internal/models"
)

// ValidateProxy runs the 5-stage validation process for a single proxy
func ValidateProxy(ctx context.Context, proxyAddr string) (models.Proxy, error) {
	proxyInfo := models.Proxy{Address: proxyAddr}

	// Stage 1: TCP Connectivity & Latency Check
	latency, err := testTCP(proxyAddr)
	if err != nil {
		return proxyInfo, errors.New("TCP test failed")
	}
	proxyInfo.Latency = latency

	// Stage 2: Protocol Detection (Concurrently tests HTTP, SOCKS5, HTTPS)
	client, proto, err := detectWorkingClient(ctx, proxyAddr)
	if err != nil {
		return proxyInfo, errors.New("Protocol detection failed")
	}
	proxyInfo.Protocol = proto

	// Stage 3: HTTPS Request Test (Ensures the proxy can tunnel SSL)
	if err := testProtocol(ctx, client, config.HTTPSTargets); err != nil {
		return proxyInfo, errors.New("HTTPS test failed")
	}

	// Stage 4 & 5: Download Speed Test & Throughput Validation
	speed, err := testSpeed(ctx, client, config.SpeedTargets)
	if err != nil {
		return proxyInfo, errors.New("Speed test failed")
	}

	// Immediate rejection rule
	if speed < config.MinSpeedKBps {
		return proxyInfo, errors.New("Speed below threshold")
	}

	proxyInfo.Speed = speed
	return proxyInfo, nil
}

func testTCP(address string) (time.Duration, error) {
	start := time.Now()
	conn, err := net.DialTimeout("tcp", address, config.ConnectTimeout)
	if err != nil {
		return 0, err
	}
	_ = conn.Close()
	return time.Since(start), nil
}

// detectWorkingClient spins up concurrent checks to quickly identify proxy protocol
func detectWorkingClient(ctx context.Context, proxyAddr string) (*http.Client, string, error) {
	protocols := []string{"http", "socks5", "https"}

	type result struct {
		client *http.Client
		proto  string
	}

	resChan := make(chan result, len(protocols))
	var wg sync.WaitGroup

	// Use the fastest possible target to test if the proxy forwards traffic
	target := config.HTTPTargets[0]
	if len(config.HTTPTargets) > 1 {
		target = config.HTTPTargets[1] // Use a generator endpoint if available (very lightweight)
	}

	for _, proto := range protocols {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			client := buildProxyClient(p + "://" + proxyAddr)

			// Fail fast context bounds for proxy detection specifically
			detectCtx, cancel := context.WithTimeout(ctx, config.ConnectTimeout)
			defer cancel()

			req, _ := http.NewRequestWithContext(detectCtx, "GET", target, nil)
			resp, err := client.Do(req)
			if err == nil {
				_ = resp.Body.Close()
				if resp.StatusCode >= 200 && resp.StatusCode < 400 {
					resChan <- result{client, p}
				}
			}
		}(proto)
	}

	// Close channel cleanly when all tests finish
	go func() {
		wg.Wait()
		close(resChan)
	}()

	// Grab the first protocol that successfully authenticates and routes
	res, ok := <-resChan
	if ok {
		return res.client, res.proto, nil
	}

	return nil, "", errors.New("no supported protocol found")
}

func testProtocol(ctx context.Context, client *http.Client, targets []string) error {
	for _, target := range targets {
		req, _ := http.NewRequestWithContext(ctx, "GET", target, nil)
		resp, err := client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 400 {
				return nil // Success on first working target
			}
		}
	}
	return errors.New("all targets failed")
}

func testSpeed(ctx context.Context, client *http.Client, targets []string) (float64, error) {
	for _, target := range targets {
		req, _ := http.NewRequestWithContext(ctx, "GET", target, nil)

		start := time.Now()
		resp, err := client.Do(req)
		if err != nil {
			continue // Try next target
		}

		// Use limit reader to only read exactly what we want if content is unexpectedly large
		written, err := io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()

		elapsedSeconds := time.Since(start).Seconds()

		if written > 0 && elapsedSeconds > 0 {
			kbps := (float64(written) / elapsedSeconds) / 1024.0
			return kbps, nil
		}
	}
	return 0, errors.New("speed test targets exhausted")
}

func buildProxyClient(proxyURLStr string) *http.Client {
	proxyURL, _ := url.Parse(proxyURLStr)
	transport := &http.Transport{
		Proxy: http.ProxyURL(proxyURL),
		DialContext: (&net.Dialer{
			Timeout:   config.ConnectTimeout,
			KeepAlive: 0, // Disabled keep-alives to preserve resources during mass tests
		}).DialContext,
		TLSHandshakeTimeout: config.ConnectTimeout,
		DisableKeepAlives:   true,
	}

	return &http.Client{
		Transport: transport,
		Timeout:   config.TestTimeout,
	}
}
