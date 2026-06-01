package validator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
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

	// Stage 2: HTTPS Request Test to determine proxy type and fetch country
	proxyType := "socks5"
	client := buildProxyClient(proxyAddr, proxyType)
	country, err := testProtocol(ctx, client, config.HTTPSTargets)
	
	if err != nil {
		proxyType = "http"
		client = buildProxyClient(proxyAddr, proxyType)
		country, err = testProtocol(ctx, client, config.HTTPSTargets)
	}

	if err != nil {
		return proxyInfo, errors.New("HTTPS test failed for both protocols")
	}

	proxyInfo.Type = proxyType
	proxyInfo.Country = "Unknown"
	if country != "" {
		proxyInfo.Country = country
	}

	// Stage 3: HTTP Request Test
	if _, err := testProtocol(ctx, client, config.HTTPTargets); err != nil {
		return proxyInfo, errors.New("HTTP test failed")
	}

	// Stage 4: Specific Sites Check (e.g. Telegram / YouTube)
	if len(config.SpecificTargets) > 0 {
		if err := testSpecificSites(ctx, client, config.SpecificTargets); err != nil {
			return proxyInfo, fmt.Errorf("Specific sites test failed: %v", err)
		}
	}

	// Stage 5: Download Speed Test & Throughput Validation
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

func testProtocol(ctx context.Context, client *http.Client, targets []string) (string, error) {
	country := "Unknown"
	for _, target := range targets {
		req, _ := http.NewRequestWithContext(ctx, "GET", target, nil)
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
		
		resp, err := client.Do(req)
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 400 {
				bodyStr := string(body)
				if strings.Contains(target, "cloudflare.com") {
					for _, line := range strings.Split(bodyStr, "\n") {
						if strings.HasPrefix(line, "loc=") {
							country = strings.TrimPrefix(line, "loc=")
							break
						}
					}
				}
				return country, nil
			}
		}
	}
	return "", errors.New("all targets failed")
}

func testSpecificSites(ctx context.Context, client *http.Client, targets []string) error {
	for _, target := range targets {
		req, _ := http.NewRequestWithContext(ctx, "GET", target, nil)
		// Many sites reject default backend request user agents
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36")
		
		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("failed to reach %s", target)
		}
		_ = resp.Body.Close()
		
		if resp.StatusCode >= 400 {
			return fmt.Errorf("target %s returned status %d", target, resp.StatusCode)
		}
	}
	return nil
}

func testSpeed(ctx context.Context, client *http.Client, targets []string) (float64, error) {
	for _, target := range targets {
		req, _ := http.NewRequestWithContext(ctx, "GET", target, nil)
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
		
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

func buildProxyClient(proxyAddr, proxyType string) *http.Client {
	var proxyURL *url.URL
	if proxyType == "socks5" {
		proxyURL, _ = url.Parse("socks5://" + proxyAddr)
	} else {
		proxyURL, _ = url.Parse("http://" + proxyAddr)
	}

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
