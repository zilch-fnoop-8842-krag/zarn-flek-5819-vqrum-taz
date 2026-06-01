package validator

import (
	"context"
	"crypto/tls"
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

	// Stage 1: TCP Connectivity Check (Fail-fast before protocol testing)
	latency, err := testTCP(proxyAddr)
	if err != nil {
		return proxyInfo, errors.New("TCP test failed")
	}
	proxyInfo.Latency = latency

	// Stage 2: Concurrent Protocol Detection (HTTP, HTTPS, SOCKS5) & Country Fetch
	proxyType, client, country, err := detectProtocol(ctx, proxyAddr)
	if err != nil {
		return proxyInfo, errors.New("protocol detection failed")
	}

	proxyInfo.Type = proxyType
	proxyInfo.Country = country

	// Stage 3: Standard HTTP Request Test
	if err := testProtocol(ctx, client, config.HTTPTargets); err != nil {
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

// detectProtocol tests HTTP, HTTPS, and SOCKS5 concurrently. 
// The first one to succeed wins, instantly cancelling the others.
func detectProtocol(ctx context.Context, proxyAddr string) (string, *http.Client, string, error) {
	detectCtx, cancel := context.WithCancel(ctx)
	defer cancel() // Automatically kills remaining pending requests once one finishes

	type result struct {
		pType   string
		client  *http.Client
		country string
		err     error
	}

	// 3 protocols to test
	resChan := make(chan result, 3)

	check := func(pType string) {
		client := buildProxyClient(proxyAddr, pType)
		target := config.HTTPSTargets[0] // highly reliable target (e.g., Cloudflare trace)

		req, _ := http.NewRequestWithContext(detectCtx, "GET", target, nil)
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

		resp, err := client.Do(req)
		if err != nil {
			resChan <- result{pType: pType, err: err}
			return
		}

		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 400 {
			country := "Unknown"
			bodyStr := string(body)
			if strings.Contains(target, "cloudflare.com") {
				for _, line := range strings.Split(bodyStr, "\n") {
					if strings.HasPrefix(line, "loc=") {
						country = strings.TrimPrefix(line, "loc=")
						break
					}
				}
			}
			resChan <- result{pType: pType, client: client, country: country, err: nil}
			return
		}
		resChan <- result{pType: pType, err: fmt.Errorf("bad status %d", resp.StatusCode)}
	}

	// Launch concurrency for all 3 protocols
	go check("http")
	go check("https")
	go check("socks5")

	// Wait for the first successful result
	for i := 0; i < 3; i++ {
		res := <-resChan
		if res.err == nil {
			return res.pType, res.client, res.country, nil
		}
	}

	return "", nil, "", errors.New("all protocols failed")
}

func testProtocol(ctx context.Context, client *http.Client, targets []string) error {
	for _, target := range targets {
		req, _ := http.NewRequestWithContext(ctx, "GET", target, nil)
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

		resp, err := client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 400 {
				return nil
			}
		}
	}
	return errors.New("all targets failed")
}

func testSpecificSites(ctx context.Context, client *http.Client, targets []string) error {
	for _, target := range targets {
		req, _ := http.NewRequestWithContext(ctx, "GET", target, nil)
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
			continue
		}

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
	// Dynamically build scheme: http://, https://, or socks5://
	proxyURL, _ := url.Parse(fmt.Sprintf("%s://%s", proxyType, proxyAddr))

	transport := &http.Transport{
		Proxy: http.ProxyURL(proxyURL),
		DialContext: (&net.Dialer{
			Timeout:   config.ConnectTimeout,
			KeepAlive: 0,
		}).DialContext,
		TLSHandshakeTimeout: config.ConnectTimeout,
		DisableKeepAlives:   true,
		// REQUIRED for HTTPS Proxies: 
		// Skips IP-based Certificate hostname mismatch verification
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}

	return &http.Client{
		Transport: transport,
		Timeout:   config.TestTimeout,
	}
}
