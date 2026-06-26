package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/schollz/progressbar/v3"
	"golang.org/x/net/proxy"
)

// Constants and Configuration
const (
	LightshotBaseURL        = "https://prnt.sc/"
	PlaceholderImagePattern = `st\.prntscr\.com/.*?/img/(qjx31j\.png|no-image\.png)` // Common placeholder image patterns
	ProxyFetchInterval      = 5 * time.Minute                                      // How often to refetch proxies
	ProxyCooldownDuration   = 2 * time.Minute                                      // How long a proxy is quarantined after failure
	HTTPClientTimeout       = 10 * time.Second                                     // Overall HTTP request timeout
	DialTimeout             = 5 * time.Second                                      // Timeout for establishing a connection
	TLSHandshakeTimeout     = 5 * time.Second                                      // Timeout for TLS handshake
	MaxProxyRetries         = 3                                                    // Max attempts for a single URL with different proxies
)

var (
	// Regex to extract the image src URL from the screenshot-image class
	imageSrcRegex    = regexp.MustCompile(`<img class="screenshot-image" src="(.*?)"`)
	// Compile the placeholder regex ONCE globally to save massive CPU overhead
	placeholderRegex = regexp.MustCompile(PlaceholderImagePattern)
	// Charset for generating Lightshot IDs (6 characters)
	idCharset        = []rune("abcdefghijklmnopqrstuvwxyz0123456789")
)

// Proxy represents a proxy server with its URL and health status.
type Proxy struct {
	URL        *url.URL
	LastFailed time.Time // Timestamp when the proxy last failed
	mu         sync.Mutex // Protects LastFailed
}

func (p *Proxy) MarkFailed() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.LastFailed = time.Now()
}

func (p *Proxy) IsHealthy() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return time.Since(p.LastFailed) > ProxyCooldownDuration
}

// ProxyManager manages a pool of proxies, handling health checks and rotation.
type ProxyManager struct {
	proxies        []*Proxy
	healthyProxies chan *Proxy
	proxyListURL   string
	mu             sync.RWMutex
	wg             sync.WaitGroup
	ctx            context.Context
	cancel         context.CancelFunc
}

func NewProxyManager(proxyListURL string) *ProxyManager {
	ctx, cancel := context.WithCancel(context.Background())
	pm := &ProxyManager{
		proxyListURL:   proxyListURL,
		healthyProxies: make(chan *Proxy, 100),
		ctx:            ctx,
		cancel:         cancel,
	}
	pm.fetchProxies()
	go pm.startProxyMonitor()
	return pm
}

func (pm *ProxyManager) GetProxy() *Proxy {
	return <-pm.healthyProxies
}

func (pm *ProxyManager) ReturnProxy(p *Proxy) {
	if p.IsHealthy() {
		select {
		case pm.healthyProxies <- p:
		case <-pm.ctx.Done():
			return
		default:
		}
	}
}

func (pm *ProxyManager) fetchProxies() {
	log.Printf("Fetching proxies from %s", pm.proxyListURL)
	reqCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, "GET", pm.proxyListURL, nil)
	if err != nil {
		log.Printf("Error creating request for proxy list: %v", err)
		return
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("Error fetching proxy list: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("Failed to fetch proxy list: HTTP %d", resp.StatusCode)
		return
	}

	scanner := bufio.NewScanner(resp.Body)
	var newProxies []*Proxy
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		proxyURL, err := url.Parse(line)
		if err != nil {
			continue
		}
		newProxies = append(newProxies, &Proxy{URL: proxyURL})
	}

	if len(newProxies) == 0 {
		log.Println("No valid proxies found in the list.")
		return
	}

	pm.mu.Lock()
	pm.proxies = newProxies
	pm.mu.Unlock()
	log.Printf("Successfully fetched %d proxies.", len(newProxies))
}

func (pm *ProxyManager) startProxyMonitor() {
	pm.wg.Add(1)
	defer pm.wg.Done()

	healthCheckTicker := time.NewTicker(5 * time.Second)
	defer healthCheckTicker.Stop()

	fetchTicker := time.NewTicker(ProxyFetchInterval)
	defer fetchTicker.Stop()

	for {
		select {
		case <-pm.ctx.Done():
			return
		case <-fetchTicker.C:
			pm.fetchProxies()
		case <-healthCheckTicker.C:
			pm.mu.RLock()
			for _, p := range pm.proxies {
				if p.IsHealthy() {
					select {
					case pm.healthyProxies <- p:
					default:
					}
				}
			}
			pm.mu.RUnlock()
		}
	}
}

func (pm *ProxyManager) Shutdown() {
	pm.cancel()
	pm.wg.Wait()
}

func generateRandomID() string {
	b := make([]rune, 6)
	for i := range b {
		num, _ := rand.Int(rand.Reader, big.NewInt(int64(len(idCharset))))
		b[i] = idCharset[num.Int64()]
	}
	return string(b)
}

func createHTTPClient(p *Proxy) (*http.Client, error) {
	var dialer proxy.Dialer
	var err error

	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   DialTimeout,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout:   TLSHandshakeTimeout,
		ResponseHeaderTimeout: HTTPClientTimeout,
		ExpectContinueTimeout: 1 * time.Second,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
	}

	switch p.URL.Scheme {
	case "http", "https":
		transport.Proxy = http.ProxyURL(p.URL)
	case "socks5":
		dialer, err = proxy.SOCKS5("tcp", p.URL.Host, nil, proxy.Direct)
		if err != nil {
			return nil, fmt.Errorf("failed to create SOCKS5 dialer: %w", err)
		}
		transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			return dialer.Dial(network, addr)
		}
	default:
		return nil, fmt.Errorf("unsupported proxy scheme: %s", p.URL.Scheme)
	}

	return &http.Client{
		Transport: transport,
		Timeout:   HTTPClientTimeout,
	}, nil
}

func worker(id int, pm *ProxyManager, idChan <-chan string, foundURLChan chan<- string, wg *sync.WaitGroup, bar *progressbar.ProgressBar) {
	defer wg.Done()

	for {
		select {
		case imgID, ok := <-idChan:
			if !ok {
				return // ID channel closed
			}

			fullURL := LightshotBaseURL + imgID
			var (
				resp *http.Response
				err  error
				body []byte
			)

			for attempts := 0; attempts < MaxProxyRetries; attempts++ {
				proxy := pm.GetProxy()

				client, clientErr := createHTTPClient(proxy)
				if clientErr != nil {
					proxy.MarkFailed()
					continue
				}

				ctx, cancel := context.WithTimeout(context.Background(), HTTPClientTimeout)
				req, reqErr := http.NewRequestWithContext(ctx, "GET", fullURL, nil)
				if reqErr != nil {
					cancel()
					proxy.MarkFailed()
					continue
				}

				resp, err = client.Do(req)
				cancel()

				if err != nil {
					proxy.MarkFailed()
					time.Sleep(100 * time.Millisecond)
					continue
				}

				// If we get a response, we MUST manually close the body to prevent memory leaks.
				if resp.StatusCode != http.StatusOK {
					if resp.StatusCode == http.StatusForbidden || (resp.StatusCode >= 500 && resp.StatusCode < 600) {
						resp.Body.Close() // Close before retrying
						proxy.MarkFailed()
						continue
					}
				}

				pm.ReturnProxy(proxy)
				body, err = io.ReadAll(resp.Body)
				resp.Body.Close() // Close immediately after reading
				
				if err != nil {
					break 
				}
				break
			}

			bar.Add(1)

			if resp == nil || resp.StatusCode != http.StatusOK {
				continue
			}

			bodyStr := string(body)
			matches := imageSrcRegex.FindStringSubmatch(bodyStr)
			if len(matches) > 1 {
				imageSrc := matches[1]

				// Use the pre-compiled regex here for massive performance boost
				if placeholderRegex.MatchString(imageSrc) ||
					strings.Contains(bodyStr, `alt="screenshot deleted"`) ||
					strings.Contains(bodyStr, `alt="Image not found"`) ||
					strings.Contains(bodyStr, `alt="No image"`) {
					continue
				}

				if strings.HasPrefix(imageSrc, "//") {
					imageSrc = "https:" + imageSrc
				}

				foundURLChan <- fmt.Sprintf("%s -> %s", fullURL, imageSrc)
			}
		case <-pm.ctx.Done():
			return
		}
	}
}

func main() {
	proxyListURL := os.Getenv("PROXY_LIST_URL")
	if proxyListURL == "" {
		proxyListURL = "https://raw.githubusercontent.com/zilch-fnoop-8842-krag/glorf-nibz-7724-xqpto-muv/refs/heads/flurbo-womplex-4491-zzyzx/valid_proxies.txt"
	}

	concurrencyStr := os.Getenv("CONCURRENCY")
	concurrency := 50
	if concurrencyStr != "" {
		if c, err := strconv.Atoi(concurrencyStr); err == nil && c > 0 {
			concurrency = c
		}
	}

	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Println("Starting Lightshot URL Scanner...")

	pm := NewProxyManager(proxyListURL)

	idChan := make(chan string, concurrency*2)
	foundURLChan := make(chan string, concurrency)

	var wgWorkers sync.WaitGroup
	var wgIDGenerator sync.WaitGroup

	bar := progressbar.NewOptions(-1,
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionShowBytes(false),
		progressbar.OptionSetDescription("[cyan]Scanning Lightshot URLs..."),
		progressbar.OptionFullWidth(),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "|",
			BarEnd:        "|",
		}),
	)

	// --- Start Workers ---
	for i := 0; i < concurrency; i++ {
		wgWorkers.Add(1)
		go worker(i, pm, idChan, foundURLChan, &wgWorkers, bar)
	}

	// --- Start ID Generator ---
	wgIDGenerator.Add(1)
	go func() {
		defer wgIDGenerator.Done()
		defer close(idChan)
		for {
			select {
			case <-pm.ctx.Done():
				return
			case idChan <- generateRandomID():
			default:
				time.Sleep(1 * time.Millisecond)
			}
		}
	}()

	// --- Output Handler ---
	outputFile, err := os.OpenFile("found_images.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open output file: %v", err)
	}
	defer outputFile.Close()
	outputWriter := bufio.NewWriter(outputFile)
	defer outputWriter.Flush()

	var wgOutput sync.WaitGroup
	wgOutput.Add(1)
	go func() {
		defer wgOutput.Done()
		// Flush every 5 seconds to be safe, instead of every line
		flushTicker := time.NewTicker(5 * time.Second)
		defer flushTicker.Stop()

		for {
			select {
			case foundURL, ok := <-foundURLChan:
				if !ok {
					return // Channel closed, exit safely
				}
				fmt.Println(foundURL)
				if _, err := outputWriter.WriteString(foundURL + "\n"); err != nil {
					log.Printf("Error writing to file: %v", err)
				}
			case <-flushTicker.C:
				outputWriter.Flush()
			}
		}
	}()

	// --- Graceful Shutdown Handling ---
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan // Blocks here until GitHub Actions sends a timeout/kill signal
	log.Println("\nReceived shutdown signal. Wrapping up safely...")

	// 1. Stop the ProxyManager (this cancels ctx, stopping generator and workers)
	pm.Shutdown()
	
	// 2. Wait for the ID generator to finish and close idChan
	wgIDGenerator.Wait()
	
	// 3. Wait for all HTTP workers to finish their current requests
	wgWorkers.Wait()
	
	// 4. Now that workers are done, no more URLs will be sent. Safe to close.
	close(foundURLChan)
	
	// 5. Wait for the output handler to write the final URLs to the file
	wgOutput.Wait()
	
	// The `defer outputWriter.Flush()` will now trigger and save the file.
	log.Println("Shutdown complete. File saved successfully.")
}
