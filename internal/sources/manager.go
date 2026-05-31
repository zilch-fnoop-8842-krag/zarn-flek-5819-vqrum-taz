package sources

import (
	"context"
	"io"
	"net/http"
	"regexp"
	"sync"

	"github.com/proxy-collector/internal/logger"
)

// List of dozens of public proxy endpoints
var proxySources = []string{
	"https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
	"https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
	"https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt",
	"https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTPS_RAW.txt",
	"https://raw.githubusercontent.com/Boster12/Free_Proxy_List/main/proxies.txt",
	"https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt",
	"https://raw.githubusercontent.com/mertguvencli/http-proxy-list/main/proxy-list/data.txt",
	"https://raw.githubusercontent.com/rdavydov/proxy-list/main/proxies/http.txt",
	"https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/http/data.txt",
	"https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-http.txt",
	"https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/master/http.txt",
	"https://raw.githubusercontent.com/yemixzy/proxy-list/main/proxy-list/data.txt",
	"https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/http.txt",
	"https://raw.githubusercontent.com/vakhov/free-proxy-list/main/proxies/http.txt",
	"https://api.proxyscrape.com/v2/?request=getproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all",
}

// Regex to accurately extract IP:PORT strings from noisy text blocks
var proxyRegex = regexp.MustCompile(`\b(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?):[0-9]{1,5}\b`)

// FetchAndDeduplicate aggregates proxies from all sources concurrently
func FetchAndDeduplicate(ctx context.Context) ([]string, int) {
	logger.Info("Starting proxy collection from %d sources...", len(proxySources))

	var wg sync.WaitGroup
	proxyChan := make(chan string, 10000)

	// Fetch from all sources concurrently
	for _, url := range proxySources {
		wg.Add(1)
		go func(src string) {
			defer wg.Done()
			fetchSource(ctx, src, proxyChan)
		}(url)
	}

	// Close channel when workers finish
	go func() {
		wg.Wait()
		close(proxyChan)
	}()

	// Efficient Deduplication using map
	uniqueMap := make(map[string]struct{})
	totalCollected := 0

	for p := range proxyChan {
		totalCollected++
		uniqueMap[p] = struct{}{}
	}

	var deduped []string
	for p := range uniqueMap {
		deduped = append(deduped, p)
	}

	logger.Info("Collected %d total proxies. Removing duplicates left %d unique proxies.", totalCollected, len(deduped))
	duplicates := totalCollected - len(deduped)

	return deduped, duplicates
}

func fetchSource(ctx context.Context, url string, out chan<- string) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return
	}

	// Quick timeout for fetches
	client := &http.Client{Timeout: 10 * http.Second}
	resp, err := client.Do(req)
	if err != nil || resp.StatusCode != 200 {
		logger.Warn("Failed to fetch source: %s", url)
		return
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return
	}

	matches := proxyRegex.FindAllString(string(bodyBytes), -1)
	for _, match := range matches {
		out <- match
	}
	logger.Info("Source %s returned %d proxies", url, len(matches))
}
