package main

import (
    "context"
    "fmt"
    "io"
    "log"
    "net"
    "net/http"
    "net/url"
    "strings"
    "sync"
    "sync/atomic"
    "time"

    "golang.org/x/net/proxy"
)

const (
    charset         = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
    idLength        = 24
    proxyListURL    = "https://raw.githubusercontent.com/zilch-fnoop-8842-krag/glorf-nibz-7724-xqpto-muv/refs/heads/flurbo-womplex-4491-zzyzx/valid_proxies.txt"
    numWorkers      = 50 // تعداد ورکرهای همزمان
    targetURLFormat = "https://i.ytimg.com/an/%s/featured_channel.jpg"
)

var (
    proxyClients []*http.Client
    proxyIndex   uint64
    requestCount uint64
)

// ساخت آیدی رندوم
func generateRandomID() string {
    var sb strings.Builder
    for i := 0; i < idLength; i++ {
        sb.WriteByte(charset[rand.Intn(len(charset))])
    }
    return sb.String()
}

// ساخت کلاینت HTTP بر اساس نوع پروکسی
func createProxyClient(proxyStr string) (*http.Client, error) {
    proxyURL, err := url.Parse(proxyStr)
    if err != nil {
        return nil, err
    }

    transport := &http.Transport{
        // تایم‌اوت برای برقراری ارتباط
        DialContext: (&net.Dialer{
            Timeout:   10 * time.Second,
            KeepAlive: 10 * time.Second,
        }).DialContext,
        TLSHandshakeTimeout:   10 * time.Second,
        ResponseHeaderTimeout: 10 * time.Second,
    }

    // اگر پروکسی SOCKS5 بود
    if strings.HasPrefix(proxyStr, "socks5://") {
        dialer, err := proxy.SOCKS5("tcp", proxyURL.Host, nil, proxy.Direct)
        if err != nil {
            return nil, err
        }
        contextDialer, ok := dialer.(proxy.ContextDialer)
        if !ok {
            return nil, fmt.Errorf("socks5 dialer does not support DialContext")
        }
        transport.DialContext = contextDialer.DialContext
    } else if strings.HasPrefix(proxyStr, "http://") || strings.HasPrefix(proxyStr, "https://") {
        // اگر پروکسی HTTP/HTTPS بود
        transport.Proxy = http.ProxyURL(proxyURL)
    }

    return &http.Client{
        Transport: transport,
        Timeout:   15 * time.Second, // تایم‌اوت کل ریکوئست
    }, nil
}

// دانلود و ساخت کلاینت‌های پروکسی
func loadProxies() error {
    log.Println("Downloading proxy list...")
    resp, err := http.Get(proxyListURL)
    if err != nil {
        return fmt.Errorf("failed to download proxy list: %v", err)
    }
    defer resp.Body.Close()

    body, err := io.ReadAll(resp.Body)
    if err != nil {
        return fmt.Errorf("failed to read proxy list: %v", err)
    }

    lines := strings.Split(strings.TrimSpace(string(body)), "\n")
    for _, line := range lines {
        line = strings.TrimSpace(line)
        if line == "" {
            continue
        }
        client, err := createProxyClient(line)
        if err != nil {
            log.Printf("Skipping proxy %s: %v", line, err)
            continue
        }
        proxyClients = append(proxyClients, client)
    }

    if len(proxyClients) == 0 {
        return fmt.Errorf("no valid proxies loaded. Exiting")
    }

    log.Printf("Successfully loaded %d proxies.\n", len(proxyClients))
    return nil
}

func worker(id int, wg *sync.WaitGroup, results chan<- string) {
    defer wg.Done()

    for {
        // گرفتن یک پروکسی به صورت Round-Robin با استفاده از Atomic (بدون قفل برای سرعت بالا)
        idx := atomic.AddUint64(&proxyIndex, 1) % uint64(len(proxyClients))
        client := proxyClients[idx]

        randomID := generateRandomID()
        checkURL := fmt.Sprintf(targetURLFormat, randomID)

        resp, err := client.Head(checkURL)
        if err != nil {
            // پروکسی مرد یا تایم‌اوت خورد، ادامه بده
            continue
        }
        resp.Body.Close()

        // ثبت تعداد ریکوئست‌ها
        count := atomic.AddUint64(&requestCount, 1)
        if count%1000 == 0 {
            log.Printf("[Status] Total requests sent so far: %d", count)
        }

        if resp.StatusCode == http.StatusOK {
            results <- checkURL
        }
    }
}

func main() {
    // مقداردهی اولیه سید رندوم
    rand.Seed(time.Now().UnixNano())

    if err := loadProxies(); err != nil {
        log.Fatalf("Error: %v", err)
    }

    var wg sync.WaitGroup
    results := make(chan string, 10)

    // گوروتین برای پرینت نتایج موفق
    go func() {
        for url := range results {
            fmt.Printf("\n🚀 [SUCCESS] Found valid avatar: %s\n", url)
        }
    }()

    log.Printf("Starting %d workers...\n", numWorkers)
    for i := 0; i < numWorkers; i++ {
        wg.Add(1)
        go worker(i, &wg, results)
    }

    // در محیط اکشنز، می‌تونیم زمان‌بندی کنیم یا تا ابد اجره بشه
    // اینجا برای اکشنز به صورت تئوری تا ابد اجرا میشه مگه اینکه تایم‌اوت اکشنز تموم بشه
    wg.Wait()
    close(results)
}
