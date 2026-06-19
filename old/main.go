package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mmdb "github.com/oschwald/maxminddb-golang"
	"golang.org/x/net/proxy"
)

// ------------------------------ config ------------------------------

const (
	checkURL        = "http://httpbin.org/ip"
	proxyTimeout    = 3 * time.Second
	downloadTimeout = 10 * time.Second
	mmdbFile        = "GeoLite2-Country.mmdb"
)

var mmdbURLs = []string{
	"https://raw.githubusercontent.com/Loyalsoldier/geoip/release/Country.mmdb",
	"https://github.com/P3TERX/GeoLite.mmdb/raw/download/GeoLite2-Country.mmdb",
}

var sources = map[string][]string{
	"http": {
		"https://raw.githubusercontent.com/officialputuid/ProxyForEveryone/master/http/http.txt",
		"https://raw.githubusercontent.com/officialputuid/ProxyForEveryone/main/http/http.txt",
		"https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt",
		"https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/https.txt",
		"https://raw.githubusercontent.com/mmpx12/proxy-list/master/http.txt",
		"https://raw.githubusercontent.com/mmpx12/proxy-list/master/https.txt",
		"https://raw.githubusercontent.com/rdavydov/proxy-list/main/proxies/http.txt",
		"https://raw.githubusercontent.com/rdavydov/proxy-list/main/proxies_anonymous/http.txt",
		"https://raw.githubusercontent.com/prxchk/proxy-list/main/http.txt",
		"https://raw.githubusercontent.com/ALIILAPRO/Proxy/main/http.txt",
		"https://proxyspace.pro/http.txt",
		"https://proxyspace.pro/https.txt",
		"https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/http.txt",
		"https://multiproxy.org/txt/status/all.txt",
		"https://alexa.lr2b.com/proxylist.txt",
		"https://raw.githubusercontent.com/dpangestuw/Free-Proxy/main/http_proxies.txt",
		"https://raw.githubusercontent.com/dinoz0rg/proxy-list/main/checked_proxies/http.txt",
		"https://raw.githubusercontent.com/databay-labs/free-proxy-list/master/http.txt",
		"https://raw.githubusercontent.com/ebrasha/abdal-proxy-hub/main/http-proxy-list-by-EbraSha.txt",
		"https://raw.githubusercontent.com/zloi-user/hideip.me/main/http.txt",
		"https://raw.githubusercontent.com/vmheaven/VMHeaven.io-Free-Proxy-List/main/http.txt",
		"https://raw.githubusercontent.com/VPSLabCloud/VPSLab-Free-Proxy-List/main/http_all.txt",
		"https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
		"https://raw.githubusercontent.com/saschazesiger/Free-Proxies/master/proxies/http.txt",
		"https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-http.txt",
		"https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTP_RAW.txt",
		"https://raw.githubusercontent.com/UserR3X/proxy-list/main/http.txt",
		"https://raw.githubusercontent.com/Hakimi0804/proxy-list/main/http.txt",
		"https://raw.githubusercontent.com/Volodichev/proxy-list/main/http.txt",
		"https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
		"https://raw.githubusercontent.com/Anonym0usWork1221/Free-Proxies/main/proxy-list/http.txt",
		"https://raw.githubusercontent.com/enseitankado/proxy-list/master/http.txt",
		"https://raw.githubusercontent.com/themiralay/Proxy-List/master/http.txt",
		"https://raw.githubusercontent.com/volam9999/proxy-list/main/http.txt",
		"https://raw.githubusercontent.com/mertguvencli/http-proxy-list/main/proxy-list.txt",
		"https://raw.githubusercontent.com/B4RC0DE-TM/Proxy-List/main/http.txt",
		"https://raw.githubusercontent.com/Im2023/Free-Proxy-List/main/http.txt",
		"https://raw.githubusercontent.com/calayji/Proxy-List/main/HTTP.txt",
		"https://raw.githubusercontent.com/topfreevpn/proxy-list/main/http.txt",
		"https://api.proxyscrape.com/v2/?request=getproxies&protocol=http&timeout=10000&country=all",
		"https://api.proxyscrape.com/v3/free-proxy-list/get?request=displayproxies&protocol=http&proxy_format=ipport&format=text&timeout=10000",
		"https://www.proxy-list.download/api/v1/get?type=http",
	},
	"socks4": {
		"https://raw.githubusercontent.com/officialputuid/ProxyForEveryone/master/socks4/socks4.txt",
		"https://raw.githubusercontent.com/officialputuid/ProxyForEveryone/main/socks4/socks4.txt",
		"https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/socks4.txt",
		"https://raw.githubusercontent.com/mmpx12/proxy-list/master/socks4.txt",
		"https://raw.githubusercontent.com/rdavydov/proxy-list/main/proxies/socks4.txt",
		"https://raw.githubusercontent.com/prxchk/proxy-list/main/socks4.txt",
		"https://raw.githubusercontent.com/ALIILAPRO/Proxy/main/socks4.txt",
		"https://proxyspace.pro/socks4.txt",
		"https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/socks4.txt",
		"https://raw.githubusercontent.com/dpangestuw/Free-Proxy/main/socks4_proxies.txt",
		"https://raw.githubusercontent.com/dinoz0rg/proxy-list/main/checked_proxies/socks4.txt",
		"https://raw.githubusercontent.com/databay-labs/free-proxy-list/master/socks4.txt",
		"https://raw.githubusercontent.com/ebrasha/abdal-proxy-hub/main/socks4-proxy-list-by-EbraSha.txt",
		"https://raw.githubusercontent.com/zloi-user/hideip.me/main/socks4.txt",
		"https://raw.githubusercontent.com/vmheaven/VMHeaven.io-Free-Proxy-List/main/socks4.txt",
		"https://raw.githubusercontent.com/VPSLabCloud/VPSLab-Free-Proxy-List/main/socks4_all.txt",
		"https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks4.txt",
		"https://raw.githubusercontent.com/saschazesiger/Free-Proxies/master/proxies/socks4.txt",
		"https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-socks4.txt",
		"https://raw.githubusercontent.com/UserR3X/proxy-list/main/socks4.txt",
		"https://raw.githubusercontent.com/Hakimi0804/proxy-list/main/socks4.txt",
		"https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks4.txt",
		"https://raw.githubusercontent.com/Anonym0usWork1221/Free-Proxies/main/proxy-list/socks4.txt",
		"https://raw.githubusercontent.com/enseitankado/proxy-list/master/socks4.txt",
		"https://raw.githubusercontent.com/themiralay/Proxy-List/master/socks4.txt",
		"https://raw.githubusercontent.com/volam9999/proxy-list/main/socks4.txt",
		"https://raw.githubusercontent.com/calayji/Proxy-List/main/SOCKS4.txt",
		"https://raw.githubusercontent.com/topfreevpn/proxy-list/main/socks4.txt",
		"https://api.proxyscrape.com/v2/?request=getproxies&protocol=socks4&timeout=10000&country=all",
		"https://www.proxy-list.download/api/v1/get?type=socks4",
	},
	"socks5": {
		"https://raw.githubusercontent.com/officialputuid/ProxyForEveryone/master/socks5/socks5.txt",
		"https://raw.githubusercontent.com/officialputuid/ProxyForEveryone/main/socks5/socks5.txt",
		"https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/socks5.txt",
		"https://raw.githubusercontent.com/mmpx12/proxy-list/master/socks5.txt",
		"https://raw.githubusercontent.com/rdavydov/proxy-list/main/proxies/socks5.txt",
		"https://raw.githubusercontent.com/prxchk/proxy-list/main/socks5.txt",
		"https://raw.githubusercontent.com/ALIILAPRO/Proxy/main/socks5.txt",
		"https://proxyspace.pro/socks5.txt",
		"https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/socks5.txt",
		"https://raw.githubusercontent.com/dpangestuw/Free-Proxy/main/socks5_proxies.txt",
		"https://raw.githubusercontent.com/dinoz0rg/proxy-list/main/checked_proxies/socks5.txt",
		"https://raw.githubusercontent.com/databay-labs/free-proxy-list/master/socks5.txt",
		"https://raw.githubusercontent.com/ebrasha/abdal-proxy-hub/main/socks5-proxy-list-by-EbraSha.txt",
		"https://raw.githubusercontent.com/zloi-user/hideip.me/main/socks5.txt",
		"https://raw.githubusercontent.com/vmheaven/VMHeaven.io-Free-Proxy-List/main/socks5.txt",
		"https://raw.githubusercontent.com/VPSLabCloud/VPSLab-Free-Proxy-List/main/socks5_all.txt",
		"https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt",
		"https://raw.githubusercontent.com/saschazesiger/Free-Proxies/master/proxies/socks5.txt",
		"https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-socks5.txt",
		"https://raw.githubusercontent.com/UserR3X/proxy-list/main/socks5.txt",
		"https://raw.githubusercontent.com/Hakimi0804/proxy-list/main/socks5.txt",
		"https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt",
		"https://raw.githubusercontent.com/Anonym0usWork1221/Free-Proxies/main/proxy-list/socks5.txt",
		"https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS5_RAW.txt",
		"https://raw.githubusercontent.com/Volodichev/proxy-list/main/socks5.txt",
		"https://raw.githubusercontent.com/enseitankado/proxy-list/master/socks5.txt",
		"https://raw.githubusercontent.com/themiralay/Proxy-List/master/socks5.txt",
		"https://raw.githubusercontent.com/volam9999/proxy-list/main/socks5.txt",
		"https://raw.githubusercontent.com/calayji/Proxy-List/main/SOCKS5.txt",
		"https://raw.githubusercontent.com/topfreevpn/proxy-list/main/socks5.txt",
		"https://raw.githubusercontent.com/Im2023/Free-Proxy-List/main/socks5.txt",
		"https://api.proxyscrape.com/v2/?request=getproxies&protocol=socks5&timeout=10000&country=all",
		"https://www.proxy-list.download/api/v1/get?type=socks5",
	},
}

var (
	ipPortRE     = regexp.MustCompile(`\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}:[0-9]{2,5}\b`)
	bareIPPortRE = regexp.MustCompile(`^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d+`)
)

// ------------------------------ socks4 dialer ------------------------------

func socks4Dial(proxyAddr, targetAddr string) (net.Conn, error) {
	host, portStr, err := net.SplitHostPort(targetAddr)
	if err != nil {
		return nil, err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, err
	}

	conn, err := net.DialTimeout("tcp", proxyAddr, proxyTimeout)
	if err != nil {
		return nil, err
	}

	ip := net.ParseIP(host)
	var req []byte
	if ip != nil {
		ip4 := ip.To4()
		req = []byte{4, 1, byte(port >> 8), byte(port), ip4[0], ip4[1], ip4[2], ip4[3], 0}
	} else {
		req = []byte{4, 1, byte(port >> 8), byte(port), 0, 0, 0, 1, 0}
		req = append(req, []byte(host)...)
		req = append(req, 0)
	}

	if _, err := conn.Write(req); err != nil {
		conn.Close()
		return nil, err
	}

	resp := make([]byte, 8)
	if _, err := io.ReadFull(conn, resp); err != nil {
		conn.Close()
		return nil, err
	}

	if resp[1] != 90 {
		conn.Close()
		return nil, fmt.Errorf("socks4 rejected: %d", resp[1])
	}

	return conn, nil
}

// ------------------------------ http client per proxy ------------------------------

func makeClient(proxyURL string) (*http.Client, error) {
	tr := &http.Transport{
		DisableKeepAlives:     true,
		IdleConnTimeout:       proxyTimeout,
		ExpectContinueTimeout: 1 * time.Second,
	}

	if strings.HasPrefix(proxyURL, "socks5://") {
		u, err := url.Parse(proxyURL)
		if err != nil {
			return nil, err
		}
		d, err := proxy.SOCKS5("tcp", u.Host, nil, proxy.Direct)
		if err != nil {
			return nil, err
		}
		if cd, ok := d.(proxy.ContextDialer); ok {
			tr.DialContext = cd.DialContext
		} else {
			tr.Dial = d.Dial
		}
	} else if strings.HasPrefix(proxyURL, "socks4://") {
		u, err := url.Parse(proxyURL)
		if err != nil {
			return nil, err
		}
		tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			return socks4Dial(u.Host, addr)
		}
	} else {
		u, err := url.Parse(proxyURL)
		if err != nil {
			return nil, err
		}
		tr.Proxy = http.ProxyURL(u)
	}

	return &http.Client{Transport: tr, Timeout: proxyTimeout + 2*time.Second}, nil
}

// ------------------------------ phase 1: scrape ------------------------------

func scrape() map[string]map[string]struct{} {
	fmt.Println("[*] Phase 1: Downloading proxy lists...")

	client := &http.Client{Timeout: downloadTimeout}
	var mu sync.Mutex
	var wg sync.WaitGroup
	limit := 50
	sem := make(chan struct{}, limit)

	merged := map[string]map[string]struct{}{
		"http":   {},
		"socks4": {},
		"socks5": {},
	}

	for cat, urls := range sources {
		for _, u := range urls {
			sem <- struct{}{}
			wg.Add(1)
			go func(category, url string) {
				defer wg.Done()
				defer func() { <-sem }()
				req, err := http.NewRequest("GET", url, nil)
				if err != nil {
					return
				}
				req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
				resp, err := client.Do(req)
				if err != nil {
					return
				}
				if resp.StatusCode != 200 {
					resp.Body.Close()
					return
				}
				body, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				matches := ipPortRE.FindAllString(string(body), -1)
				mu.Lock()
				for _, m := range matches {
					merged[category][category+"://"+m] = struct{}{}
				}
				mu.Unlock()
			}(cat, u)
		}
	}
	wg.Wait()

	for cat, set := range merged {
		fmt.Printf("[+] %s: %d unique proxies\n", cat, len(set))
	}
	return merged
}

func writeCategoryFiles(dir string, data map[string]map[string]struct{}) {
	os.MkdirAll(dir, 0755)
	allSet := map[string]struct{}{}

	for cat, set := range data {
		path := filepath.Join(dir, cat+".txt")
		f, err := os.Create(path)
		if err != nil {
			fmt.Printf("[-] Failed to create %s: %v\n", path, err)
			continue
		}
		bw := bufio.NewWriter(f)
		proxies := make([]string, 0, len(set))
		for p := range set {
			proxies = append(proxies, p)
		}
		sort.Strings(proxies)
		for _, p := range proxies {
			bw.WriteString(p + "\n")
			allSet[p] = struct{}{}
		}
		bw.Flush()
		f.Close()
		fmt.Printf("[*] Wrote %d proxies to %s\n", len(proxies), path)
	}

	path := filepath.Join(dir, "all.txt")
	f, err := os.Create(path)
	if err != nil {
		fmt.Printf("[-] Failed to create all.txt: %v\n", err)
		return
	}
	bw := bufio.NewWriter(f)
	all := make([]string, 0, len(allSet))
	for p := range allSet {
		all = append(all, p)
	}
	sort.Strings(all)
	for _, p := range all {
		bw.WriteString(p + "\n")
	}
	bw.Flush()
	f.Close()
	fmt.Printf("[*] Wrote %d total proxies to %s\n", len(all), path)
}

// ------------------------------ phase 2: check + country lookup ------------------------------

func readAllProxies(path string) []string {
	f, err := os.Open(path)
	if err != nil {
		fmt.Printf("[-] Cannot open %s: %v\n", path, err)
		return nil
	}
	defer f.Close()

	raw := map[string]struct{}{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		if bareIPPortRE.MatchString(line) {
			raw["http://"+line] = struct{}{}
		} else if strings.HasPrefix(line, "http://") ||
			strings.HasPrefix(line, "https://") ||
			strings.HasPrefix(line, "socks4://") ||
			strings.HasPrefix(line, "socks5://") {
			raw[line] = struct{}{}
		}
	}

	out := make([]string, 0, len(raw))
	for p := range raw {
		out = append(out, p)
	}
	return out
}

type httpbinResp struct {
	Origin string `json:"origin"`
}

type proxyCheckResult struct {
	proxy      string
	ok         bool
	externalIP string
	latencyMs  int64
}

func getRealIP() string {
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(checkURL)
	if err != nil {
		fmt.Printf("[-] Cannot fetch real IP: %v\n", err)
		return ""
	}
	defer resp.Body.Close()
	var hb httpbinResp
	if err := json.NewDecoder(resp.Body).Decode(&hb); err != nil {
		fmt.Printf("[-] Cannot decode real IP: %v\n", err)
		return ""
	}
	ip := hb.Origin
	if idx := strings.IndexByte(ip, ','); idx != -1 {
		ip = strings.TrimSpace(ip[:idx])
	}
	return ip
}

func checkProxies(proxies []string, realIP string) []proxyCheckResult {
	fmt.Printf("\n[*] Phase 2: Checking %d proxies...\n", len(proxies))

	ctx := context.Background()
	limit := 20000
	sem := make(chan struct{}, limit)
	resultCh := make(chan proxyCheckResult, len(proxies))

	var checked atomic.Int64
	var valid atomic.Int64
	var transparent atomic.Int64
	var wg sync.WaitGroup

	barWidth := 40
	total := len(proxies)

	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			default:
				n := checked.Load()
				v := valid.Load()
				pct := float64(n) / float64(total) * 100
				fill := int(pct / 100 * float64(barWidth))
				bar := strings.Repeat("█", fill) + strings.Repeat("░", barWidth-fill)
				fmt.Printf("\r  %s  %5.1f%%  [%d/%d]  ✓%d  ✗%d", bar, pct, n, total, v, n-v)
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	for _, p := range proxies {
		sem <- struct{}{}
		wg.Add(1)
		go func(proxy string) {
			defer wg.Done()
			defer func() { <-sem }()
			defer checked.Add(1)
			client, err := makeClient(proxy)
			if err != nil {
				resultCh <- proxyCheckResult{proxy: proxy}
				return
			}

			req, err := http.NewRequestWithContext(ctx, "GET", checkURL, nil)
			if err != nil {
				resultCh <- proxyCheckResult{proxy: proxy}
				return
			}

			start := time.Now()
			resp, err := client.Do(req)
			if err != nil {
				resultCh <- proxyCheckResult{proxy: proxy}
				return
			}

			latencyMs := time.Since(start).Milliseconds()

			if resp.StatusCode != 200 {
				resp.Body.Close()
				resultCh <- proxyCheckResult{proxy: proxy}
				return
			}

			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()

			var hb httpbinResp
			if err := json.Unmarshal(body, &hb); err != nil || hb.Origin == "" {
				resultCh <- proxyCheckResult{proxy: proxy}
				return
			}

			ip := hb.Origin
			if idx := strings.IndexByte(ip, ','); idx != -1 {
				ip = strings.TrimSpace(ip[:idx])
			}

			if realIP != "" && ip == realIP {
				transparent.Add(1)
				resultCh <- proxyCheckResult{proxy: proxy}
				return
			}

			valid.Add(1)
			resultCh <- proxyCheckResult{
				proxy:      proxy,
				ok:         true,
				externalIP: ip,
				latencyMs:  latencyMs,
			}
		}(p)
	}

	wg.Wait()
	close(done)
	close(resultCh)
	fmt.Println()

	var results []proxyCheckResult
	for r := range resultCh {
		if r.ok {
			results = append(results, r)
		}
	}

	t := int(transparent.Load())
	failed := total - len(results) - t
	fmt.Printf("[+] Working: %d | Transparent: %d | Failed: %d\n", len(results), t, failed)
	return results
}

// ------------------------------ self-hosted geoip (mmdb) ------------------------------

func downloadMMDB() error {
	for _, u := range mmdbURLs {
		fmt.Printf("[*] Downloading GeoIP database from %s ...\n", u)
		req, err := http.NewRequest("GET", u, nil)
		if err != nil {
			fmt.Printf("[-] GeoIP download request error: %v\n", err)
			continue
		}
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			fmt.Printf("[-] GeoIP download error: %v\n", err)
			continue
		}
		if resp.StatusCode != 200 {
			resp.Body.Close()
			fmt.Printf("[-] GeoIP download status %d\n", resp.StatusCode)
			continue
		}
		data, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			fmt.Printf("[-] GeoIP read error: %v\n", err)
			continue
		}
		if err := os.WriteFile(mmdbFile, data, 0644); err != nil {
			fmt.Printf("[-] GeoIP write error: %v\n", err)
			continue
		}
		fmt.Printf("[+] GeoIP database saved (%d bytes)\n", len(data))
		return nil
	}
	return fmt.Errorf("all download sources failed")
}

func openMMDB() (*mmdb.Reader, error) {
	if _, err := os.Stat(mmdbFile); os.IsNotExist(err) {
		fmt.Println("[*] GeoIP database not found, downloading...")
		if err := downloadMMDB(); err != nil {
			return nil, err
		}
	}
	db, err := mmdb.Open(mmdbFile)
	if err != nil {
		return nil, fmt.Errorf("cannot open %s: %w", mmdbFile, err)
	}
	return db, nil
}

func lookupCountry(db *mmdb.Reader, ipStr string) string {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return ""
	}
	var result map[string]any
	if err := db.Lookup(ip, &result); err != nil {
		return ""
	}
	if c, ok := result["country"].(map[string]any); ok {
		if names, ok := c["names"].(map[string]any); ok {
			if en, ok := names["en"].(string); ok {
				return en
			}
		}
	}
	return ""
}

func batchLookupCountries(ips []string) map[string]string {
	if len(ips) == 0 {
		return nil
	}

	db, err := openMMDB()
	if err != nil {
		fmt.Printf("[-] %v\n", err)
		return nil
	}
	defer db.Close()

	countryOf := make(map[string]string, len(ips))
	for _, ip := range ips {
		if c := lookupCountry(db, ip); c != "" {
			countryOf[ip] = c
		}
	}

	fmt.Printf("[+] GeoIP: resolved %d/%d IPs\n", len(countryOf), len(ips))
	return countryOf
}

// ------------------------------ results ------------------------------

type resultEntry struct {
	Proxy   string `json:"proxy"`
	Country string `json:"country"`
	Ping    int64  `json:"ping"`
}

type resultFile struct {
	Results []resultEntry `json:"results"`
}

func writeResults(dir string, valid []proxyCheckResult, countryOf map[string]string) {
	outDir := filepath.Join(dir, "checked_proxies")
	os.MkdirAll(outDir, 0755)

	httpF, _ := os.Create(filepath.Join(outDir, "http.txt"))
	socks4F, _ := os.Create(filepath.Join(outDir, "socks4.txt"))
	socks5F, _ := os.Create(filepath.Join(outDir, "socks5.txt"))
	allF, _ := os.Create(filepath.Join(outDir, "all_working.txt"))

	httpBW := bufio.NewWriter(httpF)
	socks4BW := bufio.NewWriter(socks4F)
	socks5BW := bufio.NewWriter(socks5F)
	allBW := bufio.NewWriter(allF)

	var results []resultEntry

	for _, r := range valid {
		allBW.WriteString(r.proxy + "\n")
		switch {
		case strings.HasPrefix(r.proxy, "socks5://"):
			socks5BW.WriteString(r.proxy + "\n")
		case strings.HasPrefix(r.proxy, "socks4://"):
			socks4BW.WriteString(r.proxy + "\n")
		default:
			httpBW.WriteString(r.proxy + "\n")
		}

		country := countryOf[r.externalIP]
		if country == "" {
			country = "Unknown"
		}

		results = append(results, resultEntry{
			Proxy:   r.proxy,
			Country: country,
			Ping:    r.latencyMs,
		})
	}

	httpBW.Flush()
	socks4BW.Flush()
	socks5BW.Flush()
	allBW.Flush()

	httpF.Close()
	socks4F.Close()
	socks5F.Close()
	allF.Close()

	sort.Slice(results, func(i, j int) bool {
		return results[i].Ping < results[j].Ping
	})

	out := resultFile{Results: results}
	data, _ := json.MarshalIndent(out, "", "  ")

	rp := filepath.Join(dir, "result_counter.json")
	if err := os.WriteFile(rp, data, 0644); err != nil {
		fmt.Printf("[-] Failed to write result_counter.json: %v\n", err)
	} else {
		fmt.Printf("[+] Wrote %d results to %s\n", len(results), rp)
	}
}

// ------------------------------ main ------------------------------

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	ts := time.Now().Format("2006-01-02_15-04-05")
	outDir := filepath.Join("result", ts)

	fmt.Printf("=== GO PROXY SCRAPER + CHECKER ===\n[*] Output: %s/\n", outDir)

	start := time.Now()
	data := scrape()
	writeCategoryFiles(outDir, data)
	fmt.Printf("[*] Scrape done in %v\n", time.Since(start))

	proxies := readAllProxies(filepath.Join(outDir, "all.txt"))
	if len(proxies) == 0 {
		fmt.Println("[-] No proxies to check.")
		return
	}

	realIP := getRealIP()
	valid := checkProxies(proxies, realIP)
	if len(valid) == 0 {
		fmt.Println("[-] No working proxies found.")
		return
	}

	fmt.Printf("\n[*] Phase 3: Looking up countries for %d unique IPs...\n", len(valid))

	uniqIPs := make([]string, 0, len(valid))
	seen := map[string]struct{}{}
	for _, r := range valid {
		if _, ok := seen[r.externalIP]; !ok {
			seen[r.externalIP] = struct{}{}
			uniqIPs = append(uniqIPs, r.externalIP)
		}
	}
	fmt.Printf("[+] %d unique IPs to resolve\n", len(uniqIPs))

	countryOf := batchLookupCountries(uniqIPs)
	writeResults(outDir, valid, countryOf)

	fmt.Println("=== ALL DONE ===")
}
