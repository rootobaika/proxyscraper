package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

type ghRepo struct {
	FullName      string `json:"full_name"`
	DefaultBranch string `json:"default_branch"`
	HTMLURL       string `json:"html_url"`
	Stars         int    `json:"stargazers_count"`
	UpdatedAt     string `json:"updated_at"`
}

type searchResp struct {
	Items []struct {
		FullName      string `json:"full_name"`
		DefaultBranch string `json:"default_branch"`
		HTMLURL       string `json:"html_url"`
		Stars         int    `json:"stargazers_count"`
		UpdatedAt     string `json:"updated_at"`
	} `json:"items"`
}

var searchQueries = []string{
	"proxy-list+http.txt+in:path",
	"free-proxy+http.txt+in:path",
	"proxy+list+http+in:path",
	"socks4.txt+in:path",
	"socks5.txt+in:path",
	"proxylist+http.txt+in:path",
}

var knownPaths = []string{
	"",
	"proxies",
	"proxy-list",
	"master",
	"main",
	"proxylist",
	"list",
	"data/latest/types/http",
	"data/latest/types/socks4",
	"data/latest/types/socks5",
	"checked_proxies",
}

var fileNames = []string{
	"http.txt",
	"https.txt",
	"socks4.txt",
	"socks5.txt",
	"all.txt",
	"HTTP.txt",
	"SOCKS4.txt",
	"SOCKS5.txt",
}

const outputFile = "github_sources.txt"

func main() {
	token := ""
	if len(os.Args) > 1 && !strings.HasPrefix(os.Args[1], "-") {
		token = os.Args[1]
	}

	fmt.Println("=== GitHub Proxy Hunter ===")
	fmt.Println("Searching for proxy list repositories...")
	if token == "" {
		fmt.Println("[!] No token — rate limit 60 req/h")
		fmt.Println()
	}

	cutoff := time.Now().Add(-24 * time.Hour)

	seen := map[string]bool{}
	var repos []ghRepo
	var mu sync.Mutex
	var wg sync.WaitGroup

	client := &http.Client{Timeout: 15 * time.Second}

	for _, q := range searchQueries {
		wg.Add(1)
		go func(query string) {
			defer wg.Done()
			url := fmt.Sprintf("https://api.github.com/search/repositories?q=%s&sort=updated&per_page=100", query)
			req, _ := http.NewRequest("GET", url, nil)
			req.Header.Set("Accept", "application/vnd.github.v3+json")
			req.Header.Set("User-Agent", "githunter/1.0")
			if token != "" {
				req.Header.Set("Authorization", "Bearer "+token)
			}
			resp, err := client.Do(req)
			if err != nil {
				fmt.Printf("[-] HTTP error for %s: %v\n", query, err)
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				body, _ := io.ReadAll(resp.Body)
				fmt.Printf("[-] GitHub API %d for %s: %s\n", resp.StatusCode, query, string(body)[:min(len(string(body)), 200)])
				return
			}
			body, _ := io.ReadAll(resp.Body)
			var sr searchResp
			if err := json.Unmarshal(body, &sr); err != nil {
				fmt.Printf("[-] JSON error for %s: %v\n", query, err)
				return
			}
			mu.Lock()
			for _, item := range sr.Items {
				updated, err := time.Parse(time.RFC3339, item.UpdatedAt)
				if err != nil || updated.Before(cutoff) {
					continue
				}
				if !seen[item.FullName] {
					seen[item.FullName] = true
					repos = append(repos, ghRepo{
						FullName:      item.FullName,
						DefaultBranch: item.DefaultBranch,
						HTMLURL:       item.HTMLURL,
						Stars:         item.Stars,
						UpdatedAt:     item.UpdatedAt,
					})
				}
			}
			mu.Unlock()
			fmt.Printf("[+] %s: %d repos\n", query, len(sr.Items))
		}(q)
	}
	wg.Wait()

	sort.Slice(repos, func(i, j int) bool {
		return repos[i].Stars > repos[j].Stars
	})

	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("=== GitHub Proxy Hunter === %s\n", time.Now().Format("2006-01-02 15:04:05")))
	sb.WriteString(fmt.Sprintf("Repos updated in last 24h: %d\n\n", len(repos)))
	sb.WriteString(fmt.Sprintf("=== HTTP ===\n"))
	sb.WriteString("\n")
	for _, r := range repos {
		for _, url := range genURLs(r, "http.txt", "https.txt", "HTTP.txt") {
			sb.WriteString(url + "\n")
		}
	}

	sb.WriteString(fmt.Sprintf("\n=== SOCKS4 ===\n"))
	sb.WriteString("\n")
	for _, r := range repos {
		for _, url := range genURLs(r, "socks4.txt", "SOCKS4.txt") {
			sb.WriteString(url + "\n")
		}
	}

	sb.WriteString(fmt.Sprintf("\n=== SOCKS5 ===\n"))
	sb.WriteString("\n")
	for _, r := range repos {
		for _, url := range genURLs(r, "socks5.txt", "SOCKS5.txt") {
			sb.WriteString(url + "\n")
		}
	}

	fmt.Printf("\n=== Found %d repos updated in last 24h ===\n", len(repos))
	fmt.Printf("[*] Saved to %s\n", outputFile)

	os.WriteFile(outputFile, []byte(sb.String()), 0644)

	fmt.Println()
	fmt.Println("Top repos by stars:")
	for i, r := range repos {
		if i >= 10 {
			break
		}
		fmt.Printf("  %s ⭐%d\n", r.FullName, r.Stars)
	}
}

func genURLs(r ghRepo, names ...string) []string {
	parts := strings.Split(r.FullName, "/")
	if len(parts) != 2 {
		return nil
	}
	owner, repo := parts[0], parts[1]
	base := fmt.Sprintf("https://raw.githubusercontent.com/%s/%s", owner, repo)
	var out []string
	for _, path := range knownPaths {
		for _, fn := range fileNames {
			for _, name := range names {
				if fn == name {
					url := base
					if path != "" {
						url += "/" + path
					}
					url += "/" + fn
					out = append(out, url)
				}
			}
		}
	}
	return out
}


