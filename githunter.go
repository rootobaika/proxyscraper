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
	FullName    string `json:"full_name"`
	DefaultBranch string `json:"default_branch"`
	HTMLURL     string `json:"html_url"`
	Stars       int    `json:"stargazers_count"`
	UpdatedAt   string `json:"updated_at"`
}

type searchResp struct {
	Items []struct {
		FullName    string `json:"full_name"`
		DefaultBranch string `json:"default_branch"`
		HTMLURL     string `json:"html_url"`
		Stars       int    `json:"stargazers_count"`
		UpdatedAt   string `json:"updated_at"`
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

func main() {
	token := ""
	if len(os.Args) > 1 && !strings.HasPrefix(os.Args[1], "-") {
		token = os.Args[1]
	}

	fmt.Println("=== GitHub Proxy Hunter ===")
	fmt.Println("Searching for proxy list repositories...")
	if token == "" {
		fmt.Println("[!] No token provided — rate limit 60 req/h. Use: githunter.exe YOUR_TOKEN")
		fmt.Println()
	}

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
				if !seen[item.FullName] {
					seen[item.FullName] = true
					repos = append(repos, ghRepo{
						FullName:    item.FullName,
						DefaultBranch: item.DefaultBranch,
						HTMLURL:     item.HTMLURL,
						Stars:       item.Stars,
						UpdatedAt:   item.UpdatedAt,
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

	fmt.Printf("\n=== Found %d unique repositories ===\n\n", len(repos))
	fmt.Println("HTTP sources:")
	fmt.Println("---")
	for _, r := range repos {
		parts := strings.Split(r.FullName, "/")
		if len(parts) != 2 {
			continue
		}
		owner, repo := parts[0], parts[1]
		branch := r.DefaultBranch
		if branch == "" {
			branch = "master"
		}
		base := fmt.Sprintf("https://raw.githubusercontent.com/%s/%s", owner, repo)
		for _, path := range knownPaths {
			for _, fn := range fileNames {
				url := base
				if path != "" {
					url += "/" + path
				}
				url += "/" + fn
				if strings.HasSuffix(fn, "http.txt") || strings.HasSuffix(fn, "https.txt") || fn == "HTTP.txt" {
					fmt.Println(url)
				}
			}
		}
	}

	fmt.Println("\nSOCKS4 sources:")
	fmt.Println("---")
	for _, r := range repos {
		parts := strings.Split(r.FullName, "/")
		if len(parts) != 2 {
			continue
		}
		owner, repo := parts[0], parts[1]
		branch := r.DefaultBranch
		if branch == "" {
			branch = "master"
		}
		base := fmt.Sprintf("https://raw.githubusercontent.com/%s/%s", owner, repo)
		for _, path := range knownPaths {
			for _, fn := range fileNames {
				url := base
				if path != "" {
					url += "/" + path
				}
				url += "/" + fn
				if strings.HasSuffix(fn, "socks4.txt") || fn == "SOCKS4.txt" {
					fmt.Println(url)
				}
			}
		}
	}

	fmt.Println("\nSOCKS5 sources:")
	fmt.Println("---")
	for _, r := range repos {
		parts := strings.Split(r.FullName, "/")
		if len(parts) != 2 {
			continue
		}
		owner, repo := parts[0], parts[1]
		branch := r.DefaultBranch
		if branch == "" {
			branch = "master"
		}
		base := fmt.Sprintf("https://raw.githubusercontent.com/%s/%s", owner, repo)
		for _, path := range knownPaths {
			for _, fn := range fileNames {
				url := base
				if path != "" {
					url += "/" + path
				}
				url += "/" + fn
				if strings.HasSuffix(fn, "socks5.txt") || fn == "SOCKS5.txt" {
					fmt.Println(url)
				}
			}
		}
	}

	fmt.Printf("\n=== DONE — %d repos found ===\n", len(repos))
}
