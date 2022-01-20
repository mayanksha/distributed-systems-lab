package main

import (
	"fmt"
	"sync"
)

type Fetcher interface {
	// Fetch returns the body of URL and
	// a slice of URLs found on that page.
	Fetch(url string) (body string, urls []string, err error)
}

type UrlResult struct {
	body string
	isFound bool
}

type ConcurrentMap struct {
	value map[string]UrlResult
	mutex sync.Mutex
}

var concMap = ConcurrentMap{
	value: make(map[string]UrlResult),
	mutex: sync.Mutex{},
}

// Crawl uses fetcher to recursively crawl
// pages starting with url, to a maximum of depth.
func Crawl(url string, depth int, fetcher Fetcher, wg *sync.WaitGroup) {
	defer wg.Done()

	// TODO: Fetch URLs in parallel.
	// TODO: Don't fetch the same URL twice.
	// This implementation doesn't do either:
	if depth <= 0 {
		return
	}

	_, urls, err := fetcher.Fetch(url)
	if err != nil {
		return
	}

	for _, u := range urls {
		//concMap.mutex.Lock()
		_, ok := concMap.value[u]
		//concMap.mutex.Unlock()

		if (!ok) {
			wg.Add(1)
			go Crawl(u, depth-1, fetcher, wg)
		}
	}
}

func main() {
	var wg sync.WaitGroup

	wg.Add(1)
	Crawl("https://golang.org/", 4, fetcher, &wg)
	wg.Wait()

	for k, v := range concMap.value {
		if (v.isFound) {
			fmt.Printf("found url\t: %v, body: %v\n", k, v)
		} else {
			fmt.Printf("missing url\t: %v\n", k)
		}
	}
}

// fakeFetcher is Fetcher that returns canned results.
type fakeFetcher map[string]*fakeResult

type fakeResult struct {
	body string
	urls []string
}

func (f fakeFetcher) Fetch(url string) (string, []string, error) {
	if res, ok := f[url]; ok {
		concMap.mutex.Lock()
		defer concMap.mutex.Unlock()

		_, mok := concMap.value[url]
		if (!mok) {
			concMap.value[url] = UrlResult{
				body:    res.body,
				isFound: true,
			}
		}
		return res.body, res.urls, nil
	} else {
		concMap.mutex.Lock()
		defer concMap.mutex.Unlock()
		concMap.value[url] = UrlResult{
			body:    "",
			isFound: false,
		}
	}
	return "", nil, fmt.Errorf("not found: %s", url)
}

// fetcher is a populated fakeFetcher.
var fetcher = fakeFetcher{
	"https://golang.org/": &fakeResult{
		"The Go Programming Language",
		[]string{
			"https://golang.org/pkg/",
			"https://golang.org/cmd/",
		},
	},
	"https://golang.org/pkg/": &fakeResult{
		"Packages",
		[]string{
			"https://golang.org/",
			"https://golang.org/cmd/",
			"https://golang.org/pkg/fmt/",
			"https://golang.org/pkg/os/",
		},
	},
	"https://golang.org/pkg/fmt/": &fakeResult{
		"Package fmt",
		[]string{
			"https://golang.org/",
			"https://golang.org/pkg/",
		},
	},
	"https://golang.org/pkg/os/": &fakeResult{
		"Package os",
		[]string{
			"https://golang.org/",
			"https://golang.org/pkg/",
		},
	},
}
