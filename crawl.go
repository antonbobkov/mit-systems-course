package main

import (
	"fmt"
	"sync"
	"errors"
	"time"
)

type Fetcher interface {
	// Fetch returns the body of URL and
	// a slice of URLs found on that page.
	Fetch(url string) (body string, urls []string, err error)
}

type UrlData struct {
	url string
	depth int
}


type CrawlData struct {
	mu sync.Mutex
	seen map[string]bool
	ongoing_visits map[string]bool
	to_visit []UrlData
}

func InitCrawlData() CrawlData {
	cd := CrawlData{}
	
	cd.seen = make(map[string]bool)
	cd.ongoing_visits = make(map[string]bool)
	cd.to_visit = make([]UrlData, 0)
	
	return cd
}

func (cd *CrawlData) AddUrl(url_data UrlData) {
	cd.mu.Lock()
	defer cd.mu.Unlock()
	
	if !cd.seen[url_data.url] {
		cd.seen[url_data.url] = true
		cd.to_visit = append(cd.to_visit, url_data)
	}
}

func (cd *CrawlData) GetWork() (url_data UrlData, wait bool, done bool){
	cd.mu.Lock()
	defer cd.mu.Unlock()
	
	if len(cd.to_visit) > 0 {
		// pop queue
		url_data, cd.to_visit = cd.to_visit[0], cd.to_visit[1:]
		cd.ongoing_visits[url_data.url] = true
		return url_data, /*wait=*/false, /*done=*/false
	} else if len(cd.ongoing_visits) > 0 {
		return url_data, /*wait=*/true, /*done=*/false
	} else {
		return url_data, /*wait=*/false, /*done=*/true
	}
}

func (cd *CrawlData) MarkWorkDone(url string) {
	if !cd.ongoing_visits[url] {
		panic(errors.New("bad ongoing visit"))
	}
	delete(cd.ongoing_visits, url)
}

func CrawlWorker(cd *CrawlData, fetcher Fetcher, num int) {
	fmt.Printf("Worker %v started\n", num)
	
	for {
		url_data, wait, done := cd.GetWork()
		if done { 
			fmt.Printf("Worker %v done\n", num)
			return 
		} else if wait { 
			fmt.Printf("Worker %v wait\n", num)
			time.Sleep(1 * time.Second) 
		} else {
			fmt.Printf("Worker %v fetching %v\n", num, url_data)
			CrawlOne(cd, fetcher, url_data)
		}
	}

}

func CrawlOne(cd *CrawlData, fetcher Fetcher, url_data UrlData) {
	defer cd.MarkWorkDone(url_data.url)
	
	if url_data.depth <= 0 { return }
	
	body, urls, err := fetcher.Fetch(url_data.url)
	
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("found: %s %q\n", url_data.url, body)
	
	for _, u := range urls {
		cd.AddUrl(UrlData{u, url_data.depth - 1})
	}
}

func main() {
	fmt.Println("main start")
	
	cd := InitCrawlData()
	cd.AddUrl(UrlData{"https://golang.org/", 4})
	
	for i := 0; i < 3; i++ { go CrawlWorker(&cd, fetcher, i) }
	
	fmt.Println("main sleep")
	
	time.Sleep(10 * time.Second) 
	
	fmt.Println("main done")
}

// fakeFetcher is Fetcher that returns canned results.
type fakeFetcher map[string]*fakeResult

type fakeResult struct {
	body string
	urls []string
}

func (f fakeFetcher) Fetch(url string) (string, []string, error) {
	time.Sleep(500 * time.Millisecond) 
	
	if res, ok := f[url]; ok {
		return res.body, res.urls, nil
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
