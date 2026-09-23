// Command loadgen drives an open-loop (fixed arrival rate) workload against a running
// deployment: by default 95% GET /{code} and 5% POST /shorten, with a Zipf-like hot set
// and a small fraction of unknown codes. Each stage holds one offered rate; latency is
// measured from the scheduled send time so a saturated target cannot hide queueing delay.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type stage struct {
	rate                        int
	ok, rejected, errs, skipped atomic.Int64
	redirectLat, createLat      []time.Duration
	mu                          sync.Mutex
	statuses                    map[int]int64
	elapsed                     time.Duration
	firstErr                    string
}

func main() {
	base := flag.String("base", "http://localhost:8080", "target base URL")
	rates := flag.String("rates", "200,500,1000,2000", "comma-separated offered requests/second, one stage each")
	dur := flag.Duration("stage", 30*time.Second, "duration of each stage")
	createRatio := flag.Float64("create-ratio", 0.05, "fraction of requests that are POST /shorten")
	unknownRatio := flag.Float64("unknown-ratio", 0.02, "fraction of redirects to a random unknown code")
	seed := flag.Int("seed", 500, "links created before the run; redirects draw from this pool")
	zipfS := flag.Float64("zipf", 1.1, "Zipf exponent for redirect popularity (>1)")
	maxInflight := flag.Int("max-inflight", 4096, "generator concurrency cap; requests over it are counted as skipped")
	timeout := flag.Duration("timeout", 3*time.Second, "per-request timeout")
	jsonOut := flag.String("json", "", "optional path to write per-stage results as JSON")
	flag.Parse()

	var stageRates []int
	for _, f := range strings.Split(*rates, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(f))
		if err != nil || n <= 0 {
			fatalf("invalid rate %q", f)
		}
		stageRates = append(stageRates, n)
	}
	if *createRatio < 0 || *createRatio >= 1 || *unknownRatio < 0 || *unknownRatio >= 1 || *zipfS <= 1 || *seed < 2 {
		fatalf("invalid ratio, zipf or seed setting")
	}
	root := strings.TrimRight(*base, "/")
	client := &http.Client{
		Timeout:       *timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		Transport: &http.Transport{
			MaxIdleConns: *maxInflight, MaxIdleConnsPerHost: *maxInflight, MaxConnsPerHost: *maxInflight,
			IdleConnTimeout: 30 * time.Second,
		},
	}

	codes := seedLinks(client, root, *seed)
	fmt.Printf("seeded %d links; mix: %.0f%% POST /shorten, %.0f%% GET /{code} (%.0f%% of GETs unknown); open-loop, %s per stage\n\n",
		len(codes), *createRatio*100, (1-*createRatio)*100, *unknownRatio*100, *dur)

	var results []*stage
	for _, rate := range stageRates {
		s := runStage(client, root, rate, *dur, *createRatio, *unknownRatio, *zipfS, codes, *maxInflight)
		results = append(results, s)
		printStage(s)
		time.Sleep(5 * time.Second) // let queues and in-flight work drain between stages
	}
	printSummary(results)
	if *jsonOut != "" {
		writeJSON(*jsonOut, results)
	}
}

func seedLinks(client *http.Client, root string, n int) []string {
	codes := make([]string, 0, n)
	for i := 0; len(codes) < n; i++ {
		if i > n*20 {
			fatalf("could not seed %d links; %d created (is the target healthy?)", n, len(codes))
		}
		code, status, err := create(client, root, fmt.Sprintf("https://example.com/seed/%d/%d", time.Now().UnixNano(), i))
		if err != nil || status == http.StatusTooManyRequests {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		if status != http.StatusCreated {
			fatalf("seed create returned status %d", status)
		}
		codes = append(codes, code)
	}
	return codes
}

func create(client *http.Client, root, target string) (string, int, error) {
	body, _ := json.Marshal(map[string]string{"url": target})
	req, err := http.NewRequest(http.MethodPost, root+"/shorten", bytes.NewReader(body))
	if err != nil {
		return "", 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", resp.StatusCode, err
	}
	var out struct {
		Code string `json:"code"`
	}
	if resp.StatusCode == http.StatusCreated {
		if err := json.Unmarshal(raw, &out); err != nil {
			return "", resp.StatusCode, err
		}
	}
	return out.Code, resp.StatusCode, nil
}

func runStage(client *http.Client, root string, rate int, dur time.Duration, createRatio, unknownRatio, zipfS float64, codes []string, maxInflight int) *stage {
	s := &stage{rate: rate, statuses: map[int]int64{}}
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	zipf := rand.NewZipf(rng, zipfS, 1, uint64(len(codes)-1))
	sem := make(chan struct{}, maxInflight)
	var wg sync.WaitGroup
	start := time.Now()
	total := int64(float64(rate) * dur.Seconds())
	const tick = time.Millisecond
	var dispatched int64
	for dispatched < total {
		due := int64(float64(rate) * time.Since(start).Seconds())
		if due > total {
			due = total
		}
		for ; dispatched < due; dispatched++ {
			scheduled := start.Add(time.Duration(float64(dispatched) / float64(rate) * float64(time.Second)))
			isCreate := rng.Float64() < createRatio
			var target string
			if !isCreate {
				if rng.Float64() < unknownRatio {
					target = "/" + randomCode(rng)
				} else {
					target = "/" + codes[zipf.Uint64()]
				}
			}
			select {
			case sem <- struct{}{}:
			default:
				s.skipped.Add(1)
				continue
			}
			wg.Add(1)
			go func(scheduled time.Time, isCreate bool, target string) {
				defer wg.Done()
				defer func() { <-sem }()
				s.do(client, root, scheduled, isCreate, target)
			}(scheduled, isCreate, target)
		}
		time.Sleep(tick)
	}
	wg.Wait()
	s.elapsed = time.Since(start)
	return s
}

func (s *stage) do(client *http.Client, root string, scheduled time.Time, isCreate bool, target string) {
	var status int
	var err error
	if isCreate {
		_, status, err = create(client, root, "https://example.com/load/"+randomCodeSafe()+"?t="+strconv.FormatInt(time.Now().UnixNano(), 36))
	} else {
		var resp *http.Response
		resp, err = client.Get(root + target)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			status = resp.StatusCode
		}
	}
	lat := time.Since(scheduled)
	s.mu.Lock()
	s.statuses[status]++
	if isCreate {
		s.createLat = append(s.createLat, lat)
	} else {
		s.redirectLat = append(s.redirectLat, lat)
	}
	if err != nil && s.firstErr == "" {
		s.firstErr = err.Error()
	}
	s.mu.Unlock()
	switch {
	case err != nil:
		s.errs.Add(1)
	case status == http.StatusTooManyRequests:
		s.rejected.Add(1)
	case isCreate && status == http.StatusCreated, !isCreate && (status == http.StatusFound || status == http.StatusNotFound):
		s.ok.Add(1)
	default:
		s.errs.Add(1)
	}
}

const alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

func randomCode(r *rand.Rand) string {
	b := make([]byte, 10)
	for i := range b {
		b[i] = alphabet[r.Intn(len(alphabet))]
	}
	return string(b)
}

func randomCodeSafe() string { return randomCode(rand.New(rand.NewSource(time.Now().UnixNano()))) }

func pct(d []time.Duration, p float64) time.Duration {
	if len(d) == 0 {
		return 0
	}
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	return d[min(len(d)-1, int(float64(len(d))*p))]
}

func (s *stage) done() int64 { return s.ok.Load() + s.rejected.Load() + s.errs.Load() }

func (s *stage) achieved() float64 { return float64(s.done()) / s.elapsed.Seconds() }

func (s *stage) pctOf(n int64) float64 {
	if d := s.done() + s.skipped.Load(); d > 0 {
		return float64(n) / float64(d) * 100
	}
	return 0
}

func printStage(s *stage) {
	fmt.Printf("offered %d rps -> achieved %.0f rps | redirect p50/p95/p99 %s/%s/%s | create p50/p95/p99 %s/%s/%s | ok %.2f%% 429 %.2f%% err %.2f%% generator-skipped %d | statuses %v\n",
		s.rate, s.achieved(),
		pct(s.redirectLat, .5).Round(time.Microsecond), pct(s.redirectLat, .95).Round(time.Microsecond), pct(s.redirectLat, .99).Round(time.Microsecond),
		pct(s.createLat, .5).Round(time.Microsecond), pct(s.createLat, .95).Round(time.Microsecond), pct(s.createLat, .99).Round(time.Microsecond),
		s.pctOf(s.ok.Load()), s.pctOf(s.rejected.Load()), s.pctOf(s.errs.Load()), s.skipped.Load(), s.statuses)
	if s.firstErr != "" {
		fmt.Printf("  first transport error: %s\n", s.firstErr)
	}
}

func printSummary(stages []*stage) {
	fmt.Printf("\n| Offered RPS | Achieved RPS | Redirect p50 | p95 | p99 | Create p95 | OK %% | 429 %% | Error %% |\n|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, s := range stages {
		fmt.Printf("| %d | %.0f | %s | %s | %s | %s | %.2f | %.2f | %.2f |\n", s.rate, s.achieved(),
			pct(s.redirectLat, .5).Round(time.Microsecond), pct(s.redirectLat, .95).Round(time.Microsecond), pct(s.redirectLat, .99).Round(time.Microsecond),
			pct(s.createLat, .95).Round(time.Microsecond), s.pctOf(s.ok.Load()), s.pctOf(s.rejected.Load()), s.pctOf(s.errs.Load()+s.skipped.Load()))
	}
}

func writeJSON(path string, stages []*stage) {
	type row struct {
		Offered, Skipped                                 int
		Achieved, OKPct, RejectedPct, ErrorPct           float64
		RedirectP50, RedirectP95, RedirectP99, CreateP95 string
	}
	rows := make([]row, 0, len(stages))
	for _, s := range stages {
		rows = append(rows, row{s.rate, int(s.skipped.Load()), s.achieved(), s.pctOf(s.ok.Load()), s.pctOf(s.rejected.Load()), s.pctOf(s.errs.Load()),
			pct(s.redirectLat, .5).String(), pct(s.redirectLat, .95).String(), pct(s.redirectLat, .99).String(), pct(s.createLat, .95).String()})
	}
	raw, _ := json.MarshalIndent(rows, "", "  ")
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		fatalf("write %s: %v", path, err)
	}
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "loadgen: "+format+"\n", args...)
	os.Exit(1)
}
