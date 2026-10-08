// loadtest runs isolated fixtures and an open-loop client, never real providers.
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/csv"
	"encoding/json"
	"encoding/pem"
	"flag"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

const answer = `{"id":"fixture","object":"chat.completion","model":"fake-model","choices":[{"index":0,"message":{"role":"assistant","content":"hello"},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}`

func main() {
	if err := execute(); err != nil {
		log.Fatal(err)
	}
}

func execute() error {
	if len(os.Args) < 2 {
		return fmt.Errorf("expected cert, fake, check, run, or summarize")
	}
	switch os.Args[1] {
	case "cert":
		return certificate()
	case "fake":
		mux := http.NewServeMux()
		mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "ok") })
		mux.HandleFunc("POST /v1/chat/completions", fakeChat)
		return (&http.Server{Addr: ":8443", Handler: mux, ReadHeaderTimeout: 5 * time.Second}).ListenAndServeTLS("/certs/cert.pem", "/certs/key.pem")
	case "check":
		c, err := client()
		if err != nil {
			return err
		}
		r, err := c.Get("https://127.0.0.1:8443/health")
		if err != nil {
			return err
		}
		defer r.Body.Close()
		if r.StatusCode != 200 {
			return fmt.Errorf("fixture health %d", r.StatusCode)
		}
		return nil
	case "run":
		return run()
	case "summarize":
		return summarize()
	}
	return fmt.Errorf("unknown command")
}

func certificate() error {
	// Reuse this test-only CA so existing containers keep trusting the same key.
	if pair, err := tls.LoadX509KeyPair("/certs/cert.pem", "/certs/key.pem"); err == nil {
		cert, err := x509.ParseCertificate(pair.Certificate[0])
		if err == nil && time.Until(cert.NotAfter) > 24*time.Hour {
			return nil
		}
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return err
	}
	cert := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: "Janus load fixture"}, DNSNames: []string{"fake-provider", "localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(30 * 24 * time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	if err != nil {
		return err
	}
	priv, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return err
	}
	if err = os.WriteFile("/certs/cert.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0644); err != nil {
		return err
	}
	if err = os.WriteFile("/certs/key.pem", pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: priv}), 0600); err != nil {
		return err
	}
	return os.Chown("/certs/key.pem", 10001, 10001)
}

func fakeChat(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Stream bool `json:"stream"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&body); err != nil {
		http.Error(w, "invalid fixture input", 400)
		return
	}
	pause := func(d time.Duration) bool {
		select {
		case <-time.After(d):
			return true
		case <-r.Context().Done():
			return false
		}
	}
	if !body.Stream {
		if !pause(10 * time.Millisecond) {
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, answer)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	f := w.(http.Flusher)
	fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\n")
	f.Flush()
	if !pause(3 * time.Millisecond) {
		return
	}
	fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n")
	f.Flush()
	if !pause(7 * time.Millisecond) {
		return
	}
	fmt.Fprint(w, "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5,\"total_tokens\":15}}\n\ndata: [DONE]\n\n")
	f.Flush()
}

func client() (*http.Client, error) {
	cert, err := os.ReadFile("/certs/cert.pem")
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(cert) {
		return nil, fmt.Errorf("invalid fixture CA")
	}
	return &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}, MaxIdleConns: 512, MaxIdleConnsPerHost: 256}}, nil
}

type sample struct {
	Latency, TTFT, Lag float64
	Status             int
	Error              string
}
type phase struct {
	Rejected                                                 int
	Name                                                     string `json:"name"`
	Rate                                                     int    `json:"target_rps"`
	Start, End                                               time.Time
	Scheduled, Success, Errors, Dropped                      int
	Throughput, P50MS, P99MS, TTFTP50MS, TTFTP99MS, LagP99MS float64
	Backlog                                                  int64
	MetricsBefore, MetricsAfter                              map[string]float64
	CPUAverage, CPUMax, MemoryMaxMiB                         float64
	ResourceSamples                                          int
	StoredRequests, StoredAttempts, StoredTokens             int64
	BacklogMax                                               int64
	ValidationErrors                                         []string
}
type report struct {
	AllowOverload                bool
	DurationSeconds, MaxInflight int
	Phases                       []phase
	Completed, Passed            bool
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
func percentile(v []float64, p float64) float64 {
	if len(v) == 0 {
		return 0
	}
	sort.Float64s(v)
	i := int(float64(len(v)-1) * p)
	return v[i]
}

// A TTFT observation requires a complete SSE event with visible content.
// Role-only events and partial network chunks do not count as a token.
func consumeSSE(r io.Reader, onToken func()) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	var data []string
	var content strings.Builder
	done := false
	first := true
	dispatch := func() error {
		if len(data) == 0 {
			return nil
		}
		payload := strings.Join(data, "\n")
		data = nil
		if done {
			return fmt.Errorf("event after DONE")
		}
		if payload == "[DONE]" {
			done = true
			return nil
		}
		var event struct {
			Choices []struct {
				Delta struct {
					Content string `json:"content"`
				} `json:"delta"`
			} `json:"choices"`
		}
		if err := json.Unmarshal([]byte(payload), &event); err != nil {
			return fmt.Errorf("invalid SSE JSON")
		}
		for _, c := range event.Choices {
			if c.Delta.Content != "" {
				if first {
					onToken()
					first = false
				}
				content.WriteString(c.Delta.Content)
			}
		}
		return nil
	}
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if err := dispatch(); err != nil {
				return err
			}
		} else if strings.HasPrefix(line, "data:") {
			data = append(data, strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if len(data) > 0 || !done || content.String() != "hello" {
		return fmt.Errorf("incomplete or unexpected SSE response")
	}
	return nil
}

func request(c *http.Client, url string, stream bool, scheduled time.Time) sample {
	start := time.Now()
	s := sample{Lag: ms(start.Sub(scheduled))}
	payload := fmt.Sprintf(`{"model":"fake-model","messages":[{"role":"user","content":"say hello"}],"max_completion_tokens":64,"stream":%t}`, stream)
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(payload))
	if err != nil {
		s.Error = "request"
		return s
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer fixture-client-key")
	resp, err := c.Do(req)
	if err != nil {
		s.Error = "transport"
		s.Latency = ms(time.Since(scheduled))
		return s
	}
	defer resp.Body.Close()
	s.Status = resp.StatusCode
	if resp.StatusCode == 503 {
		data, e := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		var body struct {
			Error struct {
				Type string `json:"type"`
			} `json:"error"`
		}
		if e == nil && json.Unmarshal(data, &body) == nil && body.Error.Type == "overloaded_error" && resp.Header.Get("Retry-After") == "1" {
			s.Error = "overloaded"
		} else {
			s.Error = "http"
		}
	} else if resp.StatusCode != 200 {
		io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
		s.Error = "http"
	} else if stream {
		err = consumeSSE(resp.Body, func() { s.TTFT = ms(time.Since(scheduled)) })
		if err != nil {
			s.Error = "sse"
		}
	} else {
		b, e := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		var body struct {
			Choices []struct{ Message struct{ Content string } }
			Usage   struct {
				TotalTokens int `json:"total_tokens"`
			}
		}
		if e != nil || json.Unmarshal(b, &body) != nil || len(body.Choices) != 1 || body.Choices[0].Message.Content != "hello" || body.Usage.TotalTokens != 15 {
			s.Error = "json"
		}
	}
	s.Latency = ms(time.Since(scheduled))
	return s
}

func metrics(c *http.Client) (map[string]float64, error) {
	req, _ := http.NewRequest(http.MethodGet, "http://gateway:8080/metrics", nil)
	req.Header.Set("Authorization", "Bearer fixture-client-key")
	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("metrics status %d", resp.StatusCode)
	}
	m := map[string]float64{}
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		f := strings.Fields(sc.Text())
		if len(f) == 2 && strings.HasPrefix(f[0], "janus_") && !strings.Contains(f[0], "{") {
			v, e := strconv.ParseFloat(f[1], 64)
			if e == nil {
				m[f[0]] = v
			}
		}
	}
	return m, sc.Err()
}

func settled(c *http.Client, queue *redis.Client) (map[string]float64, int64, error) {
	deadline := time.Now().Add(30 * time.Second)
	for {
		m, err := metrics(c)
		if err != nil {
			return nil, 0, err
		}
		n, err := queue.XLen(context.Background(), "janus:usage:v1").Result()
		if err != nil {
			return nil, 0, err
		}
		if m["janus_chat_inflight"] == 0 && m["janus_admission_active"] == 0 && m["janus_usage_pending_handoffs"] == 0 && m["janus_outbox_records"] == 0 && n == 0 && m["janus_usage_persisted_total"] >= m["janus_usage_enqueued_total"] {
			return m, n, nil
		}
		if time.Now().After(deadline) {
			return m, n, fmt.Errorf("usage did not drain in 30 seconds")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func run() error {
	flags := flag.NewFlagSet("run", flag.ContinueOnError)
	duration := flags.Int("duration", 10, "seconds per phase")
	max := flags.Int("inflight", 256, "maximum client concurrency")
	ratesText := flags.String("rates", "50,200,500", "requests per second")
	allowOverload := flags.Bool("allow-overload", false, "allow deliberate gateway 503 rejections; still validate every admitted response and usage record")
	if err := flags.Parse(os.Args[2:]); err != nil {
		return err
	}
	if *duration < 1 || *duration > 300 || *max < 1 || *max > 4096 {
		return fmt.Errorf("duration/inflight out of range")
	}
	var rates []int
	for _, s := range strings.Split(*ratesText, ",") {
		n, e := strconv.Atoi(s)
		if e != nil || n < 1 || n > 10000 {
			return fmt.Errorf("invalid rate")
		}
		rates = append(rates, n)
	}
	c, err := client()
	if err != nil {
		return err
	}
	queue := redis.NewClient(&redis.Options{Addr: "usage-redis:6379"})
	defer queue.Close()
	db, err := pgxpool.New(context.Background(), "postgres://janus:fixture-password@postgres:5432/janus?sslmode=disable")
	if err != nil {
		return err
	}
	defer db.Close()
	raw, err := os.Create("/reports/samples.csv")
	if err != nil {
		return err
	}
	defer raw.Close()
	csvw := csv.NewWriter(raw)
	defer csvw.Flush()
	csvw.Write([]string{"phase", "target_rps", "scheduled_latency_ms", "scheduled_ttft_ms", "scheduler_lag_ms", "status", "error"})
	result := report{DurationSeconds: *duration, MaxInflight: *max, AllowOverload: *allowOverload}
	if err := save(result); err != nil {
		return err
	}
	failed := false
	for _, rate := range rates {
		for _, scenario := range []struct {
			name, url string
			stream    bool
		}{{"direct_json", "https://fake-provider:8443/v1/chat/completions", false}, {"gateway_json", "http://gateway:8080/v1/chat/completions", false}, {"direct_sse", "https://fake-provider:8443/v1/chat/completions", true}, {"gateway_sse", "http://gateway:8080/v1/chat/completions", true}} {
			for i := 0; i < 20; i++ {
				s := request(c, scenario.url, scenario.stream, time.Now())
				if s.Error != "" {
					return fmt.Errorf("warmup failed: %s %s status=%d", scenario.name, s.Error, s.Status)
				}
			}
			before, _, e := settled(c, queue)
			if e != nil {
				return e
			}
			storedBefore, e := stored(db)
			if e != nil {
				return e
			}
			p := phase{Name: scenario.name, Rate: rate, Scheduled: rate * *duration, MetricsBefore: before}
			samples := make([]sample, p.Scheduled)
			slots := make(chan struct{}, *max)
			var wg sync.WaitGroup
			p.Start = time.Now().Add(20 * time.Millisecond)
			for i := 0; i < p.Scheduled; i++ {
				scheduled := p.Start.Add(time.Duration(i) * time.Second / time.Duration(rate))
				if wait := time.Until(scheduled); wait > 0 {
					time.Sleep(wait)
				}
				select {
				case slots <- struct{}{}:
					wg.Add(1)
					go func(i int, t time.Time) {
						defer wg.Done()
						defer func() { <-slots }()
						samples[i] = request(c, scenario.url, scenario.stream, t)
					}(i, scheduled)
				default:
					samples[i] = sample{Error: "inflight_limit", Lag: ms(time.Since(scheduled))}
					p.Dropped++
				}
			}
			wg.Wait()
			p.End = time.Now()
			backlog, e := queue.XLen(context.Background(), "janus:usage:v1").Result()
			if e != nil {
				return e
			}
			p.Backlog = backlog
			after, _, e := settled(c, queue)
			p.MetricsAfter = after
			if e != nil {
				failed = true
				fmt.Println(e)
				p.ValidationErrors = append(p.ValidationErrors, e.Error())
			}
			var latency, ttft, lag []float64
			for _, s := range samples {
				lag = append(lag, s.Lag)
				if s.Error == "overloaded" && strings.HasPrefix(p.Name, "gateway") {
					p.Rejected++
					if !*allowOverload {
						p.Errors++
					}
				} else if s.Error != "" {
					p.Errors++
				} else {
					p.Success++
					latency = append(latency, s.Latency)
					if scenario.stream {
						ttft = append(ttft, s.TTFT)
					}
				}
				csvw.Write([]string{p.Name, strconv.Itoa(rate), fmt.Sprintf("%.3f", s.Latency), fmt.Sprintf("%.3f", s.TTFT), fmt.Sprintf("%.3f", s.Lag), strconv.Itoa(s.Status), s.Error})
			}
			csvw.Flush()
			if err := csvw.Error(); err != nil {
				return err
			}
			p.P50MS = percentile(latency, .5)
			p.P99MS = percentile(latency, .99)
			p.TTFTP50MS = percentile(ttft, .5)
			p.TTFTP99MS = percentile(ttft, .99)
			p.LagP99MS = percentile(lag, .99)
			p.Throughput = float64(p.Success) / p.End.Sub(p.Start).Seconds()
			storedAfter, e := stored(db)
			if e != nil {
				return e
			}
			p.StoredRequests = storedAfter[0] - storedBefore[0]
			p.StoredAttempts = storedAfter[1] - storedBefore[1]
			p.StoredTokens = storedAfter[2] - storedBefore[2]
			expected := int64(0)
			if strings.HasPrefix(p.Name, "gateway") {
				expected = int64(p.Success)
			}
			if p.StoredRequests != expected || p.StoredAttempts != expected || p.StoredTokens != expected*15 {
				failed = true
				fmt.Println("PostgreSQL request/token totals mismatch")
				p.ValidationErrors = append(p.ValidationErrors, "PostgreSQL request/token totals mismatch")
			}
			if p.Errors > 0 {
				failed = true
				p.ValidationErrors = append(p.ValidationErrors, fmt.Sprintf("%d response errors/drops", p.Errors))
			}
			if strings.HasPrefix(p.Name, "gateway") && (p.Success == 0 || after["janus_overload_rejections_total"]-before["janus_overload_rejections_total"] != float64(p.Rejected)) {
				failed = true
				p.ValidationErrors = append(p.ValidationErrors, "no admitted responses or overload counter mismatch")
			}
			for _, key := range []string{"janus_usage_enqueue_errors_total", "janus_usage_worker_errors_total", "janus_usage_invalid_events_total", "janus_outbox_errors_total"} {
				if after[key] > before[key] {
					failed = true
					fmt.Printf("%s increased\n", key)
					p.ValidationErrors = append(p.ValidationErrors, key+" increased")
				}
			}
			if strings.HasPrefix(p.Name, "gateway") && after["janus_usage_enqueued_total"]-before["janus_usage_enqueued_total"] != float64(p.Success) {
				failed = true
				fmt.Println("usage handoff count mismatch")
				p.ValidationErrors = append(p.ValidationErrors, "usage handoff count mismatch")
			}
			if strings.HasPrefix(p.Name, "gateway") && (after["janus_outbox_operations_total"]-before["janus_outbox_operations_total"] != 2*float64(p.Success) || after["janus_outbox_writes_total"]-before["janus_outbox_writes_total"] != float64(p.Success)) {
				failed = true
				p.ValidationErrors = append(p.ValidationErrors, "journal save/ack operation count mismatch")
			}
			result.Phases = append(result.Phases, p)
			fmt.Printf("%s %d rps: %d/%d successful, %d rejected, p50 %.2f ms p99 %.2f ms, backlog %d\n", p.Name, rate, p.Success, p.Scheduled, p.Rejected, p.P50MS, p.P99MS, p.Backlog)
			if err := save(result); err != nil {
				return err
			}
		}
	}
	result.Completed = true
	result.Passed = !failed
	if err := save(result); err != nil {
		return err
	}
	if failed {
		return fmt.Errorf("load test failed; reports retained")
	}
	return nil
}

func stored(db *pgxpool.Pool) ([3]int64, error) {
	var totals [3]int64
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := db.QueryRow(ctx, `SELECT (SELECT count(*) FROM janus_usage_requests), count(*), COALESCE(sum(total_tokens),0) FROM janus_usage_attempts`).Scan(&totals[0], &totals[1], &totals[2])
	return totals, err
}

func save(r report) error {
	b, e := json.MarshalIndent(r, "", "  ")
	if e != nil {
		return e
	}
	return os.WriteFile("/reports/results.json", b, 0644)
}

func memoryMiB(s string) (float64, error) {
	fields := strings.Fields(s)
	if len(fields) == 0 {
		return 0, fmt.Errorf("empty memory")
	}
	value := fields[0]
	for _, u := range []struct {
		name   string
		factor float64
	}{{"GiB", 1024}, {"MiB", 1}, {"KiB", 1.0 / 1024}, {"GB", 1e9 / (1 << 20)}, {"MB", 1e6 / (1 << 20)}, {"kB", 1e3 / (1 << 20)}, {"B", 1.0 / (1 << 20)}} {
		if strings.HasSuffix(value, u.name) {
			n, e := strconv.ParseFloat(strings.TrimSuffix(value, u.name), 64)
			return n * u.factor, e
		}
	}
	return 0, fmt.Errorf("unrecognized memory unit")
}

func summarize() error {
	b, e := os.ReadFile("/reports/results.json")
	if e != nil {
		return e
	}
	var r report
	if e = json.Unmarshal(b, &r); e != nil {
		return e
	}
	// Rebuilding the report must not accumulate observations from a prior run
	// of summarize against this same results file.
	for i := range r.Phases {
		p := &r.Phases[i]
		p.ResourceSamples, p.CPUAverage, p.CPUMax, p.MemoryMaxMiB, p.BacklogMax = 0, 0, 0, 0, 0
	}
	raw, e := os.Open("/reports/resources.jsonl")
	if e != nil {
		return e
	}
	defer raw.Close()
	sc := bufio.NewScanner(raw)
	for sc.Scan() {
		var entry struct {
			At      time.Time                          `json:"at"`
			Backlog int64                              `json:"backlog"`
			Stats   struct{ CPUPerc, MemUsage string } `json:"stats"`
		}
		if e = json.Unmarshal(sc.Bytes(), &entry); e != nil {
			return e
		}
		cpu, e := strconv.ParseFloat(strings.TrimSuffix(entry.Stats.CPUPerc, "%"), 64)
		if e != nil {
			return e
		}
		mem, e := memoryMiB(entry.Stats.MemUsage)
		if e != nil {
			return e
		}
		for i := range r.Phases {
			p := &r.Phases[i]
			if !entry.At.Before(p.Start) && !entry.At.After(p.End) {
				p.ResourceSamples++
				p.CPUAverage += cpu
				if entry.Backlog > p.BacklogMax {
					p.BacklogMax = entry.Backlog
				}
				if cpu > p.CPUMax {
					p.CPUMax = cpu
				}
				if mem > p.MemoryMaxMiB {
					p.MemoryMaxMiB = mem
				}
			}
		}
	}
	if e = sc.Err(); e != nil {
		return e
	}
	var out bytes.Buffer
	fmt.Fprintf(&out, "# Separate-container load test\n\n%d seconds per phase; max %d inflight. Latency and TTFT include client scheduling lag. Failed requests are excluded from percentiles and counted separately.\n\n", r.DurationSeconds, r.MaxInflight)
	fmt.Fprintf(&out, "Completed: %t. Passed all response and usage checks: %t. Deliberate gateway overload rejections allowed: %t.\n\n", r.Completed, r.Passed, r.AllowOverload)
	fmt.Fprintln(&out, "| Phase | Target RPS | Successful / scheduled | Actual RPS | p50 / p99 ms | TTFT p50 / p99 ms | Lag p99 ms | CPU avg / max % | Memory max MiB | Samples | Backlog sampled max / EOF |\n|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
	for i := range r.Phases {
		p := &r.Phases[i]
		if p.ResourceSamples > 0 {
			p.CPUAverage /= float64(p.ResourceSamples)
		}
		fmt.Fprintf(&out, "| %s | %d | %d / %d | %.1f | %.2f / %.2f | %.2f / %.2f | %.2f | %.1f / %.1f | %.1f | %d | %d / %d |\n", p.Name, p.Rate, p.Success, p.Scheduled, p.Throughput, p.P50MS, p.P99MS, p.TTFTP50MS, p.TTFTP99MS, p.LagP99MS, p.CPUAverage, p.CPUMax, p.MemoryMaxMiB, p.ResourceSamples, p.BacklogMax, p.Backlog)
	}
	for _, p := range r.Phases {
		if p.Rejected > 0 {
			fmt.Fprintf(&out, "\n%s at %d RPS: %d deliberate overload rejections (excluded from successful latency percentiles).\n", p.Name, p.Rate, p.Rejected)
		}
		if len(p.ValidationErrors) > 0 {
			fmt.Fprintf(&out, "\n%s at %d RPS: %s.\n", p.Name, p.Rate, strings.Join(p.ValidationErrors, "; "))
		}
	}
	fmt.Fprintln(&out, "\nCPU/memory are coarse Docker stats observations of the gateway only. Memory excludes reclaimable cache; it is not Go heap or a true peak. Direct phases observe an idle gateway. Fixtures, client, databases and gateway share the WSL host. This is a local baseline, not a production capacity guarantee.")
	if e = save(r); e != nil {
		return e
	}
	return os.WriteFile("/reports/results.md", out.Bytes(), 0644)
}
