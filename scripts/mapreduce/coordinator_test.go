package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeWorker is a tiny stand-in for a real worker. It records calls and lets
// tests script behaviour (e.g. fail the next /reduce, die after /map).
type fakeWorker struct {
	mu sync.Mutex

	failNext       map[string]int // path -> times to fail with 500
	failPeerReduce string         // if non-empty, /reduce returns "fetch from peer X: died"
	stopAfter      map[string]int // path -> after N successes, srv.Close()
	pathCounts     map[string]int // observed counts per path
	slotData       map[int]*fakeSlot
	srv            *httptest.Server // injected after construction
	loaded         bool
	mapped         bool
	reduced        bool
}

type fakeSlot struct {
	urls   []string
	peers  []string
	result []KeyValue
}

func newFakeWorker() *fakeWorker {
	return &fakeWorker{
		failNext:   make(map[string]int),
		stopAfter:  make(map[string]int),
		pathCounts: make(map[string]int),
		slotData:   make(map[int]*fakeSlot),
	}
}

func (w *fakeWorker) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(rw http.ResponseWriter, _ *http.Request) {
		rw.WriteHeader(200)
		rw.Write([]byte("ok"))
	})
	mux.HandleFunc("POST /load", w.wrap("/load", func(rw http.ResponseWriter, r *http.Request) {
		var req loadRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		w.mu.Lock()
		slot := w.slotLocked(req.ID)
		slot.urls = append([]string(nil), req.URLs...)
		w.loaded = true
		w.mu.Unlock()
		rw.WriteHeader(200)
	}))
	mux.HandleFunc("POST /data", w.wrap("/data", func(rw http.ResponseWriter, r *http.Request) {
		slotID, _ := strconv.Atoi(r.URL.Query().Get("slot"))
		_, _ = io.ReadAll(r.Body)
		w.mu.Lock()
		w.slotLocked(slotID)
		w.loaded = true
		w.mu.Unlock()
		rw.WriteHeader(200)
	}))
	mux.HandleFunc("POST /map", w.wrap("/map", func(rw http.ResponseWriter, r *http.Request) {
		var req mapRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		w.mu.Lock()
		slot := w.slotLocked(req.ID)
		slot.peers = append([]string(nil), req.Peers...)
		w.mapped = true
		w.mu.Unlock()
		rw.WriteHeader(200)
	}))
	mux.HandleFunc("POST /reduce", w.wrap("/reduce", func(rw http.ResponseWriter, r *http.Request) {
		var body reduceRequest
		_ = json.NewDecoder(r.Body).Decode(&body)
		w.mu.Lock()
		slot := w.slotLocked(body.ID)
		if len(body.Peers) > 0 {
			slot.peers = append([]string(nil), body.Peers...)
		}
		failPeer := w.failPeerReduce
		w.mu.Unlock()
		if failPeer != "" {
			http.Error(rw, fmt.Sprintf("fetch from peer %s: died", failPeer), 500)
			return
		}
		w.mu.Lock()
		w.reduced = true
		w.mu.Unlock()
		rw.WriteHeader(200)
	}))
	mux.HandleFunc("GET /result", w.wrap("/result", func(rw http.ResponseWriter, r *http.Request) {
		slotID, _ := strconv.Atoi(r.URL.Query().Get("slot"))
		w.mu.Lock()
		results := append([]KeyValue(nil), w.slotLocked(slotID).result...)
		w.mu.Unlock()
		rw.WriteHeader(200)
		for _, kv := range results {
			fmt.Fprintf(rw, "%s\t%s\n", kv.Key, kv.Value)
		}
	}))
	return mux
}

func (w *fakeWorker) slotLocked(slotID int) *fakeSlot {
	slot := w.slotData[slotID]
	if slot == nil {
		slot = &fakeSlot{
			result: []KeyValue{{Key: fmt.Sprintf("k%d", slotID), Value: "1"}},
		}
		w.slotData[slotID] = slot
	}
	return slot
}

func (w *fakeWorker) wrap(path string, h http.HandlerFunc) http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		w.mu.Lock()
		w.pathCounts[path]++
		fail := w.failNext[path]
		if fail > 0 {
			w.failNext[path] = fail - 1
		}
		w.mu.Unlock()
		if fail > 0 {
			http.Error(rw, "scripted failure on "+path, 500)
			return
		}
		h(rw, r)
	}
}

// startFake builds N+spareCount fake workers and returns them along with
// their peer addresses.
func startFake(t *testing.T, n, spareCount int) (workers []*fakeWorker, slotPeers, sparePeers []string) {
	t.Helper()
	total := n + spareCount
	workers = make([]*fakeWorker, total)
	all := make([]string, total)
	for i := range total {
		w := newFakeWorker()
		w.srv = httptest.NewServer(w.handler())
		workers[i] = w
		u, _ := url.Parse(w.srv.URL)
		all[i] = u.Host
	}
	t.Cleanup(func() {
		for _, w := range workers {
			if w.srv != nil {
				w.srv.Close()
			}
		}
	})
	return workers, all[:n], all[n:]
}

// ── tests ───────────────────────────────────────────────────────────────────

func TestCoordinator_HappyPath(t *testing.T) {
	workers, slots, spares := startFake(t, 3, 0)
	c := newCoordinator(slots, spares, 4, 10*time.Millisecond, time.Second)
	for i, s := range c.slots {
		s.urls = []string{fmt.Sprintf("https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-01/x%d.wet.gz", i)}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	mustDrive(t, c, ctx, sMapped, "map")
	mustDrive(t, c, ctx, sReduced, "reduce")
	mustDrive(t, c, ctx, sDone, "result")

	for i, w := range workers {
		w.mu.Lock()
		if !w.loaded || !w.mapped || !w.reduced {
			t.Errorf("worker %d: loaded=%v mapped=%v reduced=%v", i, w.loaded, w.mapped, w.reduced)
		}
		w.mu.Unlock()
	}
	for _, s := range c.slots {
		if s.state != sDone {
			t.Errorf("slot %d state=%s, want done", s.id, s.state)
		}
	}
}

func TestCoordinator_ReplaceOnMapTransportError(t *testing.T) {
	// Slot 1's host will close mid-test; coordinator should pick up the spare.
	workers, slots, spares := startFake(t, 3, 2)
	c := newCoordinator(slots, spares, 4, 10*time.Millisecond, time.Second)
	for _, s := range c.slots {
		s.urls = []string{"https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-01/x.wet.gz"}
	}

	// Kill slot 1's server so /load fails.
	workers[1].srv.Close()
	workers[1].srv = nil

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	mustDrive(t, c, ctx, sMapped, "map")

	// Slot 1 should now be assigned to one of the spares.
	if c.slots[1].host == slots[1] {
		t.Errorf("slot 1 still on dead host %s", slots[1])
	}
	if c.epoch.Load() == 0 {
		t.Errorf("epoch should have been bumped on replacement")
	}
}

func TestCoordinator_ReplacePeerOnReduceFailure(t *testing.T) {
	// All map fine. Then worker[1] dies; worker[0]'s reduce returns 500 with
	// "fetch from peer <worker1>". Coordinator must blame worker[1], not 0.
	workers, slots, spares := startFake(t, 3, 2)
	c := newCoordinator(slots, spares, 4, 10*time.Millisecond, time.Second)
	for _, s := range c.slots {
		s.urls = []string{"https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-01/x.wet.gz"}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	mustDrive(t, c, ctx, sMapped, "map")

	// Worker 1 "dies" mid-reduce: make worker 0's reduce return a peer error,
	// and close worker 1's server so health watcher / next call sees it.
	originalHost1 := slots[1]
	workers[0].mu.Lock()
	workers[0].failPeerReduce = originalHost1
	workers[0].mu.Unlock()
	workers[1].srv.Close()
	workers[1].srv = nil
	// Stop returning peer error after worker 1 is replaced.
	stop := make(chan struct{})
	go func() {
		<-stop
		workers[0].mu.Lock()
		workers[0].failPeerReduce = ""
		workers[0].mu.Unlock()
	}()
	go func() {
		// Wait for replacement to happen then unblock.
		for {
			if c.slots[1].host != originalHost1 {
				close(stop)
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()

	mustDrive(t, c, ctx, sReduced, "reduce")

	if c.slots[1].host == originalHost1 {
		t.Errorf("slot 1 should be replaced; still on %s", originalHost1)
	}
	if c.slots[0].host != slots[0] {
		t.Errorf("slot 0 should NOT be replaced; got %s, want %s", c.slots[0].host, slots[0])
	}
}

func TestCoordinator_ReassignsToActiveWorkerWhenNoSpareRemains(t *testing.T) {
	workers, slots, spares := startFake(t, 2, 0) // zero spares
	c := newCoordinator(slots, spares, 4, 10*time.Millisecond, time.Second)
	for _, s := range c.slots {
		s.urls = []string{"https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-01/x.wet.gz"}
	}
	workers[0].srv.Close()
	workers[0].srv = nil

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mustDrive(t, c, ctx, sMapped, "map")
	mustDrive(t, c, ctx, sReduced, "reduce")
	mustDrive(t, c, ctx, sDone, "result")

	if got, want := c.slots[0].host, slots[1]; got != want {
		t.Fatalf("slot 0 host = %s, want fallback onto surviving worker %s", got, want)
	}
	if got, want := c.slots[1].host, slots[1]; got != want {
		t.Fatalf("slot 1 host = %s, want surviving worker to keep original slot %s", got, want)
	}
}

func TestExtractFailedSlot(t *testing.T) {
	cases := []struct {
		in   string
		want int
		ok   bool
	}{
		{`POST x/reduce returned 500: fetch from slot 1 peer 1.2.3.4:9090: died`, 1, true},
		{`fetch from slot 0 peer host.local:80 ran away`, 0, true},
		{`fetch from peer host.local:80 ran away`, 0, false},
		{`some other error`, 0, false},
	}
	for _, c := range cases {
		got, ok := extractFailedSlot(c.in)
		if ok != c.ok || got != c.want {
			t.Errorf("extractFailedSlot(%q) = (%d, %v), want (%d, %v)", c.in, got, ok, c.want, c.ok)
		}
	}
}

func TestExtractFailedPeer(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{`POST x/reduce returned 500: fetch from peer 1.2.3.4:9090: died`, "1.2.3.4:9090"},
		{`fetch from peer host.local:80 ran away`, "host.local:80"},
		{`some other error`, ""},
		{`fetch from peer onlyhost`, "onlyhost"},
	}
	for _, c := range cases {
		got := extractFailedPeer(c.in)
		if got != c.want {
			t.Errorf("extractFailedPeer(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestCoordinator_MergeResults(t *testing.T) {
	c := newCoordinator([]string{"x"}, nil, 1, time.Millisecond, time.Second)
	c.slots[0].result = []KeyValue{{"a", "3"}, {"b", "1"}}
	// Add a second virtual slot to test merge across slots.
	c.slots = append(c.slots, &slot{id: 1, result: []KeyValue{{"a", "2"}, {"c", "5"}}})

	tmp := t.TempDir()
	out := filepath.Join(tmp, "out.txt")
	if err := c.mergeResults(out); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(out)
	got := strings.TrimSpace(string(data))
	// Expected sorted desc by count, then alpha:
	// c\t5
	// a\t5
	// b\t1
	want := "a\t5\nc\t5\nb\t1"
	if got != want {
		t.Errorf("merge output:\n%s\nwant:\n%s", got, want)
	}
}

// mustDrive runs drive with a deadline and a label for clearer failures.
func mustDrive(t *testing.T, c *coordinator, ctx context.Context, target slotState, label string) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- c.drive(ctx, target) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("drive(%s): %v", label, err)
		}
	case <-time.After(8 * time.Second):
		t.Fatalf("drive(%s) timed out; slot states: %s", label, dumpSlots(c))
	}
}

func dumpSlots(c *coordinator) string {
	var parts []string
	for _, s := range c.slots {
		parts = append(parts, fmt.Sprintf("[%d:%s state=%s att=%d]", s.id, s.host, s.state, s.attempts))
	}
	sort.Strings(parts)
	return strings.Join(parts, " ")
}

// Suppress "imported and not used" with this no-op when iterating during dev.
var _ = atomic.Int64{}
