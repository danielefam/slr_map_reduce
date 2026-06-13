// Fault-tolerant slot-based coordinator for the MapReduce orchestrator.
//
// Concepts:
//
//   - Slot:  one of the N logical workers (0..N-1). The partition function
//     FNV(key) % N depends on N, so N is fixed for the whole job. A slot
//     can be served by different physical hosts over time as we replace
//     failed ones.
//
//   - Spare: a host that successfully deployed but is not assigned a slot.
//     Used to replace failed slot hosts.
//
//   - Phase: load -> map -> reduce -> result. Replacement requires replaying
//     the prerequisite phases on the new host.
//
//   - Epoch: bumped on every host swap. Used to detect that the reduce phase
//     must be re-run for every slot (because their previous reduces pulled
//     from a host that is no longer the owner).

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// slotState tracks how far a slot has progressed on its CURRENT host.
// Whenever a slot is reassigned to a new host, its state is reset to sPending
// and the prerequisite phases are replayed.
type slotState int

const (
	sPending slotState = iota // not yet loaded on the current host
	sLoaded                   // /load done
	sMapped                   // /map done; bucket files written
	sReduced                  // /reduce done
	sDone                     // /result fetched
	sFailed                   // exhausted retries and spares
)

func (s slotState) String() string {
	switch s {
	case sPending:
		return "pending"
	case sLoaded:
		return "loaded"
	case sMapped:
		return "mapped"
	case sReduced:
		return "reduced"
	case sDone:
		return "done"
	case sFailed:
		return "failed"
	}
	return "unknown"
}

// slot is one logical worker. id is fixed; host changes on replacement.
type slot struct {
	id    int
	urls  []string // for Common Crawl mode (POST /load)
	chunk []byte   // for -input mode (POST /data)

	mu       sync.Mutex
	host     string // current owner, "host:port"
	state    slotState
	attempts int
	result   []KeyValue // populated when state == sDone

	// reduceEpoch records the coordinator epoch at which this slot's reduce
	// was completed. If c.epoch is later bumped, the reduce is stale and
	// must be re-run.
	reduceEpoch int64
}

type loadRequest struct {
	URLs []string `json:"urls"`
}

// coordinator owns the slot table, the spare pool, and the configuration
// knobs for fault tolerance.
type coordinator struct {
	mu     sync.Mutex
	slots  []*slot
	spares []string // peer strings ("host:port") of unused healthy hosts
	dead   map[string]struct{}

	maxAttempts    int
	backoffInitial time.Duration
	backoffMax     time.Duration
	healthInterval time.Duration

	// epoch is bumped every time a slot's host is replaced. handleReduce
	// callers check that their reduceEpoch == current epoch before treating
	// a slot as having finished reduce; if not, the reduce is re-issued.
	epoch atomic.Int64

	// healthFailures tracks consecutive failed /health pings per host. A
	// host is declared dead after 2 consecutive failures.
	healthFailures map[string]int
	healthMu       sync.Mutex
}

func newCoordinator(slotHosts, spareHosts []string, maxAttempts int, backoffInitial, healthInterval time.Duration) *coordinator {
	c := &coordinator{
		slots:          make([]*slot, len(slotHosts)),
		spares:         append([]string(nil), spareHosts...),
		dead:           make(map[string]struct{}),
		maxAttempts:    maxAttempts,
		backoffInitial: backoffInitial,
		backoffMax:     5 * time.Second,
		healthInterval: healthInterval,
		healthFailures: make(map[string]int),
	}
	for i, h := range slotHosts {
		c.slots[i] = &slot{id: i, host: h, state: sPending}
	}
	return c
}

// peers returns a snapshot of "host:port" for each slot in id order.
// Slots without a host get an empty string.
func (c *coordinator) peers() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.slots))
	for i, s := range c.slots {
		s.mu.Lock()
		out[i] = s.host
		s.mu.Unlock()
	}
	return out
}

// mapPeers returns the peer list reducers should pull from. The slice length is
// still the full slot count so worker bucket numbering remains FNV(key) % N, but
// slots with no input are left blank because they cannot contain intermediate data.
func (c *coordinator) mapPeers() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.slots))
	for i, s := range c.slots {
		s.mu.Lock()
		if len(s.urls) > 0 || len(s.chunk) > 0 {
			out[i] = s.host
		}
		s.mu.Unlock()
	}
	return out
}

// allHosts returns slot hosts + spare hosts, used by the health watcher.
func (c *coordinator) allHosts() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	hosts := make([]string, 0, len(c.slots)+len(c.spares))
	for _, s := range c.slots {
		s.mu.Lock()
		if s.host != "" {
			hosts = append(hosts, s.host)
		}
		s.mu.Unlock()
	}
	hosts = append(hosts, c.spares...)
	return hosts
}

// hostsForCleanup returns every host the coordinator has touched, including
// dead ones, so the orchestrator can attempt cleanup everywhere.
func (c *coordinator) hostsForCleanup() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	seen := make(map[string]struct{})
	for _, s := range c.slots {
		s.mu.Lock()
		if s.host != "" {
			seen[s.host] = struct{}{}
		}
		s.mu.Unlock()
	}
	for _, p := range c.spares {
		seen[p] = struct{}{}
	}
	for h := range c.dead {
		seen[h] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for h := range seen {
		out = append(out, h)
	}
	sort.Strings(out)
	return out
}

// claimSpare pops a spare host, skipping any that we already marked dead.
func (c *coordinator) claimSpare() (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, h := range c.spares {
		if _, dead := c.dead[h]; dead {
			continue
		}
		c.spares = append(c.spares[:i], c.spares[i+1:]...)
		return h, true
	}
	return "", false
}

// markDead records that host should never be reused as a slot host.
func (c *coordinator) markDead(host string) {
	if host == "" {
		return
	}
	c.mu.Lock()
	c.dead[host] = struct{}{}
	// remove from spare pool if present
	for i, h := range c.spares {
		if h == host {
			c.spares = append(c.spares[:i], c.spares[i+1:]...)
			break
		}
	}
	c.mu.Unlock()
}

// replaceHost rebinds s to a fresh spare. The old host is marked dead. The
// slot is reset to sPending so prerequisite phases will replay on the new host.
// Returns the new host or false if no spares remain.
//
// On replacement the coordinator epoch is bumped, which forces any slot whose
// reduceEpoch is older to redo /reduce (because the peer it pulled from is
// no longer the live owner of that slot's intermediate data).
func (c *coordinator) replaceHost(s *slot, reason string) (string, bool) {
	s.mu.Lock()
	oldHost := s.host
	s.mu.Unlock()
	c.markDead(oldHost)

	newHost, ok := c.claimSpare()
	if !ok {
		log.Printf("[slot %d] no spare available to replace %s (%s)", s.id, oldHost, reason)
		return "", false
	}

	s.mu.Lock()
	s.host = newHost
	s.state = sPending
	s.reduceEpoch = 0
	s.attempts++
	s.mu.Unlock()

	c.epoch.Add(1)
	log.Printf("[slot %d] replaced %s -> %s (%s) — epoch now %d",
		s.id, oldHost, newHost, reason, c.epoch.Load())
	return newHost, true
}

// backoff returns the wait duration for the given attempt number, capped.
func (c *coordinator) backoff(attempt int) time.Duration {
	d := c.backoffInitial << attempt
	if d > c.backoffMax {
		d = c.backoffMax
	}
	return d
}

// ── per-slot remote calls ───────────────────────────────────────────────────

// loadOnHost calls POST /load (Common Crawl URL mode) or POST /data (chunk mode) on host.
func (c *coordinator) loadOnHost(ctx context.Context, host string, s *slot) error {
	if len(s.urls) > 0 {
		body, _ := json.Marshal(loadRequest{URLs: s.urls})
		return postJSONCtx(ctx, dataClient, host, "/load", body)
	}
	return postRawCtx(ctx, dataClient, host, "/data", s.chunk)
}

// mapOnHost calls POST /map on host with the slot's id and the current peer list.
func (c *coordinator) mapOnHost(ctx context.Context, host string, s *slot, peers []string) error {
	req := mapRequest{ID: s.id, Peers: peers}
	body, _ := json.Marshal(req)
	return postJSONCtx(ctx, longClient, host, "/map", body)
}

// reduceOnHost calls POST /reduce on host. The peer list is included in the
// request body so worker uses the up-to-date routing (necessary if any slot
// has been replaced since the host's last /map).
func (c *coordinator) reduceOnHost(ctx context.Context, host string, peers []string) error {
	body, _ := json.Marshal(struct {
		Peers []string `json:"peers"`
	}{peers})
	return postJSONCtx(ctx, longClient, host, "/reduce", body)
}

func (c *coordinator) resultOnHost(ctx context.Context, host string) ([]KeyValue, error) {
	return fetchResultCtx(ctx, host)
}

// ── phase drivers ───────────────────────────────────────────────────────────

// stepResult captures the outcome of one slot's single-step attempt.
type stepResult struct {
	slot     *slot
	host     string // host at time of call
	advanced bool   // true if state advanced
	err      error
}

// drive brings every slot up to the target state, retrying with spare
// substitution as needed. It returns an error only when at least one slot
// exhausts its retries or a spare is unavailable.
//
// Strategy:
//
//   - Each pass tries to advance every slot by ONE phase in parallel.
//   - After the pass, errors are inspected: a transport/timeout error
//     against the calling host marks THAT host as the culprit; a 500 from
//     the worker mentioning "fetch from peer X" marks X as the culprit.
//   - Culprit slots are replaced from the spare pool. Their state resets
//     to sPending so prerequisite phases replay on the new host.
//   - When any replacement happens during a reduce-or-later drive, every
//     slot whose reduce ran on the stale epoch is rewound to sMapped so
//     it re-runs reduce against the updated peer list.
func (c *coordinator) drive(ctx context.Context, target slotState) error {
	for {
		startEpoch := c.epoch.Load()

		results := make([]stepResult, len(c.slots))
		var wg sync.WaitGroup
		for i, s := range c.slots {
			wg.Add(1)
			go func(idx int, sl *slot) {
				defer wg.Done()
				results[idx] = c.tryOneStep(ctx, sl, target)
			}(i, s)
		}
		wg.Wait()

		if err := ctx.Err(); err != nil {
			return err
		}

		// Identify culprits and replace them.
		var replaced bool
		for _, r := range results {
			if r.err == nil {
				continue
			}
			culprit := identifyCulprit(c, r)
			if culprit == nil {
				// Could not identify a host to blame; treat as transient and retry on next pass.
				log.Printf("[drive] unattributed error on slot %d: %v", r.slot.id, r.err)
				continue
			}

			culprit.mu.Lock()
			culprit.attempts++
			attempts := culprit.attempts
			culprit.mu.Unlock()
			if attempts > c.maxAttempts {
				return fmt.Errorf("slot %d on %s exhausted %d attempts: %w",
					culprit.id, hostOf(culprit), c.maxAttempts, r.err)
			}
			if _, ok := c.replaceHost(culprit, r.err.Error()); !ok {
				return fmt.Errorf("slot %d: no spare available: %w", culprit.id, r.err)
			}
			replaced = true

			// Backoff before the next pass.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(c.backoff(attempts - 1)):
			}
		}

		// If any host swap occurred during a reduce-or-later drive, force
		// re-execution of /reduce for every slot whose reduce was completed
		// at the previous epoch.
		if target >= sReduced && c.epoch.Load() != startEpoch {
			cur := c.epoch.Load()
			for _, s := range c.slots {
				s.mu.Lock()
				if s.state >= sReduced && s.reduceEpoch < cur {
					s.state = sMapped
					s.result = nil
				}
				s.mu.Unlock()
			}
		}

		// Done?
		if !replaced && allAtLeast(c.slots, target) {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// tryOneStep advances one slot by a single phase on its current host. It
// performs no retries; the outer drive loop handles replacement.
func (c *coordinator) tryOneStep(ctx context.Context, s *slot, target slotState) stepResult {
	s.mu.Lock()
	state := s.state
	host := s.host
	s.mu.Unlock()

	res := stepResult{slot: s, host: host}
	if state >= target {
		return res
	}
	if host == "" {
		res.err = fmt.Errorf("slot %d: no host assigned", s.id)
		return res
	}

	var err error
	var next slotState
	switch state {
	case sPending:
		err = c.loadOnHost(ctx, host, s)
		next = sLoaded
	case sLoaded:
		peers := c.peers()
		err = c.mapOnHost(ctx, host, s, peers)
		next = sMapped
	case sMapped:
		peers := c.mapPeers()
		err = c.reduceOnHost(ctx, host, peers)
		next = sReduced
	case sReduced:
		var kvs []KeyValue
		kvs, err = c.resultOnHost(ctx, host)
		if err == nil {
			s.mu.Lock()
			s.result = kvs
			s.mu.Unlock()
		}
		next = sDone
	default:
		res.err = fmt.Errorf("slot %d: unexpected state %s", s.id, state)
		return res
	}

	if err != nil {
		// Don't treat context.Canceled as a slot-level error when it came
		// from the watcher cancelling its own check; let the caller handle it.
		if errors.Is(err, context.Canceled) && ctx.Err() != nil {
			res.err = err
			return res
		}
		log.Printf("[slot %d] %s on %s failed: %v", s.id, state, host, err)
		res.err = err
		return res
	}

	s.mu.Lock()
	s.state = next
	if next == sReduced {
		s.reduceEpoch = c.epoch.Load()
	}
	s.mu.Unlock()
	res.advanced = true
	return res
}

// identifyCulprit decides which host should be marked as the cause of err.
//
//   - If the error message contains "fetch from peer HOST:PORT", the dead
//     peer is the culprit (this is the worker's /reduce 500 format).
//   - Otherwise the calling host is the culprit (transport error, timeout,
//     or any other 5xx without a parseable peer).
//
// Returns the slot owning the culprit host, or nil if no slot currently
// owns that host (e.g. it was already replaced).
func identifyCulprit(c *coordinator, r stepResult) *slot {
	msg := ""
	if r.err != nil {
		msg = r.err.Error()
	}
	if dead := extractFailedPeer(msg); dead != "" {
		if s := c.slotByHost(dead); s != nil {
			return s
		}
	}
	if r.host == "" {
		return nil
	}
	return c.slotByHost(r.host)
}

// extractFailedPeer scans msg for the worker's reduce error pattern
// "fetch from peer HOST:PORT" and returns HOST:PORT, or "" if not found.
func extractFailedPeer(msg string) string {
	const marker = "fetch from peer "
	i := indexOf(msg, marker)
	if i < 0 {
		return ""
	}
	rest := msg[i+len(marker):]
	end := indexOfAny(rest, " :,\n\t\"'") // peer ends at whitespace or punctuation other than the embedded ':'
	// peer is "host:port" — first ':' is part of it, not a terminator.
	// Split at the SECOND ':' or whitespace.
	colon := -1
	for j, ch := range rest {
		if ch == ':' {
			if colon < 0 {
				colon = j
			} else {
				end = j
				break
			}
		}
		if ch == ' ' || ch == '\n' || ch == '\t' || ch == ',' || ch == '"' || ch == '\'' {
			end = j
			break
		}
	}
	if end < 0 {
		end = len(rest)
	}
	return rest[:end]
}

// indexOf and indexOfAny are tiny helpers to avoid bringing in strings here.
func indexOf(s, sub string) int {
	if len(sub) == 0 {
		return 0
	}
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func indexOfAny(s, chars string) int {
	for i, c := range s {
		for _, t := range chars {
			if c == t {
				return i
			}
		}
	}
	return -1
}

// slotByHost returns the slot whose current host equals h, or nil.
func (c *coordinator) slotByHost(h string) *slot {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, s := range c.slots {
		s.mu.Lock()
		match := s.host == h
		s.mu.Unlock()
		if match {
			return s
		}
	}
	return nil
}

// hostOf returns the slot's current host (helper for log messages).
func hostOf(s *slot) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.host
}

// allAtLeast reports whether every slot has reached target state.
func allAtLeast(slots []*slot, target slotState) bool {
	for _, s := range slots {
		s.mu.Lock()
		ok := s.state >= target
		s.mu.Unlock()
		if !ok {
			return false
		}
	}
	return true
}

// ── health watcher ──────────────────────────────────────────────────────────

// watchHealth runs until ctx is cancelled, polling /health on every slot host
// and spare. A host that fails two consecutive polls is marked dead; the
// matching slot (if any) is forced into replacement on its next phase attempt.
//
// The watcher does NOT cancel in-flight phase requests directly — that adds
// race complexity and the per-attempt RPCs already have short health-poll
// detection. Instead it marks the host dead, and advanceSlot's next iteration
// (after the in-flight call eventually errors or times out) will see no host
// or the wrong host and trigger replacement.
//
// To make detection prompt for the common case (host stops responding mid-call),
// we cancel any in-flight context bound to that slot via slot.cancel — but
// implementing that here would require plumbing cancel funcs into every call.
// For now we rely on the fact that POSTs over TCP with ServerAlive settings
// will fail within seconds when a server dies.
func (c *coordinator) watchHealth(ctx context.Context) {
	ticker := time.NewTicker(c.healthInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.pollHealth(ctx)
		}
	}
}

func (c *coordinator) pollHealth(ctx context.Context) {
	hosts := c.allHosts()
	var wg sync.WaitGroup
	for _, h := range hosts {
		wg.Add(1)
		go func(host string) {
			defer wg.Done()
			ok := healthCheck(ctx, host)
			c.healthMu.Lock()
			if ok {
				delete(c.healthFailures, host)
			} else {
				c.healthFailures[host]++
				if c.healthFailures[host] >= 2 {
					log.Printf("[health] %s failed %d consecutive checks; marking dead",
						host, c.healthFailures[host])
					c.healthMu.Unlock()
					c.markDead(host)
					return
				}
			}
			c.healthMu.Unlock()
		}(h)
	}
	wg.Wait()
}

// healthCheck returns true iff GET /health responds 200 within shortClient's timeout.
func healthCheck(ctx context.Context, peer string) bool {
	pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(pctx, http.MethodGet, "http://"+peer+"/health", nil)
	if err != nil {
		return false
	}
	resp, err := shortClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// ── final result merge ──────────────────────────────────────────────────────

// mergeResults gathers every slot's collected KVs, merges counts, sorts, and
// writes the output file.
func (c *coordinator) mergeResults(outputFile string) error {
	merged := make(map[string]int)
	for _, s := range c.slots {
		s.mu.Lock()
		for _, kv := range s.result {
			v, _ := strconv.Atoi(kv.Value)
			merged[kv.Key] += v
		}
		s.mu.Unlock()
	}
	return writeMerged(merged, outputFile)
}

func writeMerged(merged map[string]int, outputFile string) error {
	type entry struct {
		key   string
		count int
	}
	entries := make([]entry, 0, len(merged))
	for k, v := range merged {
		entries = append(entries, entry{k, v})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].count != entries[j].count {
			return entries[i].count > entries[j].count
		}
		return entries[i].key < entries[j].key
	})
	var buf bytes.Buffer
	for _, e := range entries {
		fmt.Fprintf(&buf, "%s\t%d\n", e.key, e.count)
	}
	return os.WriteFile(outputFile, buf.Bytes(), 0o644)
}
