// bench-client runs one benchmark scenario against a NATS server URL.
// The identical binary is used against both servers, so any client bias
// cancels out of the comparison. Each scenario warms up, then measures.
//
// Modes:
//
//	pubsub   - 1 publisher conn, 1 subscriber conn; measures end-to-end
//	           publish+deliver throughput (time from first publish until
//	           the subscriber has received every message).
//	reqreply - 1 service conn replying on bench.req, 1 requester conn;
//	           measures per-request round-trip latency.
//	js       - JetStream publish with ack wait, one stream, memory storage.
//
// Output is one RESULT line, machine-parseable by scripts/bench.sh:
//
//	RESULT scenario=<name> count=<n> size=<bytes> elapsed_ms=<ms>
//	       msgs_per_sec=<f> mb_per_sec=<f> mean_us=<f> p50_us=<f> p99_us=<f>
//
// Latency fields are 0 for throughput modes.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
)

func payload(size int) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = 'x'
	}
	return b
}

func runPubSub(nc *nats.Conn, sub *nats.Conn, size, count, warmup int) {
	p := payload(size)
	received := make(chan struct{})
	var n int64
	_, err := sub.Subscribe("bench", func(m *nats.Msg) {
		if atomic.AddInt64(&n, 1) == int64(count) {
			close(received)
		}
	})
	if err != nil {
		log.Fatalf("subscribe: %v", err)
	}
	if err := sub.Flush(); err != nil {
		log.Fatalf("sub flush: %v", err)
	}
	// Warm-up (not measured), then reset the counter for the timed run.
	for i := 0; i < warmup; i++ {
		if err := nc.Publish("bench", p); err != nil {
			log.Fatalf("warmup publish: %v", err)
		}
	}
	if err := nc.Flush(); err != nil {
		log.Fatalf("warmup flush: %v", err)
	}
	// Drain warm-up deliveries before starting the clock. If the
	// subscriber was dropped (e.g. as a slow consumer), this expires
	// and the measured run then also stalls; both are reported as a
	// failure by the timeout below, not hidden.
	deadline := time.Now().Add(15 * time.Second)
	for atomic.LoadInt64(&n) < int64(warmup) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	atomic.StoreInt64(&n, 0)
	// Re-arm the done signal for the measured run.
	received = make(chan struct{})
	// The subscriber callback above closes the old channel; install a
	// second subscription-free wait by polling the counter instead.
	start := time.Now()
	for i := 0; i < count; i++ {
		if err := nc.Publish("bench", p); err != nil {
			log.Fatalf("publish: %v", err)
		}
	}
	if err := nc.Flush(); err != nil {
		log.Fatalf("flush: %v", err)
	}
	for atomic.LoadInt64(&n) < int64(count) {
		select {
		case <-received:
		case <-time.After(30 * time.Second):
			log.Fatalf("timeout waiting for deliveries: got %d of %d", atomic.LoadInt64(&n), count)
		}
		// The callback closes `received` only at count; the loop above
		// re-checks the counter, so a spurious wake is harmless.
		if atomic.LoadInt64(&n) >= int64(count) {
			break
		}
	}
	elapsed := time.Since(start)
	secs := elapsed.Seconds()
	fmt.Printf("RESULT scenario=pubsub-%d count=%d size=%d elapsed_ms=%.1f msgs_per_sec=%.1f mb_per_sec=%.3f mean_us=0 p50_us=0 p99_us=0\n",
		size, count, size, float64(elapsed.Milliseconds()), float64(count)/secs, float64(count*size)/secs/1e6)
}

func runReqReply(nc *nats.Conn, svc *nats.Conn, size, count, warmup int) {
	p := payload(size)
	_, err := svc.Subscribe("bench.req", func(m *nats.Msg) {
		_ = m.Respond(p)
	})
	if err != nil {
		log.Fatalf("service subscribe: %v", err)
	}
	if err := svc.Flush(); err != nil {
		log.Fatalf("service flush: %v", err)
	}
	doReq := func() time.Duration {
		start := time.Now()
		_, err := nc.Request("bench.req", p, 10*time.Second)
		if err != nil {
			log.Fatalf("request: %v", err)
		}
		return time.Since(start)
	}
	for i := 0; i < warmup; i++ {
		doReq()
	}
	lats := make([]time.Duration, 0, count)
	for i := 0; i < count; i++ {
		lats = append(lats, doReq())
	}
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	var sum time.Duration
	for _, d := range lats {
		sum += d
	}
	mean := sum / time.Duration(len(lats))
	p50 := lats[len(lats)/2]
	p99 := lats[len(lats)*99/100]
	// Throughput-style fields are meaningless here; report latency only,
	// with elapsed as the sum of round trips for completeness.
	fmt.Printf("RESULT scenario=reqreply-%d count=%d size=%d elapsed_ms=%.1f msgs_per_sec=0 mb_per_sec=0 mean_us=%.1f p50_us=%.1f p99_us=%.1f\n",
		size, count, size, float64(sum.Milliseconds()), float64(mean.Microseconds()), float64(p50.Microseconds()), float64(p99.Microseconds()))
}

func runJS(nc *nats.Conn, size, count, warmup int) {
	js, err := nc.JetStream()
	if err != nil {
		log.Fatalf("jetstream: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err = js.AddStream(&nats.StreamConfig{
		Name: "BENCH", Subjects: []string{"bench.js"},
		Storage: nats.MemoryStorage,
	}, nats.Context(ctx))
	if err != nil {
		log.Fatalf("add stream: %v", err)
	}
	defer func() {
		_ = js.DeleteStream("BENCH")
	}()
	p := payload(size)
	pub := func() {
		fut, err := js.PublishAsync("bench.js", p)
		if err != nil {
			log.Fatalf("js publish: %v", err)
		}
		select {
		case <-fut.Ok():
		case err := <-fut.Err():
			log.Fatalf("js publish ack: %v", err)
		case <-time.After(30 * time.Second):
			log.Fatalf("js publish ack timeout")
		}
	}
	for i := 0; i < warmup; i++ {
		pub()
	}
	start := time.Now()
	for i := 0; i < count; i++ {
		pub()
	}
	elapsed := time.Since(start)
	secs := elapsed.Seconds()
	fmt.Printf("RESULT scenario=js-%d count=%d size=%d elapsed_ms=%.1f msgs_per_sec=%.1f mb_per_sec=%.3f mean_us=0 p50_us=0 p99_us=0\n",
		size, count, size, float64(elapsed.Milliseconds()), float64(count)/secs, float64(count*size)/secs/1e6)
}

func main() {
	mode := flag.String("mode", "", "pubsub | reqreply | js")
	addr := flag.String("addr", "nats://127.0.0.1:4222", "server URL")
	size := flag.Int("size", 256, "payload bytes")
	count := flag.Int("count", 200000, "measured messages/requests")
	warmup := flag.Int("warmup", 10000, "warm-up messages/requests (unmeasured)")
	flag.Parse()
	if *mode == "" {
		fmt.Fprintln(os.Stderr, "missing -mode")
		os.Exit(2)
	}
	nc, err := nats.Connect(*addr)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer nc.Close()
	switch *mode {
	case "pubsub":
		sub, err := nats.Connect(*addr)
		if err != nil {
			log.Fatalf("sub connect: %v", err)
		}
		defer sub.Close()
		runPubSub(nc, sub, *size, *count, *warmup)
	case "reqreply":
		svc, err := nats.Connect(*addr)
		if err != nil {
			log.Fatalf("svc connect: %v", err)
		}
		defer svc.Close()
		runReqReply(nc, svc, *size, *count, *warmup)
	case "js":
		runJS(nc, *size, *count, *warmup)
	default:
		fmt.Fprintf(os.Stderr, "unknown mode %q\n", *mode)
		os.Exit(2)
	}
}
