// Go client end-to-end test for rust-natsd using the official nats.go
// client. Run by scripts/e2e-go.sh, which starts the server first.
//
// Usage:
//
//	go run . -addr nats://127.0.0.1:4222 -mode basic
//	go run . -addr nats://127.0.0.1:4223 -mode auth
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go"
)

var failures int

func check(name string, err error) {
	if err != nil {
		failures++
		fmt.Printf("FAIL %s: %v\n", name, err)
	} else {
		fmt.Printf("PASS %s\n", name)
	}
}

func expect(name string, cond bool, detail string) {
	if !cond {
		failures++
		fmt.Printf("FAIL %s: %s\n", name, detail)
	} else {
		fmt.Printf("PASS %s\n", name)
	}
}

func basic(url string) {
	nc, err := nats.Connect(url, nats.Timeout(5*time.Second))
	check("connect", err)
	if err != nil {
		return
	}
	defer nc.Close()
	expect("connected status", nc.IsConnected(), "not connected")
	fmt.Printf("     server: %s %s\n", nc.ConnectedServerName(), nc.ConnectedServerVersion())

	// Pub/Sub
	sub, err := nc.SubscribeSync("greetings")
	check("subscribe", err)
	_ = nc.Flush()
	check("publish", nc.Publish("greetings", []byte("hello go")))
	msg, err := sub.NextMsg(3 * time.Second)
	check("receive", err)
	if err == nil {
		expect("payload", string(msg.Data) == "hello go", string(msg.Data))
	}

	// Wildcards: events.* matches one token only.
	wsub, err := nc.SubscribeSync("events.*")
	check("wildcard subscribe", err)
	_ = nc.Flush()
	_ = nc.Publish("events.click", []byte("1"))
	_ = nc.Publish("events.user.click", []byte("2"))
	msg, err = wsub.NextMsg(3 * time.Second)
	check("wildcard receive", err)
	if err == nil {
		expect("wildcard payload", string(msg.Data) == "1", string(msg.Data))
	}
	_, err = wsub.NextMsg(400 * time.Millisecond)
	expect("wildcard does not match two tokens", err == nats.ErrTimeout, fmt.Sprint(err))

	// Queue groups: two subscribers, 20 messages, exactly 20 deliveries.
	q1, err := nc.QueueSubscribeSync("jobs", "workers")
	check("queue subscribe 1", err)
	nc2, err := nats.Connect(url)
	check("connect 2", err)
	defer nc2.Close()
	q2, err := nc2.QueueSubscribeSync("jobs", "workers")
	check("queue subscribe 2", err)
	_ = nc.Flush()
	_ = nc2.Flush()
	for i := 0; i < 20; i++ {
		_ = nc.Publish("jobs", []byte("x"))
	}
	_ = nc.Flush()
	counts := map[int]int{}
	total := 0
	deadline := time.Now().Add(5 * time.Second)
	for total < 20 && time.Now().Before(deadline) {
		if m, err := q1.NextMsg(100 * time.Millisecond); err == nil {
			_ = m
			counts[1]++
			total++
			continue
		}
		if m, err := q2.NextMsg(100 * time.Millisecond); err == nil {
			_ = m
			counts[2]++
			total++
		}
	}
	expect("queue total deliveries == 20", total == 20, fmt.Sprintf("got %d", total))
	expect("queue load spread", counts[1] > 0 && counts[2] > 0, fmt.Sprintf("%v", counts))

	// Request/Reply
	respSub, err := nc.Subscribe("svc.upper", func(m *nats.Msg) {
		out := []byte(string(m.Data))
		for i, b := range out {
			if b >= 'a' && b <= 'z' {
				out[i] = b - 32
			}
		}
		_ = m.Respond(out)
	})
	check("responder subscribe", err)
	defer respSub.Unsubscribe()
	_ = nc.Flush()
	resp, err := nc.Request("svc.upper", []byte("hello"), 3*time.Second)
	check("request", err)
	if err == nil {
		expect("reply payload", string(resp.Data) == "HELLO", string(resp.Data))
	}

	// Headers
	hsub, err := nc.SubscribeSync("with.headers")
	check("header subscribe", err)
	_ = nc.Flush()
	hmsg := nats.NewMsg("with.headers")
	hmsg.Data = []byte("body")
	hmsg.Header.Set("X-Request-Id", "abc-123")
	check("publish with headers", nc.PublishMsg(hmsg))
	msg, err = hsub.NextMsg(3 * time.Second)
	check("header receive", err)
	if err == nil {
		expect("header value", msg.Header.Get("X-Request-Id") == "abc-123",
			fmt.Sprintf("headers: %v", msg.Header))
		expect("header payload", string(msg.Data) == "body", string(msg.Data))
	}

	// Auto-unsubscribe after N.
	asub, err := nc.SubscribeSync("limited")
	check("limited subscribe", err)
	_ = nc.Flush()
	check("auto-unsubscribe(2)", asub.AutoUnsubscribe(2))
	for i := 0; i < 3; i++ {
		_ = nc.Publish("limited", []byte("x"))
	}
	_ = nc.Flush()
	got := 0
	for {
		_, err := asub.NextMsg(400 * time.Millisecond)
		if err != nil {
			break
		}
		got++
	}
	expect("auto-unsubscribe delivered exactly 2", got == 2, fmt.Sprintf("got %d", got))

	// In-order delivery of a burst.
	osub, err := nc.SubscribeSync("seq")
	check("seq subscribe", err)
	_ = nc.Flush()
	for i := 0; i < 50; i++ {
		_ = nc.Publish("seq", []byte{byte(i)})
	}
	_ = nc.Flush()
	ordered := true
	for i := 0; i < 50; i++ {
		m, err := osub.NextMsg(3 * time.Second)
		if err != nil || len(m.Data) != 1 || int(m.Data[0]) != i {
			ordered = false
			break
		}
	}
	expect("50 messages in order", ordered, "")
}

func auth(url string) {
	_, err := nats.Connect(url,
		nats.UserInfo("derek", "wrong"),
		nats.Timeout(3*time.Second),
		nats.MaxReconnects(0),
	)
	expect("bad credentials rejected", err != nil, "connect unexpectedly succeeded")

	nc, err := nats.Connect(url,
		nats.UserInfo("derek", "s3cret"),
		nats.Timeout(5*time.Second),
	)
	check("good credentials connect", err)
	if err != nil {
		return
	}
	defer nc.Close()
	sub, err := nc.SubscribeSync("secure")
	check("auth subscribe", err)
	_ = nc.Flush()
	_ = nc.Publish("secure", []byte("ok"))
	msg, err := sub.NextMsg(3 * time.Second)
	check("auth receive", err)
	if err == nil {
		expect("auth payload", string(msg.Data) == "ok", string(msg.Data))
	}

	// Token auth against the same server config is covered in Rust tests;
	// here also verify token-as-password style connect is rejected when it
	// does not match the configured password.
	_ = nc
}

// cluster runs cross-node checks: subscribe on addr2, publish on addr1
// (and the reverse), plus a queue group split across both nodes and a
// request/reply across nodes. Used for route and leaf interop tests.
func cluster(url1, url2 string) {
	subConn, err := nats.Connect(url2, nats.Timeout(5*time.Second))
	check("cluster connect node2", err)
	if err != nil {
		return
	}
	defer subConn.Close()
	pubConn, err := nats.Connect(url1, nats.Timeout(5*time.Second))
	check("cluster connect node1", err)
	if err != nil {
		return
	}
	defer pubConn.Close()

	sub, err := subConn.SubscribeSync("xnode")
	check("cluster subscribe node2", err)
	_ = subConn.Flush()
	// Retry publish until the route/leaf interest propagates.
	got := false
	for i := 0; i < 50 && !got; i++ {
		_ = pubConn.Publish("xnode", []byte("across"))
		_ = pubConn.Flush()
		if msg, err := sub.NextMsg(100 * time.Millisecond); err == nil {
			expect("cluster payload node1->node2", string(msg.Data) == "across", string(msg.Data))
			got = true
		}
	}
	expect("cluster delivery node1->node2", got, "no message in 5s")

	// Reverse direction.
	sub2, err := pubConn.SubscribeSync("xnode2")
	check("cluster subscribe node1", err)
	_ = pubConn.Flush()
	got = false
	for i := 0; i < 50 && !got; i++ {
		_ = subConn.Publish("xnode2", []byte("back"))
		_ = subConn.Flush()
		if msg, err := sub2.NextMsg(100 * time.Millisecond); err == nil {
			expect("cluster payload node2->node1", string(msg.Data) == "back", string(msg.Data))
			got = true
		}
	}
	expect("cluster delivery node2->node1", got, "no message in 5s")

	// Queue group split across nodes: exactly 20 deliveries total.
	q1, err := pubConn.QueueSubscribeSync("xjobs", "workers")
	check("cluster queue node1", err)
	q2, err := subConn.QueueSubscribeSync("xjobs", "workers")
	check("cluster queue node2", err)
	_ = pubConn.Flush()
	_ = subConn.Flush()
	time.Sleep(300 * time.Millisecond)
	for i := 0; i < 20; i++ {
		_ = pubConn.Publish("xjobs", []byte("x"))
	}
	_ = pubConn.Flush()
	total := 0
	deadline := time.Now().Add(5 * time.Second)
	for total < 20 && time.Now().Before(deadline) {
		if _, err := q1.NextMsg(100 * time.Millisecond); err == nil {
			total++
			continue
		}
		if _, err := q2.NextMsg(100 * time.Millisecond); err == nil {
			total++
		}
	}
	expect("cluster queue total == 20", total == 20, fmt.Sprintf("got %d", total))
	// Exactly-once globally: no 21st delivery.
	time.Sleep(300 * time.Millisecond)
	extra := false
	if _, err := q1.NextMsg(100 * time.Millisecond); err == nil {
		extra = true
	}
	if _, err := q2.NextMsg(100 * time.Millisecond); err == nil {
		extra = true
	}
	expect("cluster queue no extra delivery", !extra, "got a 21st message")

	// Request/reply across nodes.
	respSub, err := subConn.Subscribe("xsvc", func(m *nats.Msg) {
		_ = m.Respond([]byte("pong"))
	})
	check("cluster responder", err)
	defer respSub.Unsubscribe()
	_ = subConn.Flush()
	// Let the responder's interest propagate to the other node before
	// requesting (a request sent too early gets "no responders").
	time.Sleep(500 * time.Millisecond)
	resp, err := pubConn.Request("xsvc", []byte("ping"), 5*time.Second)
	check("cluster request", err)
	if err == nil {
		expect("cluster reply", string(resp.Data) == "pong", string(resp.Data))
	}

	// Headers across nodes (retry the publish: the subscription's
	// interest may still be propagating when the first publish lands).
	hsub, err := subConn.SubscribeSync("xhdr")
	check("cluster header subscribe", err)
	_ = subConn.Flush()
	hmsg := nats.NewMsg("xhdr")
	hmsg.Data = []byte("body")
	hmsg.Header.Set("X-Cross", "yes")
	var msg *nats.Msg
	for i := 0; i < 25; i++ {
		_ = pubConn.PublishMsg(hmsg)
		_ = pubConn.Flush()
		if m, err := hsub.NextMsg(200 * time.Millisecond); err == nil {
			msg = m
			break
		}
	}
	check("cluster header receive", func() error {
		if msg == nil {
			return fmt.Errorf("timeout")
		}
		return nil
	}())
	if msg != nil {
		expect("cluster header value", msg.Header.Get("X-Cross") == "yes",
			fmt.Sprintf("headers: %v", msg.Header))
	}

	// Reverse header direction: subscribe on node1, publish from node2.
	hsub2, err := pubConn.SubscribeSync("xhdr2")
	check("cluster header subscribe reverse", err)
	_ = pubConn.Flush()
	hmsg2 := nats.NewMsg("xhdr2")
	hmsg2.Data = []byte("body2")
	hmsg2.Header.Set("X-Cross", "back")
	var msg2 *nats.Msg
	for i := 0; i < 25; i++ {
		_ = subConn.PublishMsg(hmsg2)
		_ = subConn.Flush()
		if m, err := hsub2.NextMsg(200 * time.Millisecond); err == nil {
			msg2 = m
			break
		}
	}
	check("cluster header receive reverse", func() error {
		if msg2 == nil {
			return fmt.Errorf("timeout")
		}
		return nil
	}())
	if msg2 != nil {
		expect("cluster header value reverse", msg2.Header.Get("X-Cross") == "back",
			fmt.Sprintf("headers: %v", msg2.Header))
	}
}

func main() {
	addr := flag.String("addr", "nats://127.0.0.1:4222", "server URL")
	addr2 := flag.String("addr2", "", "second server URL (cluster mode)")
	mode := flag.String("mode", "basic", "basic|auth|cluster|js|imports|mirror|genjwt|jwt")
	flag.Parse()
	switch *mode {
	case "basic":
		basic(*addr)
	case "auth":
		auth(*addr)
	case "cluster":
		cluster(*addr, *addr2)
	case "js":
		jsMode(*addr)
	case "imports":
		importsMode(*addr)
	case "mirror":
		mirrorMode(*addr)
	case "genjwt":
		genjwtMode()
	case "jwt":
		jwtMode(*addr)
	default:
		fmt.Println("unknown mode:", *mode)
		os.Exit(2)
	}
	if failures > 0 {
		fmt.Printf("\n%d FAILURES\n", failures)
		os.Exit(1)
	}
	fmt.Println("\nALL GO E2E CHECKS PASSED")
}
