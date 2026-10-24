// Command parity-go-client runs client-visible JetStream/KV probes
// against one server and prints a JSON outcome (no timestamps, no ids,
// so Go and Rust outcomes are directly comparable).
//
// Modes:
//
//	js       full JetStream suite (stream, pull, push, redelivery,
//	         MSG.GET, purge, delete, workqueue, max-per-subject, deliver-last)
//	jscreate create ORDERS stream + 2 messages (persistence step 1)
//	jscheck  verify ORDERS survived a restart (persistence step 2)
//	kv       KeyValue put/get/delete basics
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go"
)

func out(v any) {
	b, _ := json.Marshal(v)
	fmt.Println(string(b))
}

func fatal(v any) {
	out(v)
	os.Exit(1)
}

func connect(url string) *nats.Conn {
	nc, err := nats.Connect(url, nats.MaxReconnects(0))
	if err != nil {
		fatal(map[string]any{"error": "connect: " + err.Error()})
	}
	return nc
}

func fetchAll(sub *nats.Subscription, maxWait time.Duration) []string {
	msgs, err := sub.Fetch(10, nats.MaxWait(maxWait))
	if err != nil && len(msgs) == 0 {
		return []string{}
	}
	got := []string{}
	for _, m := range msgs {
		got = append(got, string(m.Data))
		_ = m.Ack()
	}
	return got
}

func jsMode(nc *nats.Conn) {
	js, err := nc.JetStream()
	if err != nil {
		fatal(map[string]any{"error": "js: " + err.Error()})
	}
	res := map[string]any{}
	// Stream create + info.
	si, err := js.AddStream(&nats.StreamConfig{Name: "ORDERS", Subjects: []string{"orders.>"}})
	if err != nil {
		fatal(map[string]any{"error": "addstream: " + err.Error()})
	}
	res["stream"] = map[string]any{"name": si.Config.Name, "msgs": si.State.Msgs, "first_seq": si.State.FirstSeq, "last_seq": si.State.LastSeq}
	if _, err := js.Publish("orders.1", []byte("hello")); err != nil {
		fatal(map[string]any{"error": "publish1: " + err.Error()})
	}
	if _, err := js.Publish("orders.2", []byte("world")); err != nil {
		fatal(map[string]any{"error": "publish2: " + err.Error()})
	}
	si, _ = js.StreamInfo("ORDERS")
	res["after_publish"] = map[string]any{"msgs": si.State.Msgs, "first_seq": si.State.FirstSeq, "last_seq": si.State.LastSeq}
	// Pull consume.
	pull, err := js.PullSubscribe("orders.>", "c-pull")
	if err != nil {
		fatal(map[string]any{"error": "pullsub: " + err.Error()})
	}
	res["pull"] = fetchAll(pull, 3*time.Second)
	// Push consume (subscribe first, then publish).
	pushCh := make(chan string, 10)
	push, err := js.Subscribe("orders.>", func(m *nats.Msg) {
		pushCh <- string(m.Data)
		_ = m.Ack()
	}, nats.Durable("c-push"), nats.DeliverNew())
	if err != nil {
		fatal(map[string]any{"error": "pushsub: " + err.Error()})
	}
	time.Sleep(300 * time.Millisecond)
	if _, err := js.Publish("orders.3", []byte("pushmsg")); err != nil {
		fatal(map[string]any{"error": "publish3: " + err.Error()})
	}
	var pushGot []string
	select {
	case p := <-pushCh:
		pushGot = []string{p}
	case <-time.After(3 * time.Second):
		pushGot = []string{}
	}
	res["push"] = pushGot
	_ = push.Unsubscribe()
	// MSG.GET.
	m, err := js.GetMsg("ORDERS", 2)
	if err != nil {
		fatal(map[string]any{"error": "msgget: " + err.Error()})
	}
	res["msg_get_seq2"] = string(m.Data)
	// Redelivery: fetch without ack, wait past AckWait, fetch again.
	red, err := js.AddConsumer("ORDERS", &nats.ConsumerConfig{
		Durable: "c-redeliver", AckPolicy: nats.AckExplicitPolicy, AckWait: 1 * time.Second,
	})
	if err != nil {
		fatal(map[string]any{"error": "redeliver-consumer: " + err.Error()})
	}
	_ = red
	rsub, err := js.PullSubscribe("orders.>", "c-redeliver", nats.BindStream("ORDERS"))
	if err != nil {
		fatal(map[string]any{"error": "redeliver-sub: " + err.Error()})
	}
	first, err := rsub.Fetch(1, nats.MaxWait(3*time.Second))
	firstPayload := ""
	if err == nil && len(first) > 0 {
		firstPayload = string(first[0].Data)
		// Deliberately do NOT ack: it must be redelivered.
	}
	time.Sleep(2200 * time.Millisecond)
	second, err := rsub.Fetch(1, nats.MaxWait(3*time.Second))
	secondPayload := ""
	if err == nil && len(second) > 0 {
		secondPayload = string(second[0].Data)
		_ = second[0].Ack()
	}
	res["redelivery"] = map[string]any{"first": firstPayload, "second": secondPayload}
	// Purge then delete.
	if err := js.PurgeStream("ORDERS"); err != nil {
		fatal(map[string]any{"error": "purge: " + err.Error()})
	}
	si, _ = js.StreamInfo("ORDERS")
	res["after_purge_msgs"] = si.State.Msgs
	if err := js.DeleteStream("ORDERS"); err != nil {
		fatal(map[string]any{"error": "delete: " + err.Error()})
	}
	// Workqueue: ack removes the message for everyone.
	if _, err := js.AddStream(&nats.StreamConfig{Name: "WQ", Subjects: []string{"wq.>"}, Retention: nats.WorkQueuePolicy}); err != nil {
		fatal(map[string]any{"error": "wq-stream: " + err.Error()})
	}
	if _, err := js.Publish("wq.1", []byte("w1")); err != nil {
		fatal(map[string]any{"error": "wq-pub1: " + err.Error()})
	}
	if _, err := js.Publish("wq.2", []byte("w2")); err != nil {
		fatal(map[string]any{"error": "wq-pub2: " + err.Error()})
	}
	wsub, err := js.PullSubscribe("wq.>", "c-wq")
	if err != nil {
		fatal(map[string]any{"error": "wq-sub: " + err.Error()})
	}
	res["workqueue"] = fetchAll(wsub, 3*time.Second)
	// After acking both, the workqueue stream must be empty. (A second
	// consumer with the same filter is rejected on workqueue streams by
	// Go — "filtered consumer not unique" — so the stream state, not a
	// second consumer, is the check.)
	// Acks are processed asynchronously by the server; poll until the
	// workqueue drains (or 2s elapse) instead of racing a single read.
	wmsgs := uint64(0)
	for i := 0; i < 20; i++ {
		wsi, err := js.StreamInfo("WQ")
		if err != nil {
			fatal(map[string]any{"error": "wq-info: " + err.Error()})
		}
		wmsgs = wsi.State.Msgs
		if wmsgs == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	res["workqueue_after_ack"] = wmsgs
	// MaxMsgsPerSubject.
	if _, err := js.AddStream(&nats.StreamConfig{Name: "MPS", Subjects: []string{"mps.>"}, MaxMsgsPerSubject: 2}); err != nil {
		fatal(map[string]any{"error": "mps-stream: " + err.Error()})
	}
	for _, p := range []string{"a", "b", "c"} {
		if _, err := js.Publish("mps.1", []byte(p)); err != nil {
			fatal(map[string]any{"error": "mps-pub: " + err.Error()})
		}
	}
	msub, err := js.PullSubscribe("mps.>", "c-mps")
	if err != nil {
		fatal(map[string]any{"error": "mps-sub: " + err.Error()})
	}
	res["max_per_subject"] = fetchAll(msub, 3*time.Second)
	// DeliverLast.
	if _, err := js.AddStream(&nats.StreamConfig{Name: "DL", Subjects: []string{"dl.>"}}); err != nil {
		fatal(map[string]any{"error": "dl-stream: " + err.Error()})
	}
	for _, p := range []string{"d1", "d2", "d3"} {
		if _, err := js.Publish("dl.1", []byte(p)); err != nil {
			fatal(map[string]any{"error": "dl-pub: " + err.Error()})
		}
	}
	if _, err := js.AddConsumer("DL", &nats.ConsumerConfig{Durable: "c-last", DeliverPolicy: nats.DeliverLastPolicy, AckPolicy: nats.AckExplicitPolicy}); err != nil {
		fatal(map[string]any{"error": "dl-consumer: " + err.Error()})
	}
	dsub, err := js.PullSubscribe("", "c-last", nats.Bind("DL", "c-last"))
	if err != nil {
		fatal(map[string]any{"error": "dl-sub: " + err.Error()})
	}
	res["deliver_last"] = fetchAll(dsub, 3*time.Second)
	out(res)
}

func jsCreate(nc *nats.Conn) {
	js, err := nc.JetStream()
	if err != nil {
		fatal(map[string]any{"error": "js: " + err.Error()})
	}
	if _, err := js.AddStream(&nats.StreamConfig{Name: "ORDERS", Subjects: []string{"orders.>"}}); err != nil {
		fatal(map[string]any{"error": "addstream: " + err.Error()})
	}
	if _, err := js.Publish("orders.1", []byte("hello")); err != nil {
		fatal(map[string]any{"error": "publish1: " + err.Error()})
	}
	if _, err := js.Publish("orders.2", []byte("world")); err != nil {
		fatal(map[string]any{"error": "publish2: " + err.Error()})
	}
	out(map[string]any{"created": true, "msgs": 2})
}

func jsCheck(nc *nats.Conn) {
	js, err := nc.JetStream()
	if err != nil {
		fatal(map[string]any{"error": "js: " + err.Error()})
	}
	si, err := js.StreamInfo("ORDERS")
	if err != nil {
		fatal(map[string]any{"error": "streaminfo: " + err.Error()})
	}
	m, err := js.GetMsg("ORDERS", 2)
	if err != nil {
		fatal(map[string]any{"error": "msgget: " + err.Error()})
	}
	sub, err := js.PullSubscribe("orders.>", "c-check")
	if err != nil {
		fatal(map[string]any{"error": "pullsub: " + err.Error()})
	}
	out(map[string]any{
		"stream": si.Config.Name, "msgs": si.State.Msgs,
		"msg_get_seq2": string(m.Data), "pull": fetchAll(sub, 3*time.Second),
	})
}

func kvMode(nc *nats.Conn) {
	js, err := nc.JetStream()
	if err != nil {
		fatal(map[string]any{"error": "js: " + err.Error()})
	}
	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "TESTKV"})
	if err != nil {
		fatal(map[string]any{"error": "createkv: " + err.Error()})
	}
	if _, err := kv.Put("k1", []byte("v1")); err != nil {
		fatal(map[string]any{"error": "put: " + err.Error()})
	}
	e, err := kv.Get("k1")
	if err != nil {
		fatal(map[string]any{"error": "get: " + err.Error()})
	}
	if err := kv.Delete("k1"); err != nil {
		fatal(map[string]any{"error": "delete: " + err.Error()})
	}
	_, errAfter := kv.Get("k1")
	res := map[string]any{"get_value": string(e.Value())}
	if errAfter != nil {
		res["get_after_delete_error"] = "key_not_found"
	} else {
		res["get_after_delete_error"] = "none (key still present)"
	}
	out(res)
}

func main() {
	addr := flag.String("addr", "", "nats://host:port")
	mode := flag.String("mode", "js", "js|jscreate|jscheck|kv")
	flag.Parse()
	if *addr == "" {
		fatal(map[string]any{"error": "missing -addr"})
	}
	nc := connect(*addr)
	defer nc.Close()
	switch *mode {
	case "js":
		jsMode(nc)
	case "jscreate":
		jsCreate(nc)
	case "jscheck":
		jsCheck(nc)
	case "kv":
		kvMode(nc)
	default:
		fatal(map[string]any{"error": "unknown mode " + *mode})
	}
}
