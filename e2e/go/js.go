// JetStream checks against rust-natsd using the official nats.go client.
package main

import (
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

func jsMode(url string) {
	nc, err := nats.Connect(url, nats.Timeout(5*time.Second))
	check("js connect", err)
	if err != nil {
		return
	}
	defer nc.Close()
	js, err := nc.JetStream()
	check("js context", err)
	if err != nil {
		return
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "ORDERS",
		Subjects: []string{"orders.>"},
	})
	check("js stream create", err)

	ack, err := js.Publish("orders.1", []byte("m1"), nats.MsgId("id-1"))
	check("js publish 1", err)
	if err == nil {
		expect("js publish stream", ack.Stream == "ORDERS", ack.Stream)
		expect("js publish seq", ack.Sequence == 1, fmt.Sprintf("%d", ack.Sequence))
		expect("js publish not dup", !ack.Duplicate, "unexpected duplicate")
	}
	dup, err := js.Publish("orders.1", []byte("m1"), nats.MsgId("id-1"))
	check("js publish duplicate", err)
	if err == nil {
		expect("js duplicate flag", dup.Duplicate, "not flagged duplicate")
		expect("js duplicate seq", dup.Sequence == 1, fmt.Sprintf("%d", dup.Sequence))
	}
	_, err = js.Publish("orders.2", []byte("m2"), nats.ExpectLastSequence(1))
	check("js publish expected seq ok", err)
	_, err = js.Publish("orders.2", []byte("bad"), nats.ExpectLastSequence(99))
	expect("js publish expected seq rejects", err != nil, "mismatch accepted")

	msg, err := js.GetMsg("ORDERS", 1)
	check("js get msg 1", err)
	if err == nil {
		expect("js get msg payload", string(msg.Data) == "m1", string(msg.Data))
		expect("js get msg subject", msg.Subject == "orders.1", msg.Subject)
	}
	last, err := js.GetLastMsg("ORDERS", "orders.2")
	check("js get last msg", err)
	if err == nil {
		expect("js get last payload", string(last.Data) == "m2", string(last.Data))
	}

	_, err = js.AddConsumer("ORDERS", &nats.ConsumerConfig{
		Durable:       "c1",
		DeliverPolicy: nats.DeliverAllPolicy,
		AckPolicy:     nats.AckExplicitPolicy,
	})
	check("js consumer create", err)
	sub, err := js.PullSubscribe("orders.>", "c1")
	check("js pull subscribe", err)
	if err == nil {
		msgs, err := sub.Fetch(2, nats.MaxWait(3*time.Second))
		check("js fetch 2", err)
		if err == nil && len(msgs) == 2 {
			expect("js fetch payloads", string(msgs[0].Data) == "m1" && string(msgs[1].Data) == "m2",
				fmt.Sprintf("%q %q", msgs[0].Data, msgs[1].Data))
			check("js ack 1", msgs[0].Ack())
			check("js ack 2", msgs[1].Ack())
		} else {
			expect("js fetch count", false, fmt.Sprintf("got %d", len(msgs)))
		}
		_, err = js.Publish("orders.3", []byte("m3"))
		check("js publish 3", err)
		msgs, err = sub.Fetch(1, nats.MaxWait(3*time.Second))
		check("js fetch 3", err)
		if err == nil && len(msgs) == 1 {
			check("js nak", msgs[0].Nak())
			msgs, err = sub.Fetch(1, nats.MaxWait(3*time.Second))
			check("js fetch redelivery", err)
			if err == nil && len(msgs) == 1 {
				expect("js redelivery payload", string(msgs[0].Data) == "m3", string(msgs[0].Data))
				check("js ack redelivery", msgs[0].Ack())
			} else {
				expect("js redelivery count", false, fmt.Sprintf("got %d", len(msgs)))
			}
		}
	}

	// Push consumer via a JetStream subscription.
	pushSub, err := js.SubscribeSync("orders.>", nats.Durable("push1"), nats.DeliverNew())
	check("js push subscribe", err)
	if err == nil {
		_, err = js.Publish("orders.9", []byte("m9"))
		check("js publish 9", err)
		m, err := pushSub.NextMsg(3 * time.Second)
		check("js push receive", err)
		if err == nil {
			expect("js push payload", string(m.Data) == "m9", string(m.Data))
			check("js push ack", m.Ack())
		}
	}

	info, err := js.StreamInfo("ORDERS")
	check("js stream info", err)
	if err == nil {
		expect("js stream msgs", info.State.Msgs == 4, fmt.Sprintf("%d", info.State.Msgs))
	}
	check("js purge", js.PurgeStream("ORDERS"))
	check("js delete stream", js.DeleteStream("ORDERS"))
}
