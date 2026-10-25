// Advanced Go-client checks for rust-natsd, run by
// scripts/e2e-go-advanced.sh: account service imports, JetStream
// mirroring, and JWT operator auth — all via the official nats.go
// client. The JWT chain is minted here with nkeys (no nsc needed):
// mode genjwt writes a .conf for the server plus a creds JSON file,
// mode jwt then connects with nats.UserJWT against that server.

package main

import (
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

var (
	advOut   = flag.String("out", "", "genjwt: .conf path to write")
	advCreds = flag.String("creds", "", "genjwt: creds JSON path to write; jwt: creds JSON path to read")
)

func b64url(data []byte) string {
	return base64.RawURLEncoding.EncodeToString(data)
}

func makeJWT(claims map[string]interface{}, signer nkeys.KeyPair) string {
	header := b64url([]byte(`{"typ":"JWT","alg":"ed25519-nkey"}`))
	payloadBytes, _ := json.Marshal(claims)
	payload := b64url(payloadBytes)
	signingInput := header + "." + payload
	sig, err := signer.Sign([]byte(signingInput))
	if err != nil {
		panic(err)
	}
	return signingInput + "." + b64url(sig)
}

func mustKP(kp nkeys.KeyPair, err error) nkeys.KeyPair {
	if err != nil {
		panic(err)
	}
	return kp
}

func pub(kp nkeys.KeyPair) string {
	s, err := kp.PublicKey()
	if err != nil {
		panic(err)
	}
	return s
}

func seed(kp nkeys.KeyPair) string {
	s, err := kp.Seed()
	if err != nil {
		panic(err)
	}
	return string(s)
}

// genjwtMode mints an operator/account/user chain and writes the
// server .conf (inline operator JWT + resolver preload, the Go shape
// rust-natsd accepts) and a creds JSON for jwtMode.
func genjwtMode() {
	op := mustKP(nkeys.CreateOperator())
	acct := mustKP(nkeys.CreateAccount())
	sysAcct := mustKP(nkeys.CreateAccount())
	user := mustKP(nkeys.CreateUser())
	now := time.Now().Unix()

	opJWT := makeJWT(map[string]interface{}{
		"iss": pub(op), "sub": pub(op), "iat": now,
		"nats": map[string]interface{}{
			"type": "operator", "version": 2, "name": "OP",
			"system_account": pub(sysAcct),
		},
	}, op)
	acctJWT := makeJWT(map[string]interface{}{
		"iss": pub(op), "sub": pub(acct), "iat": now, "name": "ACCT",
		"nats": map[string]interface{}{
			"type": "account", "version": 2, "name": "ACCT",
			"limits": map[string]interface{}{
				"payload": 1048576, "subs": 1000, "data": -1,
				"conn": 100, "leaf": -1, "imports": -1, "exports": -1,
				"wildcards": true,
			},
		},
	}, op)
	userJWT := makeJWT(map[string]interface{}{
		"iss": pub(acct), "sub": pub(user), "iat": now, "name": "bob",
		"nats": map[string]interface{}{
			"type": "user", "version": 2, "name": "bob",
			"pub": map[string]interface{}{"allow": []string{"allowed.>"}},
			"sub": map[string]interface{}{"allow": []string{"allowed.>"}},
			"payload": 1048576, "subs": -1, "data": -1,
		},
	}, acct)

	conf := fmt.Sprintf("host: \"127.0.0.1\"\nport: 0\ncluster {\n  port: 0\n}\noperator: \"%s\"\nresolver {\n  type: memory\n}\nresolver_preload: {\n  \"%s\": \"%s\"\n}\n", opJWT, pub(acct), acctJWT)
	if err := os.WriteFile(*advOut, []byte(conf), 0600); err != nil {
		panic(err)
	}
	creds, _ := json.Marshal(map[string]string{
		"user_jwt": userJWT, "user_seed": seed(user),
	})
	if err := os.WriteFile(*advCreds, creds, 0600); err != nil {
		panic(err)
	}
	fmt.Println("PASS genjwt wrote conf and creds")
}

// jwtMode reproduces a server bug (see the ignored Rust test
// jwt_login_without_nkey_field_authenticates): nats.go UserJWT never
// sends a CONNECT nkey field, but rust-natsd requires nkey == JWT sub,
// so this currently fails with Authorization Violation. It is kept so
// the fix can be verified by wiring it into scripts/e2e-go-advanced.sh.
func jwtMode(url string) {
	raw, err := os.ReadFile(*advCreds)
	check("jwt creds read", err)
	if err != nil {
		return
	}
	var creds map[string]string
	if err := json.Unmarshal(raw, &creds); err != nil {
		check("jwt creds json", err)
		return
	}
	kp, err := nkeys.FromSeed([]byte(creds["user_seed"]))
	check("jwt user keypair", err)
	if err != nil {
		return
	}
	userJWT := creds["user_jwt"]
	nc, err := nats.Connect(url,
		nats.UserJWT(
			func() (string, error) { return userJWT, nil },
			func(nonce []byte) ([]byte, error) { return kp.Sign(nonce) },
		),
		nats.Timeout(5*time.Second),
		nats.MaxReconnects(0),
	)
	check("jwt connect", err)
	if err != nil {
		return
	}
	defer nc.Close()
	sub, err := nc.SubscribeSync("allowed.news")
	check("jwt subscribe allowed", err)
	_ = nc.Flush()
	_ = nc.Publish("allowed.news", []byte("hi"))
	msg, err := sub.NextMsg(3 * time.Second)
	check("jwt receive allowed", err)
	if err == nil {
		expect("jwt payload", string(msg.Data) == "hi", string(msg.Data))
	}
	// A tampered JWT must be rejected.
	bad := userJWT[:len(userJWT)-2] + "AA"
	_, err = nats.Connect(url,
		nats.UserJWT(
			func() (string, error) { return bad, nil },
			func(nonce []byte) ([]byte, error) { return kp.Sign(nonce) },
		),
		nats.Timeout(5*time.Second),
		nats.MaxReconnects(0),
	)
	expect("jwt tampered rejected", err != nil, "tampered JWT accepted")
}

func importsMode(url string) {
	responder, err := nats.Connect(url,
		nats.UserInfo("alice", "alicepw"),
		nats.Timeout(5*time.Second),
	)
	check("imports responder connect", err)
	if err != nil {
		return
	}
	defer responder.Close()
	sub, err := responder.Subscribe("svc.echo", func(m *nats.Msg) {
		_ = responder.Publish(m.Reply, []byte("echo:"+string(m.Data)))
	})
	check("imports responder subscribe", err)
	_ = responder.Flush()
	_ = sub

	requester, err := nats.Connect(url,
		nats.UserInfo("bob", "bobpw"),
		nats.Timeout(5*time.Second),
	)
	check("imports requester connect", err)
	if err != nil {
		return
	}
	defer requester.Close()
	// B imports A's svc.> as local.>: a request to local.echo must be
	// served by A's svc.echo responder through the import.
	resp, err := requester.Request("local.echo", []byte("hi"), 3*time.Second)
	check("imports request through import", err)
	if err == nil {
		expect("imports echo payload", string(resp.Data) == "echo:hi", string(resp.Data))
	}
}

func mirrorMode(url string) {
	nc, err := nats.Connect(url,
		nats.UserInfo("alice", "alicepw"),
		nats.Timeout(5*time.Second),
	)
	check("mirror connect", err)
	if err != nil {
		return
	}
	defer nc.Close()
	js, err := nc.JetStream()
	check("mirror js context", err)
	if err != nil {
		return
	}
	_, err = js.AddStream(&nats.StreamConfig{
		Name: "ORIGIN", Subjects: []string{"origin.>"},
	})
	check("mirror origin stream", err)
	_, err = js.Publish("origin.1", []byte("m1"))
	check("mirror publish 1", err)
	_, err = js.Publish("origin.2", []byte("m2"))
	check("mirror publish 2", err)
	_, err = js.AddStream(&nats.StreamConfig{
		Name:   "MIRROR",
		Mirror: &nats.StreamSource{Name: "ORIGIN"},
	})
	check("mirror stream create", err)
	msg, err := js.GetMsg("MIRROR", 1)
	check("mirror backfill msg 1", err)
	if err == nil {
		expect("mirror backfill payload", string(msg.Data) == "m1", string(msg.Data))
		expect("mirror backfill subject", msg.Subject == "origin.1", msg.Subject)
	}
	// Live copy: publish after mirror creation, then poll for it.
	_, err = js.Publish("origin.3", []byte("m3"))
	check("mirror publish 3", err)
	var live *nats.RawStreamMsg
	for i := 0; i < 30 && live == nil; i++ {
		m, err := js.GetMsg("MIRROR", 3)
		if err == nil {
			live = m
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if live == nil {
		expect("mirror live copy", false, "msg 3 never arrived in mirror")
	} else {
		expect("mirror live payload", string(live.Data) == "m3", string(live.Data))
	}
}
