// Command nkeytool generates throwaway nkey keypairs and signs messages
// for the parity suite's raw-wire nkey probes. The seed it prints is a
// test-only key generated fresh per parity run; it is never committed
// and must never be used for anything real.
//
// Usage:
//
//	nkeytool gen                  -> {"pubkey": "...", "seed": "..."}
//	nkeytool sign <seed> <msg>    -> RawURLEncoding base64 signature
//	                                 (the encoding nats.go itself uses)
package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"

	"github.com/nats-io/nkeys"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: nkeytool gen|sign <seed> <msg>")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "gen":
		kp, err := nkeys.CreateUser()
		if err != nil {
			fatal(err)
		}
		seed, err := kp.Seed()
		if err != nil {
			fatal(err)
		}
		pub, err := kp.PublicKey()
		if err != nil {
			fatal(err)
		}
		b, _ := json.Marshal(map[string]string{"pubkey": pub, "seed": string(seed)})
		fmt.Println(string(b))
	case "sign":
		if len(os.Args) < 4 {
			fmt.Fprintln(os.Stderr, "usage: nkeytool sign <seed> <msg>")
			os.Exit(2)
		}
		kp, err := nkeys.FromSeed([]byte(os.Args[2]))
		if err != nil {
			fatal(err)
		}
		sig, err := kp.Sign([]byte(os.Args[3]))
		if err != nil {
			fatal(err)
		}
		fmt.Println(base64.RawURLEncoding.EncodeToString(sig))
	default:
		fmt.Fprintln(os.Stderr, "unknown subcommand")
		os.Exit(2)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
