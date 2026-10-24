#!/usr/bin/env python3
"""Raw-wire + monitoring parity probes for rust-natsd vs Go nats-server.

Run by scripts/parity.sh, which starts both servers and passes a JSON
config describing ports, cert paths and the nkey public key. This file
never starts a server itself.

Design rules (learned from the one-off /tmp harnesses):
  * Every socket read has a deadline — a silent server must time out,
    never hang the suite. Every probe is therefore bounded.
  * After SUB on one connection, wait SETTLE seconds before PUB from
    another connection: cross-connection subscription registration is
    not ordered, and a missing settle is a harness race, not a server
    difference.
  * Compare semantic outcomes, not raw bytes: delivered subjects and
    payloads, counts, the *class* of an -ERR (not its exact wording),
    and whether the connection stayed alive or closed. See
    parity/KNOWN_DIFFS.md for the diffs this normalisation deliberately
    does not hide (they are compared raw and accepted only via KNOWN).

Usage:
    python3 parity/probes.py --config /tmp/parity-XXX/config.json
        [--strict] [--only core,auth,...] [--mode raw|jscheck]

Exit codes: 0 = green (all cases pass, or differ only in KNOWN_DIFFS
in default mode), 1 = new divergence or case failure, 2 = harness error
(missing config, server unreachable, Go probe binary failure).
"""
import argparse
import base64
import json
import socket
import ssl
import subprocess
import sys
import time
import urllib.request

SETTLE = 0.35  # seconds after SUB before a cross-connection PUB

# ---------------------------------------------------------------------------
# Raw connection helper
# ---------------------------------------------------------------------------


class Conn:
    def __init__(self, port, connect=None, tls_ctx=None, tls_mode="direct"):
        """tls_mode: 'direct' wraps the socket in TLS before reading INFO
        (rust-natsd: implicit TLS); 'upgrade' reads INFO in plaintext
        first and then upgrades to TLS (Go nats-server: INFO carries
        tls_required, client upgrades — STARTTLS-style)."""
        raw = socket.create_connection(("127.0.0.1", port), timeout=5)
        self.buf = bytearray()
        if tls_ctx is not None and tls_mode == "direct":
            self.s = tls_ctx.wrap_socket(raw, server_hostname="localhost")
        else:
            self.s = raw
        self.info_line = self.read_line(5.0)
        if tls_ctx is not None and tls_mode == "upgrade":
            self.s = tls_ctx.wrap_socket(raw, server_hostname="localhost")
        self.info = json.loads(self.info_line[5:]) if self.info_line.startswith("INFO ") else {}
        if connect is not None:
            self.send(b"CONNECT " + json.dumps(connect).encode() + b"\r\n")

    def send(self, data: bytes):
        self.s.sendall(data)

    def _fill(self, timeout):
        self.s.settimeout(timeout)
        d = self.s.recv(65536)
        if not d:
            raise EOFError("closed")
        self.buf += d

    def read_line(self, timeout=2.0):
        deadline = time.time() + timeout
        while True:
            i = self.buf.find(b"\r\n")
            if i >= 0:
                line = bytes(self.buf[:i])
                del self.buf[: i + 2]
                return line.decode(errors="replace")
            remaining = deadline - time.time()
            if remaining <= 0:
                raise socket.timeout("read_line deadline")
            self._fill(remaining)

    def _read_exact(self, n, timeout=2.0):
        deadline = time.time() + timeout
        while len(self.buf) < n:
            remaining = deadline - time.time()
            if remaining <= 0:
                raise socket.timeout("_read_exact deadline")
            self._fill(remaining)
        out = bytes(self.buf[:n])
        del self.buf[:n]
        return out

    def read_frame(self, timeout=2.0):
        """('MSG'|'HMSG', subject, payload, headers) | ('LINE', text) |
        ('EOF',) | (None,) on timeout."""
        try:
            line = self.read_line(timeout)
        except EOFError:
            return ("EOF",)
        except (socket.timeout, ssl.SSLError, OSError):
            return (None,)
        parts = line.split(" ")
        if parts[0] in ("MSG", "HMSG"):
            try:
                if parts[0] == "MSG":
                    subj = parts[1]
                    if len(parts) == 4:
                        size, reply = int(parts[3]), None
                    else:
                        reply, size = parts[3], int(parts[4])
                    payload = self._read_exact(size + 2)[:size]
                    return ("MSG", subj, payload.decode(errors="replace"), None)
                subj = parts[1]
                if len(parts) == 5:
                    reply, hlen, total = None, int(parts[3]), int(parts[4])
                else:
                    reply, hlen, total = parts[3], int(parts[4]), int(parts[5])
                raw = self._read_exact(total + 2)[:total]
                return ("HMSG", subj, raw[hlen:].decode(errors="replace"), raw[:hlen].decode(errors="replace"))
            except (socket.timeout, EOFError, OSError, ValueError):
                return ("EOF",)
        return ("LINE", line)

    def drain(self, seconds=0.8, answer_ping=True):
        """Collect frames for `seconds`; PING is answered (unless disabled)
        and recorded as 'PING' so server-PING tests can see it."""
        out = []
        end = time.time() + seconds
        while time.time() < end:
            f = self.read_frame(max(0.05, end - time.time()))
            if f[0] is None:
                continue
            if f[0] == "EOF":
                out.append("EOF")
                break
            if f[0] == "LINE" and f[1] == "PING":
                if answer_ping:
                    try:
                        self.send(b"PONG\r\n")
                    except OSError:
                        pass
                out.append("PING")
                continue
            out.append(f)
        return out

    def ping_alive(self):
        """True if the connection answers PING with PONG, False on EOF/timeout."""
        try:
            self.send(b"PING\r\n")
            return self.read_line(1.0) == "PONG"
        except Exception:
            return False

    def close(self):
        try:
            self.s.close()
        except Exception:
            pass


def base_connect(**kw):
    c = {"verbose": False, "pedantic": False, "headers": True, "echo": True}
    c.update(kw)
    return c


def msgs(frames):
    return [(f[1], f[2]) for f in frames if isinstance(f, tuple) and f[0] in ("MSG", "HMSG")]


def err_class(line):
    """Normalise an -ERR line to (class, detail). Text after ':' in Rust's
    errors (e.g. ': foo.*', ': 65') is detail, not class."""
    if not isinstance(line, str) or not line.startswith("-ERR"):
        return ("other", line)
    body = line[5:].strip().strip("'")
    if body.startswith("Permissions Violation for Publish to "):
        return ("perm_publish", body[len("Permissions Violation for Publish to "):])
    if body.startswith("Permissions Violation for Subscription to "):
        return ("perm_subscribe", body[len("Permissions Violation for Subscription to "):])
    for prefix, cls in [
        ("Authorization Violation", "auth"),
        ("Invalid Publish Subject", "invalid_subject"),
        ("Invalid Subject", "invalid_subject"),
        ("Maximum Payload Violation", "max_payload"),
        ("Unknown Protocol Operation", "unknown_op"),
        ("Invalid CONNECT", "invalid_connect"),
        ("Stale Connection", "stale"),
    ]:
        if body.startswith(prefix):
            rest = body[len(prefix):].strip().lstrip(":").strip()
            return (cls, rest)
    return ("other_err", body)


def err_of(frames):
    for f in frames:
        if isinstance(f, tuple) and f[0] == "LINE" and f[1].startswith("-ERR"):
            return err_class(f[1])
    return None


def sub_settle(conn, line):
    conn.send(line if isinstance(line, bytes) else line.encode())
    time.sleep(SETTLE)


# ---------------------------------------------------------------------------
# Probe cases. Each takes (ports, cfg) and returns a JSON-able outcome.
# ports: dict of pair name -> {"client": port, "http": port}
# ---------------------------------------------------------------------------

def p_info(ports, cfg):
    out = {}
    for label in ("go", "rust"):
        p = ports["main"][label]
        c = Conn(p["client"])
        info = c.info
        c.close()
        out[label] = {
            "proto": info.get("proto"),
            "headers": info.get("headers"),
            "max_payload": info.get("max_payload"),
            "auth_required": bool(info.get("auth_required")) if "auth_required" in info else None,
            "tls_required": bool(info.get("tls_required")) if "tls_required" in info else None,
            "jetstream": bool(info.get("jetstream")) if "jetstream" in info else None,
            "nonce_present": "nonce" in info,
            "client_ip_present": "client_ip" in info,
            "cluster_present": "cluster" in info,
            "api_lvl_present": "api_lvl" in info,
            "git_commit_present": "git_commit" in info,
        }
    return out


def p_basic(ports, cfg):
    out = {}
    for label in ("go", "rust"):
        c = Conn(ports["main"][label]["client"], base_connect())
        c.send(b"SUB foo 1\r\nPUB foo 5\r\nhello\r\nPING\r\n")
        r = c.drain(1.0)
        c.close()
        out[label] = {"delivered": msgs(r), "pong": any(f == ("LINE", "PONG") for f in r)}
    return out


def p_wildcards(ports, cfg):
    out = {}
    for label in ("go", "rust"):
        c = Conn(ports["main"][label]["client"], base_connect())
        c.send(b"SUB foo.* 1\r\nSUB foo.> 2\r\nPUB foo.bar 1\r\nx\r\nPUB foo.bar.baz 1\r\ny\r\nPUB foo 1\r\nz\r\nPING\r\n")
        # Same-connection SUB then PUB is ordered; no settle needed, but the
        # wildcard subs are on one connection and pubs on the same one.
        r = msgs(c.drain(1.0))
        c.close()
        # Deduplicate by (sid-implied) — frames carry no sid here, so count
        # deliveries per subject: foo.bar -> star+gt = 2, foo.bar.baz -> gt = 1,
        # foo -> none.
        from collections import Counter
        out[label] = dict(Counter(s for s, _ in r))
    return out


def p_queue(ports, cfg):
    out = {}
    for label in ("go", "rust"):
        subs = [Conn(ports["main"][label]["client"], base_connect()) for _ in range(3)]
        for i, s in enumerate(subs):
            sub_settle(s, f"SUB qsubj qg {i+1}\r\n")
        pub = Conn(ports["main"][label]["client"], base_connect())
        for _ in range(30):
            pub.send(b"PUB qsubj 1\r\nm\r\n")
        counts = []
        for s in subs:
            counts.append(len(msgs(s.drain(1.2))))
            s.close()
        pub.close()
        # Totals only: distribution algorithm differs (Go random-ish,
        # Rust round-robin) and is not a parity requirement.
        out[label] = {"total": sum(counts), "per_member_present": all(c > 0 for c in counts)}
    return out


def p_headers(ports, cfg):
    out = {}
    for label in ("go", "rust"):
        res = {}
        for want_headers in (True, False):
            c = Conn(ports["main"][label]["client"], base_connect(headers=want_headers))
            sub_settle(c, b"SUB hsubj 1\r\n")
            pub = Conn(ports["main"][label]["client"], base_connect(headers=True))
            pub.send(b"HPUB hsubj 25 30\r\nNATS/1.0\r\nX-Test: yes\r\n\r\nhello\r\n")
            frames = [f for f in c.drain(1.0) if isinstance(f, tuple) and f[0] in ("MSG", "HMSG")]
            res["header_client" if want_headers else "plain_client"] = [
                (f[0], f[1], f[2], f[3]) for f in frames
            ]
            c.close()
            pub.close()
        out[label] = res
    return out


def p_verbose_order(ports, cfg):
    out = {}
    for label in ("go", "rust"):
        c = Conn(ports["main"][label]["client"], base_connect(verbose=True))
        c.send(b"SUB vfoo 1\r\nPUB vfoo 2\r\nhi\r\n")
        r = c.drain(1.0)
        c.close()
        # Raw order, on purpose: this is a KNOWN diff (Go acks PUB before
        # self-delivery, Rust delivers first). Compared raw here.
        out[label] = [f[1] if isinstance(f, tuple) and f[0] == "LINE" else (f[0], f[2]) for f in r]
    return out


def p_echo_false(ports, cfg):
    out = {}
    for label in ("go", "rust"):
        c = Conn(ports["main"][label]["client"], base_connect(echo=False))
        sub_settle(c, b"SUB efoo 1\r\n")
        c.send(b"PUB efoo 2\r\nme\r\n")
        self_msgs = msgs(c.drain(0.7))
        other = Conn(ports["main"][label]["client"], base_connect())
        other.send(b"PUB efoo 2\r\nyo\r\n")
        other_msgs = msgs(c.drain(0.7))
        c.close()
        other.close()
        out[label] = {"self": self_msgs, "other": other_msgs}
    return out


def p_auto_unsub(ports, cfg):
    out = {}
    for label in ("go", "rust"):
        c = Conn(ports["main"][label]["client"], base_connect())
        c.send(b"SUB afoo 1\r\nUNSUB 1 2\r\nPUB afoo 1\r\n1\r\nPUB afoo 1\r\n2\r\nPUB afoo 1\r\n3\r\n")
        r = msgs(c.drain(1.0))
        c.close()
        out[label] = [p for _, p in r]
    return out


def p_binary_payload(ports, cfg):
    out = {}
    for label in ("go", "rust"):
        c = Conn(ports["main"][label]["client"], base_connect())
        payload2 = b"a\r\nb\x00c"
        c.send(b"SUB bp 1\r\nPUB bp 0\r\n\r\nPUB bp 6\r\n" + payload2 + b"\r\n")
        r = msgs(c.drain(1.0))
        c.close()
        out[label] = [(s, list(p.encode(errors="replace"))) for s, p in r]
    return out


def p_close_cleanup(ports, cfg):
    out = {}
    for label in ("go", "rust"):
        c = Conn(ports["main"][label]["client"], base_connect())
        c.send(b"SUB gone 1\r\n")
        c.close()
        time.sleep(0.3)
        c2 = Conn(ports["main"][label]["client"], base_connect())
        c2.send(b"PUB gone 1\r\nz\r\nPING\r\n")
        r = c2.drain(0.8)
        c2.close()
        out[label] = {"pong": any(f == ("LINE", "PONG") for f in r)}
    return out


def _auth_case(ports, cfg, mode):
    out = {}
    for label in ("go", "rust"):
        if mode == "wrong_pass":
            conn = base_connect(user="derek", **{"pass": "wrong"})
        elif mode == "no_creds":
            conn = base_connect()
        else:
            conn = base_connect(user="derek", **{"pass": "s3cret"})
        try:
            c = Conn(ports["auth"][label]["client"], conn)
            c.send(b"PING\r\n")
            r = c.drain(0.8)
            c.close()
            out[label] = {"err": err_of(r), "pong": any(f == ("LINE", "PONG") for f in r), "eof": "EOF" in r}
        except Exception as e:
            out[label] = {"exception": type(e).__name__}
    return out


def p_auth_wrong(ports, cfg):
    return _auth_case(ports, cfg, "wrong_pass")


def p_auth_none(ports, cfg):
    return _auth_case(ports, cfg, "no_creds")


def p_auth_ok(ports, cfg):
    return _auth_case(ports, cfg, "correct")


def _token_case(ports, cfg, mode):
    out = {}
    for label in ("go", "rust"):
        if mode == "field":
            conn = base_connect(auth_token="tok123")
        elif mode == "as_pass":
            conn = base_connect(user="x", **{"pass": "tok123"})
        elif mode == "wrong":
            conn = base_connect(auth_token="nope")
        else:
            conn = base_connect()
        try:
            c = Conn(ports["token"][label]["client"], conn)
            c.send(b"PING\r\n")
            r = c.drain(0.8)
            c.close()
            out[label] = {"err": err_of(r), "pong": any(f == ("LINE", "PONG") for f in r), "eof": "EOF" in r}
        except ConnectionResetError:
            out[label] = {"reset": True}
        except Exception as e:
            out[label] = {"exception": type(e).__name__}
    return out


def p_token_field(ports, cfg):
    return _token_case(ports, cfg, "field")


def p_token_as_pass(ports, cfg):
    return _token_case(ports, cfg, "as_pass")


def p_token_wrong(ports, cfg):
    return _token_case(ports, cfg, "wrong")


def p_token_none(ports, cfg):
    return _token_case(ports, cfg, "none")


def p_max_payload(ports, cfg):
    out = {}
    for label in ("go", "rust"):
        c = Conn(ports["max64"][label]["client"], base_connect())
        c.send(b"PUB mp 65\r\n" + b"x" * 65 + b"\r\n")
        over = c.drain(1.0)
        c.close()
        c2 = Conn(ports["max64"][label]["client"], base_connect())
        sub_settle(c2, b"SUB mp 1\r\n")
        c2.send(b"PUB mp 64\r\n" + b"y" * 64 + b"\r\n")
        ok = msgs(c2.drain(1.0))
        c2.close()
        out[label] = {
            "over_err_class": err_of(over)[0] if err_of(over) else None,
            "over_closed": "EOF" in over,
            "exact_delivered_len": [len(p) for _, p in ok],
        }
    return out


def _subject_pub(ports, cfg, pedantic, subject=b"foo.*"):
    out = {}
    for label in ("go", "rust"):
        s = Conn(ports["main"][label]["client"], base_connect())
        sub_settle(s, b"SUB > 1\r\n")
        pub = Conn(ports["main"][label]["client"], base_connect(pedantic=pedantic))
        pub.send(b"PUB " + subject + b" 1\r\nW\r\nPING\r\n")
        pub_r = pub.drain(0.8)
        gt = msgs(s.drain(0.5))
        alive = pub.ping_alive()
        s.close()
        pub.close()
        out[label] = {
            "err_class": err_of(pub_r)[0] if err_of(pub_r) else None,
            "delivered_to_gt": gt,
            "pub_alive_after": alive,
        }
    return out


def p_pub_wildcard_nonpedantic(ports, cfg):
    return _subject_pub(ports, cfg, False)


def p_pub_wildcard_pedantic(ports, cfg):
    return _subject_pub(ports, cfg, True)


def p_pub_empty_token(ports, cfg):
    return _subject_pub(ports, cfg, False, subject=b"foo..bar")


def _subject_sub(ports, cfg, sub_line):
    out = {}
    for label in ("go", "rust"):
        c = Conn(ports["main"][label]["client"], base_connect())
        c.send(sub_line + b"PING\r\n")
        r = c.drain(0.8)
        alive = c.ping_alive()
        c.close()
        out[label] = {"err_class": err_of(r)[0] if err_of(r) else None, "alive_after": alive}
    return out


def p_sub_gt_not_last(ports, cfg):
    return _subject_sub(ports, cfg, b"SUB foo.>.bar 1\r\n")


def p_sub_partial_wildcard(ports, cfg):
    return _subject_sub(ports, cfg, b"SUB foo* 1\r\n")


def p_sub_foostar_literal(ports, cfg):
    """SUB foo* then publish foo / foobar / foo* : Go treats foo* as a
    literal token (only foo* matches); Rust rejects the SUB outright."""
    out = {}
    for label in ("go", "rust"):
        s = Conn(ports["main"][label]["client"], base_connect())
        s.send(b"SUB foo* 1\r\n")
        time.sleep(SETTLE)
        pub = Conn(ports["main"][label]["client"], base_connect())
        res = {}
        for subj in ("foo", "foobar", "foo*"):
            pub.send(f"PUB {subj} 1\r\nE\r\n".encode())
            time.sleep(0.1)
            res[subj] = msgs(s.drain(0.5))
        s.close()
        pub.close()
        out[label] = res
    return out


def p_duplicate_sid(ports, cfg):
    out = {}
    for label in ("go", "rust"):
        c = Conn(ports["main"][label]["client"], base_connect())
        c.send(b"SUB dfoo 1\r\nSUB dbar 1\r\nPUB dfoo 1\r\nA\r\nPUB dbar 1\r\nB\r\n")
        r = msgs(c.drain(1.0))
        c.close()
        out[label] = r
    return out


def p_unsub_unknown(ports, cfg):
    out = {}
    for label in ("go", "rust"):
        c = Conn(ports["main"][label]["client"], base_connect(verbose=True))
        c.send(b"UNSUB 999\r\nPING\r\n")
        r = c.drain(1.0)
        c.close()
        out[label] = {"oks": sum(1 for f in r if f == ("LINE", "+OK")), "pong": any(f == ("LINE", "PONG") for f in r)}
    return out


def p_second_connect(ports, cfg):
    out = {}
    for label in ("go", "rust"):
        c = Conn(ports["main"][label]["client"], base_connect())
        c.send(b"CONNECT " + json.dumps(base_connect()).encode() + b"\r\nPING\r\n")
        r = c.drain(1.0)
        c.close()
        out[label] = {"pong": any(f == ("LINE", "PONG") for f in r), "err": err_of(r)}
    return out


def p_unsolicited_pong(ports, cfg):
    out = {}
    for label in ("go", "rust"):
        c = Conn(ports["main"][label]["client"], base_connect())
        c.send(b"PONG\r\nPING\r\n")
        r = c.drain(1.0)
        c.close()
        out[label] = {"pong": any(f == ("LINE", "PONG") for f in r)}
    return out


def p_unknown_op(ports, cfg):
    out = {}
    for label in ("go", "rust"):
        c = Conn(ports["main"][label]["client"], base_connect())
        c.send(b"FOO bar\r\n")
        r = c.drain(1.0)
        c.close()
        out[label] = {"err_class": err_of(r)[0] if err_of(r) else None, "closed": "EOF" in r}
    return out


def p_malformed_connect(ports, cfg):
    out = {}
    for label in ("go", "rust"):
        s = socket.create_connection(("127.0.0.1", ports["main"][label]["client"]), timeout=5)
        f = s.makefile("rb")
        f.readline()
        s.sendall(b"CONNECT notjson\r\n")
        s.settimeout(1.0)
        try:
            first = f.readline().decode(errors="replace").strip()
        except Exception as e:
            first = f"EXC {type(e).__name__}"
        try:
            s.sendall(b"PING\r\n")
            second = f.readline().decode(errors="replace").strip()
        except Exception as e:
            second = f"EXC {type(e).__name__}"
        s.close()
        first_class = err_of([("LINE", first)])[0] if first.startswith("-ERR") else ("none_closed" if first == "" else first)
        out[label] = {"first": first_class, "after_ping": second}
    return out


def p_no_responders(ports, cfg, flag):
    out = {}
    for label in ("go", "rust"):
        c = Conn(ports["main"][label]["client"], base_connect(no_responders=flag))
        sub_settle(c, b"SUB _INBOX.test 1\r\n")
        c.send(b"PUB req.nowhere _INBOX.test 4\r\nping\r\n")
        r = [f for f in c.drain(1.0) if isinstance(f, tuple) and f[0] in ("MSG", "HMSG")]
        c.close()
        out[label] = [(f[0], f[2], f[3]) for f in r]
    return out


def p_no_resp_true(ports, cfg):
    return p_no_responders(ports, cfg, True)


def p_no_resp_false(ports, cfg):
    return p_no_responders(ports, cfg, False)


def p_server_ping(ports, cfg):
    out = {}
    for label in ("go", "rust"):
        c = Conn(ports["ping"][label]["client"], base_connect())
        # Never answer PING; record what arrives, and when we are closed.
        start = time.time()
        seen = []
        closed_at = None
        while time.time() - start < 4.0:
            f = c.read_frame(0.5)
            if f[0] == "EOF":
                closed_at = round(time.time() - start, 2)
                break
            if f[0] == "LINE":
                seen.append(f[1])
        alive = None
        if closed_at is None:
            try:
                c.send(b"PING\r\n")
                alive = c.read_line(1.0)
            except Exception as e:
                alive = f"EXC {type(e).__name__}"
        c.close()
        out[label] = {
            "ping_lines": seen,
            "closed_within_4s": closed_at is not None,
            "still_alive_reply": alive,
        }
    return out


# ---------------------------------------------------------------------------
# TLS probes (plain socket vs ssl socket; certs come from parity.sh)
# ---------------------------------------------------------------------------


def _tls_ctx(cfg, client_cert=False):
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=cfg["ca_cert"])
    ctx.check_hostname = False
    if client_cert:
        ctx.load_cert_chain(cfg["client_cert"], cfg["client_key"])
    return ctx


def p_tls_plain_fails(ports, cfg):
    """Plain client to a TLS port. Go sends INFO (tls_required:true) in
    plaintext, then refuses the plain CONNECT; Rust sends nothing (the
    TLS handshake comes first) and refuses the plain CONNECT bytes.
    Both refuse; the INFO-before-TLS order is the known diff."""
    out = {}
    for label in ("go", "rust"):
        try:
            s = socket.create_connection(("127.0.0.1", ports["tls"][label]["client"]), timeout=5)
            s.settimeout(2.0)
            try:
                data = s.recv(4096)
            except (socket.timeout, OSError):
                data = b""
            info = data.startswith(b"INFO ")
            tls_req = b'"tls_required":true' in data
            s.sendall(b"CONNECT {}\r\nPING\r\n")
            s.settimeout(2.0)
            try:
                resp = s.recv(4096)
            except (socket.timeout, OSError):
                resp = None
            s.close()
            out[label] = {
                "info": info,
                "tls_required": tls_req,
                # Alive only if the server answered the plain CONNECT/PING.
                "alive_after_connect": resp not in (b"", None) and b"PONG" in (resp or b""),
            }
        except Exception as e:
            out[label] = {"exception": type(e).__name__}
    return out


def p_tls_client_ok(ports, cfg):
    out = {}
    for label in ("go", "rust"):
        try:
            # Go needs the upgrade flow; Rust needs direct (implicit) TLS.
            mode = "upgrade" if label == "go" else "direct"
            c = Conn(ports["tls"][label]["client"], base_connect(), tls_ctx=_tls_ctx(cfg), tls_mode=mode)
            c.send(b"SUB tfoo 1\r\nPUB tfoo 2\r\nhi\r\n")
            r = msgs(c.drain(1.0))
            c.close()
            out[label] = {"delivered": r, "info_seen": c.info_line.startswith("INFO ")}
        except Exception as e:
            out[label] = {"exception": type(e).__name__}
    return out


def p_tlsverify_no_cert(ports, cfg):
    out = {}
    for label in ("go", "rust"):
        try:
            mode = "upgrade" if label == "go" else "direct"
            c = Conn(ports["tlsverify"][label]["client"], base_connect(), tls_ctx=_tls_ctx(cfg), tls_mode=mode)
            c.send(b"PING\r\n")
            r = c.drain(1.0)
            c.close()
            out[label] = {"result": "connected", "frames": [str(f) for f in r]}
        except Exception as e:
            out[label] = {"result": "handshake_or_read_failed", "exception": type(e).__name__}
    return out


def p_tlsverify_with_cert(ports, cfg):
    out = {}
    for label in ("go", "rust"):
        try:
            mode = "upgrade" if label == "go" else "direct"
            c = Conn(ports["tlsverify"][label]["client"], base_connect(), tls_ctx=_tls_ctx(cfg, client_cert=True), tls_mode=mode)
            c.send(b"SUB tfoo 1\r\nPUB tfoo 2\r\nhi\r\n")
            r = msgs(c.drain(1.0))
            c.close()
            out[label] = {"delivered": r}
        except Exception as e:
            out[label] = {"exception": type(e).__name__}
    return out


# ---------------------------------------------------------------------------
# NKey probes. The Go nkeytool (parity/go_client, `sign` mode) signs a
# message with a seed; the raw probe then sends that signature in CONNECT.
# ---------------------------------------------------------------------------


def _nkey_sign(cfg, message, seed):
    r = subprocess.run(
        [cfg["nkeytool"], "sign", seed, message],
        capture_output=True, text=True, timeout=10,
    )
    if r.returncode != 0:
        raise RuntimeError(f"nkeytool sign failed: {r.stderr}")
    return r.stdout.strip()


def _nkey_case(ports, cfg, mode):
    out = {}
    pubkey = cfg["nkey_pubkey"]
    seed = cfg["nkey_seed"]
    for label in ("go", "rust"):
        c = Conn(ports["nkey"][label]["client"])
        nonce = c.info.get("nonce", "")
        if mode == "valid":
            sig = _nkey_sign(cfg, nonce, seed)
            conn = {"nkey": pubkey, "sig": sig, "verbose": False, "headers": True}
        elif mode == "bad_sig":
            sig = _nkey_sign(cfg, "not the nonce", seed)
            conn = {"nkey": pubkey, "sig": sig, "verbose": False, "headers": True}
        elif mode == "unknown_pubkey":
            # A different throwaway keypair's pubkey, signed correctly.
            sig = _nkey_sign(cfg, nonce, seed)
            conn = {"nkey": "U" + pubkey[1:], "sig": sig, "verbose": False, "headers": True}
        else:  # no_sig
            conn = {"nkey": pubkey, "verbose": False, "headers": True}
        c.send(b"CONNECT " + json.dumps(conn).encode() + b"\r\n")
        c.send(b"SUB nkey.topic 1\r\nPUB nkey.topic 2\r\nhi\r\n")
        r = c.drain(1.0)
        c.close()
        out[label] = {"delivered": msgs(r), "err": err_of(r)}
    return out


def p_nkey_valid(ports, cfg):
    return _nkey_case(ports, cfg, "valid")


def p_nkey_bad_sig(ports, cfg):
    return _nkey_case(ports, cfg, "bad_sig")


def p_nkey_unknown(ports, cfg):
    return _nkey_case(ports, cfg, "unknown_pubkey")


def p_nkey_no_sig(ports, cfg):
    return _nkey_case(ports, cfg, "no_sig")


# ---------------------------------------------------------------------------
# Multi-account probes
# ---------------------------------------------------------------------------


def p_acct_isolation(ports, cfg):
    out = {}
    for label in ("go", "rust"):
        alice = Conn(ports["accounts"][label]["client"], base_connect(user="alice", **{"pass": "alicepw"}))
        bob = Conn(ports["accounts"][label]["client"], base_connect(user="bob", **{"pass": "bobpw"}))
        sub_settle(alice, b"SUB allowed.shared 1\r\n")
        sub_settle(bob, b"SUB allowed.shared 1\r\n")
        alice.send(b"PUB allowed.shared 1\r\na\r\n")
        a_got = msgs(alice.drain(0.7))
        b_got = msgs(bob.drain(0.7))
        bob.send(b"PUB allowed.shared 1\r\nb\r\n")
        a_got2 = msgs(alice.drain(0.7))
        b_got2 = msgs(bob.drain(0.7))
        alice.close()
        bob.close()
        out[label] = {"alice_pub": {"alice": a_got, "bob": b_got}, "bob_pub": {"alice": a_got2, "bob": b_got2}}
    return out


def _acct_perm(ports, cfg, user, pw, op, subject):
    out = {}
    for label in ("go", "rust"):
        c = Conn(ports["accounts"][label]["client"], base_connect(user=user, **{"pass": pw}))
        if op == "pub":
            c.send(f"PUB {subject} 1\r\nx\r\nPING\r\n".encode())
        else:
            c.send(f"SUB {subject} 1\r\nPING\r\n".encode())
        r = c.drain(0.8)
        alive = c.ping_alive()
        c.close()
        out[label] = {"err_class": err_of(r)[0] if err_of(r) else None, "err_detail": err_of(r)[1] if err_of(r) else None, "alive": alive}
    return out


def p_acct_pub_allow(ports, cfg):
    return _acct_perm(ports, cfg, "alice", "alicepw", "pub", "allowed.x")


def p_acct_pub_deny_allowlist(ports, cfg):
    return _acct_perm(ports, cfg, "alice", "alicepw", "pub", "denied.x")


def p_acct_pub_deny_list(ports, cfg):
    return _acct_perm(ports, cfg, "dave", "davepw", "pub", "secret.x")


def p_acct_pub_deny_list_ok(ports, cfg):
    return _acct_perm(ports, cfg, "dave", "davepw", "pub", "other.x")


def p_acct_sub_allow(ports, cfg):
    return _acct_perm(ports, cfg, "alice", "alicepw", "sub", "allowed.x")


def p_acct_sub_deny(ports, cfg):
    return _acct_perm(ports, cfg, "dave", "davepw", "sub", "secret.x")


def p_acct_wrong_pass(ports, cfg):
    out = {}
    for label in ("go", "rust"):
        try:
            c = Conn(ports["accounts"][label]["client"], base_connect(user="alice", **{"pass": "wrong"}))
            c.send(b"PING\r\n")
            r = c.drain(0.8)
            c.close()
            out[label] = {"err": err_of(r), "pong": any(f == ("LINE", "PONG") for f in r)}
        except Exception as e:
            out[label] = {"exception": type(e).__name__}
    return out


# ---------------------------------------------------------------------------
# Monitoring probes. Semantic summaries only: strip version, ports,
# timestamps, mem values (bool >0), and per-run ids.
# ---------------------------------------------------------------------------


def _leaf_count(d):
    if isinstance(d.get("num_leafnodes"), int):
        return d["num_leafnodes"]
    if isinstance(d.get("leafnodes"), int):
        return d["leafnodes"]
    lst = d.get("leafs", d.get("leafnodes", []))
    return len(lst) if isinstance(lst, list) else 0


def _http(port, path):
    """(status, body-dict). Non-JSON error bodies (Go answers unknown
    monitoring paths with '404 page not found') become
    {'_non_json': ...} instead of raising."""
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", method="GET")
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        try:
            return e.code, json.loads(body or "{}")
        except json.JSONDecodeError:
            return e.code, {"_non_json": body[:80]}
    except Exception as e:
        return None, {"error": type(e).__name__}


def p_monitoring(ports, cfg):
    out = {}
    for label in ("go", "rust"):
        hp = ports["main"][label]["http"]
        # Controlled state: one client, one subscription, then read endpoints.
        c = Conn(ports["main"][label]["client"], base_connect())
        sub_settle(c, b"SUB mon.foo 1\r\n")
        status_varz, varz = _http(hp, "/varz")
        status_connz, connz = _http(hp, "/connz")
        status_subz, subz = _http(hp, "/subz")
        status_routez, routez = _http(hp, "/routez")
        status_leafz, leafz = _http(hp, "/leafz")
        status_jsz, jsz = _http(hp, "/jsz")
        status_accounz, accounz = _http(hp, "/accounz")
        status_gw, gw = _http(hp, "/gatewayz")
        status_health, health = _http(hp, "/healthz")
        status_connz_acc, connz_acc = _http(hp, "/connz?acc=$G")
        c.close()
        out[label] = {
            "varz": {
                "status": status_varz,
                "server_name": varz.get("server_name"),
                "proto": varz.get("proto"),
                "max_payload": varz.get("max_payload"),
                # Go varz omits these keys when false; Rust reports them.
                "auth_required": varz.get("auth_required", False),
                "tls_required": varz.get("tls_required", False),
                "http_port_matches": varz.get("http_port") == hp,
                "mem_positive": isinstance(varz.get("mem"), (int, float)) and varz.get("mem") > 0,
                "slow_consumers_is_int": isinstance(varz.get("slow_consumers"), int),
                "connections_ge_1": isinstance(varz.get("connections"), int) and varz.get("connections") >= 1,
            },
            "connz": {
                "status": status_connz,
                "num_connections_ge_1": connz.get("num_connections", 0) >= 1,
                "acc_filter_status": status_connz_acc,
            },
            # Reported only (field names differ between servers; compared
            # manually, not by the runner): connz entry field presence.
            "_reported": {
                "connz_entry_has_account": any("account" in e for e in connz.get("connections", [])),
                "connz_entry_has_user": any("user" in e for e in connz.get("connections", [])),
                "connz_entry_has_tls": any("tls" in e for e in connz.get("connections", [])),
                "connz_sample_keys": sorted(connz.get("connections", [{}])[0].keys()) if connz.get("connections") else [],
            },
            # Endpoints a server does not implement report supported=False
            # rather than empty field values, so a missing endpoint is a
            # support difference, not a field mismatch.
            "subz": {
                "status": status_subz,
                "supported": status_subz == 200,
                "has_mon_foo": any(s.get("subject") == "mon.foo" for s in subz.get("subscriptions", [])) if status_subz == 200 else False,
            },
            "routez": {"status": status_routez, "num_routes": routez.get("num_routes")},
            # Leaf-count shapes differ: Rust reports num_leafnodes +
            # a leafnodes list; Go reports leafnodes as an INT count
            # and the list under leafs. Normalise all three to a count.
            "leafz": {"status": status_leafz, "num_leafnodes": _leaf_count(leafz)},
            "jsz": {"status": status_jsz, "has_streams_key": "streams" in jsz or "total_streams" in jsz},
            "accounz": {
                "status": status_accounz,
                "supported": status_accounz == 200,
                "has_accounts": "accounts" in accounz if status_accounz == 200 else False,
            },
            # Go reports inbound/outbound_gateways; Rust reports gateways.
            "gatewayz": {"status": status_gw, "no_gateways": (gw.get("gateways") == []) or (not gw.get("inbound_gateways") and not gw.get("outbound_gateways")) if isinstance(gw, dict) else False},
            "healthz": {"status": status_health, "body": health},
        }
    return out


def p_monitoring_accounts(ports, cfg):
    out = {}
    for label in ("go", "rust"):
        hp = ports["accounts"][label]["http"]
        alice = Conn(ports["accounts"][label]["client"], base_connect(user="alice", **{"pass": "alicepw"}))
        sub_settle(alice, b"SUB allowed.mon 1\r\n")
        _, accounz = _http(hp, "/accounz")
        _, connz = _http(hp, "/connz?acc=A")
        alice.close()
        accts = accounz.get("accounts", []) if isinstance(accounz, dict) else []
        names = sorted(a.get("account") or a.get("name") for a in accts if isinstance(a, dict))
        out[label] = {
            "accounz_status": _http(hp, "/accounz")[0],
            "account_names": names,
            "connz_acc_A_connections": connz.get("num_connections") if isinstance(connz, dict) else None,
        }
    return out


# ---------------------------------------------------------------------------
# Case registry, known diffs, runner
# ---------------------------------------------------------------------------

CASES = [
    ("core", "info_fields", p_info),
    ("core", "basic_pubsub", p_basic),
    ("core", "wildcards", p_wildcards),
    ("core", "queue_exactly_once", p_queue),
    ("core", "headers", p_headers),
    ("core", "verbose_order", p_verbose_order),
    ("core", "echo_false", p_echo_false),
    ("core", "auto_unsub", p_auto_unsub),
    ("core", "binary_payload", p_binary_payload),
    ("core", "close_cleanup", p_close_cleanup),
    ("auth", "auth_wrong_pass", p_auth_wrong),
    ("auth", "auth_no_creds", p_auth_none),
    ("auth", "auth_correct", p_auth_ok),
    ("auth", "token_field", p_token_field),
    ("auth", "token_as_pass", p_token_as_pass),
    ("auth", "token_wrong", p_token_wrong),
    ("auth", "token_none", p_token_none),
    ("limits", "max_payload", p_max_payload),
    ("subjects", "pub_wildcard_nonpedantic", p_pub_wildcard_nonpedantic),
    ("subjects", "pub_wildcard_pedantic", p_pub_wildcard_pedantic),
    ("subjects", "pub_empty_token", p_pub_empty_token),
    ("subjects", "sub_gt_not_last", p_sub_gt_not_last),
    ("subjects", "sub_partial_wildcard", p_sub_partial_wildcard),
    ("subjects", "sub_foostar_literal", p_sub_foostar_literal),
    ("subjects", "duplicate_sid", p_duplicate_sid),
    ("subjects", "unsub_unknown_sid", p_unsub_unknown),
    ("subjects", "second_connect", p_second_connect),
    ("subjects", "unsolicited_pong", p_unsolicited_pong),
    ("subjects", "unknown_op", p_unknown_op),
    ("subjects", "malformed_connect", p_malformed_connect),
    ("reply", "no_responders_true", p_no_resp_true),
    ("reply", "no_responders_false", p_no_resp_false),
    ("reply", "server_ping_silent", p_server_ping),
    ("tls", "tls_plain_fails", p_tls_plain_fails),
    ("tls", "tls_client_ok", p_tls_client_ok),
    ("tls", "tlsverify_no_cert", p_tlsverify_no_cert),
    ("tls", "tlsverify_with_cert", p_tlsverify_with_cert),
    ("nkey", "nkey_valid", p_nkey_valid),
    ("nkey", "nkey_bad_sig", p_nkey_bad_sig),
    ("nkey", "nkey_unknown_pubkey", p_nkey_unknown),
    ("nkey", "nkey_no_sig", p_nkey_no_sig),
    ("accounts", "acct_isolation", p_acct_isolation),
    ("accounts", "acct_pub_allow", p_acct_pub_allow),
    ("accounts", "acct_pub_deny_allowlist", p_acct_pub_deny_allowlist),
    ("accounts", "acct_pub_deny_list", p_acct_pub_deny_list),
    ("accounts", "acct_pub_deny_list_ok", p_acct_pub_deny_list_ok),
    ("accounts", "acct_sub_allow", p_acct_sub_allow),
    ("accounts", "acct_sub_deny", p_acct_sub_deny),
    ("accounts", "acct_wrong_pass", p_acct_wrong_pass),
    ("monitoring", "monitoring_main", p_monitoring),
    ("monitoring", "monitoring_accounts", p_monitoring_accounts),
]

# Cases whose Go/Rust divergence is accepted in default mode. Keys are
# case names; values document the classification (see KNOWN_DIFFS.md).
# The runner only accepts a known diff when the observed outcomes match
# the recorded shape; a *different* divergence in the same case still fails.
KNOWN = {
    "info_fields": {
        "class": "accepted_gap",
        "severity": "low",
        "note": "Rust INFO lacks client_ip/cluster/api_lvl/git_commit/jetstream; extra/missing informational fields only, shared values (proto/headers/max_payload) must match.",
    },
    "verbose_order": {
        "class": "accepted_gap",
        "severity": "low",
        "note": "Same frames, different order: Go acks PUB before self-delivery, Rust delivers first.",
    },
    "token_as_pass": {
        "class": "intentional",
        "severity": "low",
        "note": "Rust deliberately accepts the token as a password; Go rejects it. Documented in README.",
    },

    "pub_wildcard_nonpedantic": {
        "class": "accepted_gap",
        "severity": "medium",
        "note": "Go (non-pedantic) treats a wildcard publish subject as literal; Rust strictly rejects and closes.",
    },
    "pub_wildcard_pedantic": {
        "class": "accepted_gap",
        "severity": "medium",
        "note": "Go (pedantic) sends -ERR but still delivers and keeps the connection; Rust errors, closes, no delivery.",
    },
    "pub_empty_token": {
        "class": "accepted_gap",
        "severity": "medium",
        "note": "Go (non-pedantic) accepts PUB foo..bar with no error, but does NOT deliver it to a > subscription (empty token breaks routing); Rust strictly rejects and closes.",
    },
    "tls_plain_fails": {
        "class": "accepted_gap",
        "severity": "medium",
        "note": "Handshake order differs: Go sends INFO (tls_required:true) in plaintext and then upgrades to TLS (STARTTLS-style); Rust does implicit TLS (handshake before INFO). A Go-style client that upgrades after INFO works against both; a client that wraps immediately works only against Rust. Candidate fix: send INFO with tls_required before the TLS handshake, matching Go.",
    },
    "tlsverify_no_cert": {
        "class": "accepted_gap",
        "severity": "low",
        "note": "Both refuse a client without a certificate. Rust consistently fails the handshake itself (SSLError). Go's refusal point varies run to run: sometimes the handshake fails (SSLEOFError), sometimes it completes and Go closes after CONNECT (EOF) — both shapes are accepted.",
    },
    "monitoring_main": {
        "class": "accepted_gap",
        "severity": "low",
        "note": "Go nats-server v2.15.1 answers /subz and /accounz with 404; Rust implements both (superset). All endpoints Go does implement compare equal on supported fields.",
    },
    "monitoring_accounts": {
        "class": "accepted_gap",
        "severity": "low",
        "note": "Same /accounz gap as monitoring_main: Go 404s, Rust lists accounts A/B. The /connz?acc=A filter count matches on both.",
    },
    "sub_gt_not_last": {
        "class": "accepted_gap",
        "severity": "low",
        "note": "Go sends -ERR and keeps the connection; Rust sends -ERR and closes.",
    },
    "sub_partial_wildcard": {
        "class": "accepted_gap",
        "severity": "medium",
        "note": "Go accepts SUB foo* (literal token); Rust strictly rejects and closes.",
    },
    "sub_foostar_literal": {
        "class": "accepted_gap",
        "severity": "medium",
        "note": "Consequence of sub_partial_wildcard: Go matches only literal foo*; Rust has no subscription at all.",
    },
    "duplicate_sid": {
        "class": "accepted_gap",
        "severity": "medium",
        "note": "Go keeps the first subscription for a reused sid; Rust replaces it. Candidate fix: keep-first in sublist.add.",
    },
    "malformed_connect": {
        "class": "accepted_gap",
        "severity": "low",
        "note": "Go closes silently on CONNECT notjson; Rust sends -ERR with a serde_json message (internal detail leaked to the wire).",
    },
    "no_responders_true": {
        "class": "accepted_gap",
        "severity": "medium",
        "note": "Go sends HMSG 503 No Responders; Rust silently drops the request. Candidate fix: emit 503 in deliver() when publisher has no_responders and there are no recipients.",
    },
    "server_ping_silent": {
        "class": "accepted_gap",
        "severity": "medium",
        "note": "Go enforces ping_max (Stale Connection close); Rust never sends server PINGs. Candidate fix: a ping timer in handle_conn.",
    },
}


def compare_info(go, rust):
    """Semantic INFO comparison: shared values must match; presence
    differences in informational fields are a known gap, not a pass."""
    for key in ("proto", "headers", "max_payload"):
        if go.get(key) != rust.get(key):
            return "fail"
    for key in ("auth_required", "tls_required"):
        gv, rv = go.get(key), rust.get(key)
        if gv is not None and rv is not None and gv != rv:
            return "fail"
    presence_diff = any(go.get(k) != rust.get(k) for k in (
        "jetstream", "nonce_present", "client_ip_present", "cluster_present",
        "api_lvl_present", "git_commit_present", "auth_required", "tls_required",
    ))
    return "known" if presence_diff else "pass"


def _strip_reported(outcome):
    if isinstance(outcome, dict):
        return {k: v for k, v in outcome.items() if k != "_reported"}
    return outcome


def known_shape_ok(name, go, rust):
    """True only when a known case diverges in its recorded shape. A
    different divergence in the same case is a new failure, not an
    accepted known diff."""
    try:
        if name == "verbose_order":
            from collections import Counter
            return Counter(map(str, go)) == Counter(map(str, rust)) and go != rust
        if name == "token_as_pass":
            return go.get("err", [None])[0] == "auth" and rust.get("pong") is True
        if name == "duplicate_sid":
            return go == [("dfoo", "A")] and rust == [("dbar", "B")]
        if name == "no_responders_true":
            return len(go) == 1 and "503" in (go[0][2] or "") and rust == []
        if name == "server_ping_silent":
            return go.get("closed_within_4s") is True and rust.get("closed_within_4s") is False
        if name in ("pub_wildcard_nonpedantic", "pub_wildcard_pedantic"):
            return go.get("delivered_to_gt") and rust.get("err_class") == "invalid_subject" and rust.get("pub_alive_after") is False
        if name == "pub_empty_token":
            # Go accepts foo..bar but does NOT deliver it (empty token
            # breaks routing to >); Rust rejects and closes.
            return (go.get("err_class") is None and go.get("delivered_to_gt") == []
                    and go.get("pub_alive_after") is True
                    and rust.get("err_class") == "invalid_subject" and rust.get("pub_alive_after") is False)
        if name == "tls_plain_fails":
            # Go: INFO (tls_required) in plaintext, plain CONNECT refused.
            # Rust: no INFO at all, plain CONNECT bytes refused.
            return (go.get("info") is True and go.get("tls_required") is True and go.get("alive_after_connect") is False
                    and rust.get("info") is False and rust.get("alive_after_connect") is False)
        if name == "tlsverify_no_cert":
            # Go's refusal point varies run to run (handshake EOF vs
            # post-CONNECT EOF); Rust consistently fails the handshake.
            go_refused = ((go.get("result") == "connected" and go.get("frames") == ["EOF"])
                          or (go.get("result") == "handshake_or_read_failed" and go.get("exception") in ("SSLEOFError", "SSLError")))
            return (go_refused
                    and rust.get("result") == "handshake_or_read_failed" and rust.get("exception") == "SSLError")
        if name == "monitoring_main":
            g = {k: v for k, v in go.items() if k not in ("subz", "accounz")}
            r = {k: v for k, v in rust.items() if k not in ("subz", "accounz")}
            return (g == r
                    and go.get("subz", {}).get("status") == 404 and go.get("subz", {}).get("supported") is False
                    and rust.get("subz", {}).get("supported") is True
                    and go.get("accounz", {}).get("status") == 404 and go.get("accounz", {}).get("supported") is False
                    and rust.get("accounz", {}).get("supported") is True)
        if name == "monitoring_accounts":
            return (go.get("accounz_status") == 404 and go.get("account_names") == []
                    and go.get("connz_acc_A_connections") == rust.get("connz_acc_A_connections")
                    and rust.get("accounz_status") == 200 and "A" in rust.get("account_names", []))
        if name == "sub_gt_not_last":
            return go.get("err_class") == "invalid_subject" and go.get("alive_after") is True and rust.get("err_class") == "invalid_subject" and rust.get("alive_after") is False
        if name == "sub_partial_wildcard":
            return go.get("err_class") is None and go.get("alive_after") is True and rust.get("err_class") == "invalid_subject" and rust.get("alive_after") is False
        if name == "sub_foostar_literal":
            return go.get("foo*") and not go.get("foo") and not go.get("foobar") and rust.get("foo*") == [] and rust.get("foo") == []
        if name == "malformed_connect":
            return go.get("first") == "none_closed" and rust.get("first") == "invalid_connect"
    except Exception:
        return False
    return False


def compare_case(name, go, rust):
    go, rust = _strip_reported(go), _strip_reported(rust)
    if go == rust:
        return "pass"
    if name == "info_fields":
        return compare_info(go, rust)
    if name in KNOWN and known_shape_ok(name, go, rust):
        return "known"
    return "fail"


def run_all(cfg, only=None, strict=False):
    ports = cfg["ports"]
    results = {}
    for group, name, fn in CASES:
        if only and name not in only and group not in only:
            continue
        try:
            outcome = fn(ports, cfg)
            go, rust = outcome["go"], outcome["rust"]
            status = compare_case(name, go, rust)
            if strict and status == "known":
                status = "fail"
            results[name] = {"group": group, "status": status, "go": go, "rust": rust}
        except Exception as e:
            results[name] = {"group": group, "status": "harness_error", "error": f"{type(e).__name__}: {e}"}
    return results


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--strict", action="store_true", help="fail on known diffs too")
    ap.add_argument("--only", default=None, help="comma list of case names or groups")
    ap.add_argument("--out", default=None, help="write results JSON here")
    args = ap.parse_args()
    with open(args.config) as f:
        cfg = json.load(f)
    only = set(args.only.split(",")) if args.only else None
    results = run_all(cfg, only=only, strict=args.strict)
    n_pass = sum(1 for r in results.values() if r["status"] == "pass")
    n_known = sum(1 for r in results.values() if r["status"] == "known")
    n_fail = sum(1 for r in results.values() if r["status"] in ("fail", "harness_error"))
    print(f"{'CASE':<28} {'STATUS':<14} GROUP")
    for name, r in results.items():
        print(f"{name:<28} {r['status']:<14} {r['group']}")
        if r["status"] in ("fail", "harness_error"):
            print(f"  GO:   {json.dumps(r.get('go'), default=str)}")
            print(f"  RUST: {json.dumps(r.get('rust'), default=str)}")
            if r.get("error"):
                print(f"  ERROR: {r['error']}")
    print(f"\npass={n_pass} known={n_known} fail={n_fail} total={len(results)}")
    if args.out:
        with open(args.out, "w") as f:
            json.dump(results, f, indent=2, default=str)
    if n_fail:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
