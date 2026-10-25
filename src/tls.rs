//! TLS for client connections (Phase 12), route/leaf connections
//! (Phase 22), and client-certificate-to-user mapping.
//!
//! Client listener order (Go's STARTTLS, probed): the server sends
//! INFO in plaintext first, then upgrades the same socket to TLS.
//! Route order is different (Go, probed): implicit TLS — the dialer
//! sends its ClientHello first and no plaintext INFO is ever sent.
//! Leaf order is STARTTLS-like (Go, probed): the hub sends INFO in
//! plaintext (with `tls_required`), then upgrades before CONNECT.

use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{ClientConfig, RootCertStore, ServerConfig};

use crate::config::TlsSettings;

fn load_certs(path: &str) -> Result<Vec<CertificateDer<'static>>, String> {
    let file = File::open(path).map_err(|e| format!("cannot open cert file {path}: {e}"))?;
    rustls_pemfile::certs(&mut BufReader::new(file))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("cannot parse cert file {path}: {e}"))
}

fn load_key(path: &str) -> Result<PrivateKeyDer<'static>, String> {
    let file = File::open(path).map_err(|e| format!("cannot open key file {path}: {e}"))?;
    rustls_pemfile::private_key(&mut BufReader::new(file))
        .map_err(|e| format!("cannot parse key file {path}: {e}"))?
        .ok_or_else(|| format!("no private key found in {path}"))
}

fn load_roots(path: &str) -> Result<RootCertStore, String> {
    let mut roots = RootCertStore::empty();
    for cert in load_certs(path)? {
        roots
            .add(cert)
            .map_err(|e| format!("cannot add CA cert from {path}: {e:?}"))?;
    }
    Ok(roots)
}

/// Install the ring CryptoProvider as the process default (rustls 0.23
/// needs exactly one provider; with the ring feature the crate does not
/// pick one automatically in all code paths, e.g. the WebPKI verifier).
/// Safe to call repeatedly; later calls are ignored.
pub fn install_default_provider() {
    let _ =
        rustls::crypto::CryptoProvider::install_default(rustls::crypto::ring::default_provider());
}

/// Build a rustls server config from the settings. `verify` (or
/// `verify_and_map`, which implies it) without a CA file is a startup
/// error: there would be nothing to verify against. A CA file without
/// `verify` still installs the roots, but client certificates are
/// optional (rustls `allow_unauthenticated`).
pub fn build_server_config(settings: &TlsSettings) -> Result<Arc<ServerConfig>, String> {
    install_default_provider();
    let certs = load_certs(&settings.cert_file)?;
    if certs.is_empty() {
        return Err(format!("no certificates found in {}", settings.cert_file));
    }
    let key = load_key(&settings.key_file)?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
        .map_err(|e| format!("TLS protocol versions: {e:?}"))?;
    let require_client_cert = settings.verify || settings.verify_and_map;
    let builder = match (&settings.ca_file, require_client_cert) {
        (Some(ca), true) => {
            let roots = load_roots(ca)?;
            let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
                .build()
                .map_err(|e| format!("TLS client verifier: {e:?}"))?;
            builder.with_client_cert_verifier(verifier)
        }
        (Some(ca), false) => {
            let roots = load_roots(ca)?;
            let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
                .allow_unauthenticated()
                .build()
                .map_err(|e| format!("TLS client verifier: {e:?}"))?;
            builder.with_client_cert_verifier(verifier)
        }
        (None, true) => {
            return Err("--tlsverify requires --tlscacert (nothing to verify against)".to_string());
        }
        (None, false) => builder.with_no_client_auth(),
    };
    let config = builder
        .with_single_cert(certs, key)
        .map_err(|e| format!("TLS cert/key: {e:?}"))?;
    Ok(Arc::new(config))
}

/// Build a rustls client config for dialing a TLS route/leaf peer:
/// the server certificate is verified against `ca_file` (required —
/// without roots there is nothing to verify against), and this
/// node's certificate is presented when cert/key are configured.
pub fn build_client_config(settings: &TlsSettings) -> Result<Arc<ClientConfig>, String> {
    install_default_provider();
    let Some(ca) = &settings.ca_file else {
        return Err("route/leaf TLS dialing requires a CA file".to_string());
    };
    let roots = load_roots(ca)?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
        .map_err(|e| format!("TLS protocol versions: {e:?}"))?
        .with_root_certificates(roots);
    let config = if !settings.cert_file.is_empty() && !settings.key_file.is_empty() {
        let certs = load_certs(&settings.cert_file)?;
        if certs.is_empty() {
            return Err(format!("no certificates found in {}", settings.cert_file));
        }
        let key = load_key(&settings.key_file)?;
        builder
            .with_client_auth_cert(certs, key)
            .map_err(|e| format!("TLS client cert/key: {e:?}"))?
    } else {
        builder.with_no_client_auth()
    };
    Ok(Arc::new(config))
}

// ---------------------------------------------------------------------------
// Certificate identity parsing (for verify_and_map)
// ---------------------------------------------------------------------------

/// Identity carried by one client certificate, in Go's lookup order:
/// email SANs, then DNS SANs, then URI SANs, then the subject CN, then
/// the full distinguished name (RFC 2253 form, e.g. `CN=alice`).
/// `candidates()` returns them in that order; the first candidate that
/// equals a configured user name authenticates as that user.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct CertIdentity {
    pub email_sans: Vec<String>,
    pub dns_sans: Vec<String>,
    pub uri_sans: Vec<String>,
    pub cn: Option<String>,
    pub dn: String,
}

impl CertIdentity {
    pub fn candidates(&self) -> Vec<String> {
        let mut out = Vec::new();
        out.extend(self.email_sans.iter().cloned());
        out.extend(self.dns_sans.iter().cloned());
        out.extend(self.uri_sans.iter().cloned());
        if let Some(cn) = &self.cn {
            out.push(cn.clone());
        }
        if !self.dn.is_empty() {
            out.push(self.dn.clone());
        }
        out
    }
}

/// Minimal DER reader: single-byte tags, short/long-form lengths
/// (indefinite form rejected — DER forbids it).
struct Der<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Der<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Der { buf, pos: 0 }
    }

    fn next(&mut self) -> Option<(u8, &'a [u8])> {
        if self.pos >= self.buf.len() {
            return None;
        }
        let tag = self.buf[self.pos];
        self.pos += 1;
        if self.pos >= self.buf.len() {
            return None;
        }
        let first = self.buf[self.pos];
        self.pos += 1;
        let len = if first & 0x80 == 0 {
            first as usize
        } else {
            let n = (first & 0x7f) as usize;
            if n == 0 || n > 4 || self.pos + n > self.buf.len() {
                return None;
            }
            let mut len = 0usize;
            for _ in 0..n {
                len = (len << 8) | self.buf[self.pos] as usize;
                self.pos += 1;
            }
            len
        };
        if self.pos + len > self.buf.len() {
            return None;
        }
        let value = &self.buf[self.pos..self.pos + len];
        self.pos += len;
        Some((tag, value))
    }
}

fn parse_oid(value: &[u8]) -> Option<String> {
    if value.is_empty() {
        return None;
    }
    let mut parts: Vec<u64> = vec![(value[0] / 40) as u64, (value[0] % 40) as u64];
    let mut cur: u64 = 0;
    let mut any = false;
    for &b in &value[1..] {
        cur = (cur << 7) | (b & 0x7f) as u64;
        any = true;
        if b & 0x80 == 0 {
            parts.push(cur);
            cur = 0;
            any = false;
        }
    }
    if any {
        return None;
    }
    Some(
        parts
            .iter()
            .map(|p| p.to_string())
            .collect::<Vec<_>>()
            .join("."),
    )
}

fn oid_short_name(oid: &str) -> &str {
    match oid {
        "2.5.4.3" => "CN",
        "2.5.4.5" => "SERIALNUMBER",
        "2.5.4.6" => "C",
        "2.5.4.7" => "L",
        "2.5.4.8" => "ST",
        "2.5.4.10" => "O",
        "2.5.4.11" => "OU",
        "2.5.4.42" => "GIVENNAME",
        "2.5.4.4" => "SN",
        _ => "",
    }
}

/// Decode a directory-string value by tag. BMPString is UTF-16BE;
/// everything else is read as (lossy) UTF-8 text.
fn decode_string(tag: u8, value: &[u8]) -> String {
    if tag == 0x1e {
        let units: Vec<u16> = value
            .as_chunks::<2>()
            .0
            .iter()
            .map(|c| u16::from_be_bytes(*c))
            .collect();
        String::from_utf16_lossy(&units)
    } else {
        String::from_utf8_lossy(value).into_owned()
    }
}

/// Parse a Name (SEQUENCE OF SET OF AttributeTypeAndValue) into
/// (CN, DN string). Returns None on structural failure.
fn parse_name(value: &[u8]) -> Option<(Option<String>, String)> {
    let mut der = Der::new(value);
    let mut cn = None;
    let mut rdns: Vec<String> = Vec::new();
    while let Some((tag, set_value)) = der.next() {
        if tag != 0x31 {
            return None;
        }
        let mut set = Der::new(set_value);
        let mut attrs: Vec<String> = Vec::new();
        while let Some((atag, aval)) = set.next() {
            if atag != 0x30 {
                return None;
            }
            let mut attr = Der::new(aval);
            let (otag, oval) = attr.next()?;
            if otag != 0x06 {
                return None;
            }
            let oid = parse_oid(oval)?;
            let (vtag, vval) = attr.next()?;
            let text = decode_string(vtag, vval);
            let short = oid_short_name(&oid);
            if short.is_empty() {
                attrs.push(format!("{oid}={text}"));
            } else {
                if oid == "2.5.4.3" && cn.is_none() {
                    cn = Some(text.clone());
                }
                attrs.push(format!("{short}={text}"));
            }
        }
        if attrs.is_empty() {
            return None;
        }
        rdns.push(attrs.join("+"));
    }
    Some((cn, rdns.join(",")))
}

/// Parse the first certificate in `der` (a DER-encoded X.509
/// certificate, as rustls hands out peer certificates) into its
/// identity. Returns None on any structural failure — the caller
/// then treats the certificate as unmapped.
pub fn parse_cert_identity(der: &[u8]) -> Option<CertIdentity> {
    let mut top = Der::new(der);
    let (tag, cert_value) = top.next()?;
    if tag != 0x30 {
        return None;
    }
    let mut cert = Der::new(cert_value);
    let (tag, tbs_value) = cert.next()?;
    if tag != 0x30 {
        return None;
    }
    // Collect the [3] extensions block; the subject is found by a
    // second pass (see `parse_subject_from_tbs`).
    let mut tbs = Der::new(tbs_value);
    let mut extensions: Option<Vec<u8>> = None;
    while let Some((tag, value)) = tbs.next() {
        if tag == 0xa3 {
            extensions = Some(value.to_vec());
        }
    }
    let (cn, dn) = parse_subject_from_tbs(tbs_value)?;
    let mut identity = CertIdentity {
        cn,
        dn,
        ..Default::default()
    };
    if let Some(ext_block) = extensions {
        // [3] EXPLICIT wraps a SEQUENCE of extensions.
        let mut wrapper = Der::new(&ext_block);
        let (tag, seq_value) = wrapper.next()?;
        if tag == 0x30 {
            let mut exts = Der::new(seq_value);
            while let Some((tag, value)) = exts.next() {
                if tag != 0x30 {
                    continue;
                }
                let mut ext = Der::new(value);
                let (otag, oval) = match ext.next() {
                    Some(v) => v,
                    None => continue,
                };
                if otag != 0x06 {
                    continue;
                }
                let oid = match parse_oid(oval) {
                    Some(o) => o,
                    None => continue,
                };
                // Optional critical BOOLEAN, then the OCTET STRING.
                let mut octet: Option<&[u8]> = None;
                while let Some((tag, value)) = ext.next() {
                    if tag == 0x04 {
                        octet = Some(value);
                    }
                }
                if oid == "2.5.29.17" {
                    if let Some(octet) = octet {
                        let mut sans = Der::new(octet);
                        if let Some((tag, seq)) = sans.next() {
                            if tag == 0x30 {
                                let mut names = Der::new(seq);
                                while let Some((tag, value)) = names.next() {
                                    let text = String::from_utf8_lossy(value).into_owned();
                                    match tag {
                                        0x81 => identity.email_sans.push(text),
                                        0x82 => identity.dns_sans.push(text),
                                        0x86 => identity.uri_sans.push(text),
                                        _ => {}
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    Some(identity)
}

/// Find the subject Name in a TBS certificate by child position:
/// children are version?([0]), serial, alg, issuer, validity,
/// subject, spki, ... — subject is the SEQUENCE child that comes
/// right after the validity SEQUENCE child.
fn parse_subject_from_tbs(tbs_value: &[u8]) -> Option<(Option<String>, String)> {
    let mut tbs = Der::new(tbs_value);
    let mut prev_was_validity = false;
    while let Some((tag, value)) = tbs.next() {
        if tag == 0x30 {
            // Validity is the SEQUENCE whose children are time tags
            // (0x17 UTCTime / 0x18 GeneralizedTime).
            let mut inner = Der::new(value);
            let is_validity = matches!(inner.next().map(|(t, _)| t), Some(0x17) | Some(0x18));
            if prev_was_validity {
                return parse_name(value);
            }
            if is_validity {
                prev_was_validity = true;
            }
        }
    }
    None
}
