//! TLS for client connections (Phase 12).
//!
//! When a TLS configuration is present, the client listener does the TLS
//! handshake *before* any NATS protocol: a plain-text client therefore
//! never sees INFO — the handshake fails and the connection is closed.
//! This matches `nats-server` with `tls {}`: TLS is required on that
//! port, not opportunistic. Route/leaf TLS is out of scope (see README).

use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};

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

/// Build a rustls server config from the settings. `verify` without a CA
/// file is a startup error: there would be nothing to verify against.
/// A CA file without `verify` still installs the roots, but client
/// certificates are optional (rustls `allow_unauthenticated`).
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
    let builder = match (&settings.ca_file, settings.verify) {
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
