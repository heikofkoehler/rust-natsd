//! JWT operator mode: NATS JWT verification and trust establishment.
//!
//! A NATS JWT is `base64url(header).base64url(payload).base64url(sig)`
//! with header `{"typ":"JWT","alg":"ed25519-nkey"}` and `sig` an
//! Ed25519 signature over the `header.payload` bytes, made with the
//! issuer's NKey. Verification uses the already-depended-on `nkeys`
//! crate (public-only `KeyPair::from_public_key` + `verify`) — no new
//! dependency, and no `jsonwebtoken`-style generic JWT crate, which
//! does not speak `ed25519-nkey` anyway.
//!
//! Claim chain (all links verified, no fallback to "accept and warn"):
//! - operator JWT: self-signed (`iss == sub`, an `O…` key),
//!   `nats.type == "operator"`.
//! - account JWT: `iss` is the operator public key or one of the
//!   operator's signing keys, `sub` is an `A…` key,
//!   `nats.type == "account"`.
//! - user JWT: `iss` is a trusted account's public key or one of its
//!   signing keys, `sub` is a `U…` key, `nats.type == "user"`. On top
//!   of the JWT chain, the client must also prove possession of the
//!   user private key by signing the per-connection INFO nonce (the
//!   existing standalone-nkey check): a `jwt` field alone, or with a
//!   missing/invalid nonce signature, never authenticates. The key
//!   for that signature is the JWT `sub` claim: when CONNECT carries
//!   an `nkey` field it must equal `sub` (Phase 28B), and when it
//!   does not — the shape nats.go's UserJWT always sends, since
//!   UserJWT and Nkey are mutually exclusive there — the key is
//!   derived directly from `sub`, as Go's server does.
//!
//! Time validation: `exp` in the past rejects (no leeway — stricter
//! than any clock-skew allowance Go might grant, which can only make
//! this server reject tokens Go would accept, never the reverse);
//! `nbf` in the future rejects; `iat` more than 60 s in the future
//! rejects as insane (a small allowance for clock skew, not a
//! security boundary — `exp` is). A missing `exp` is accepted, as in
//! Go, where operator/account/user JWTs without expiry are common.
//!
//! Account JWTs are converted into the existing [`Account`] machinery
//! at trust-establishment time (server start / config load), so JWT
//! imports/exports reuse the area-6 import system with no second
//! implementation: import source pubkeys and export account pubkeys
//! are translated to account *names* where the account is trusted;
//! imports from, and exports to, untrusted accounts are dropped
//! (fail-closed). Account JWT `limits` are not enforced (connections
//! use the server-wide `max_payload`); account `default_permissions`
//! become the account-level permission fallback for user JWTs that
//! omit a pub/sub axis. Note for tokens also meant for Go: Go's
//! limits keys are `conn`/`subs`/`payload`/`data`/`leaf`/
//! `imports`/`exports` (jwt v2), and an absent numeric limit decodes
//! to 0, which Go enforces as a real limit (only -1 is unlimited) —
//! a limits-less account JWT is rejected by Go on the first
//! connection. Rust ignores limits entirely, so the same token
//! authenticates here either way.

use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::Value;

use crate::config::{Account, Export, Import, ImportKind, Permissions};

/// Seconds of future skew tolerated for `iat` (sanity only).
const IAT_FUTURE_TOLERANCE_SECS: i64 = 60;

/// Current Unix time in seconds.
pub fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn b64url_decode(s: &str) -> Result<Vec<u8>, String> {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(s)
        .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(s))
        .map_err(|_| format!("invalid base64url: {s}"))
}

fn b64url_encode(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// Encode a JWT for `claims` signed by `signer`. Header is fixed to
/// the NATS header. Used by tests and tooling; the server only
/// verifies.
pub fn encode_jwt(claims: &Value, signer: &nkeys::KeyPair) -> Result<String, String> {
    let header = serde_json::json!({"typ": "JWT", "alg": "ed25519-nkey"});
    let header_b64 = b64url_encode(header.to_string().as_bytes());
    let payload_b64 = b64url_encode(claims.to_string().as_bytes());
    let signing_input = format!("{header_b64}.{payload_b64}");
    let sig = signer
        .sign(signing_input.as_bytes())
        .map_err(|e| format!("signing failed: {e}"))?;
    Ok(format!("{signing_input}.{}", b64url_encode(&sig)))
}

/// A JWT split and decoded, signature not yet verified.
struct Decoded {
    payload: Value,
    signing_input: String,
    signature: Vec<u8>,
}

fn decode_unverified(token: &str) -> Result<Decoded, String> {
    let token = token.trim();
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 || parts.iter().any(|p| p.is_empty()) {
        return Err("malformed JWT: expected header.payload.signature".to_string());
    }
    let header_bytes = b64url_decode(parts[0])?;
    let header: Value = serde_json::from_slice(&header_bytes)
        .map_err(|_| "malformed JWT header JSON".to_string())?;
    if header.get("alg").and_then(|v| v.as_str()) != Some("ed25519-nkey") {
        return Err("JWT header alg is not ed25519-nkey".to_string());
    }
    if header.get("typ").and_then(|v| v.as_str()) != Some("JWT") {
        return Err("JWT header typ is not JWT".to_string());
    }
    let payload_bytes = b64url_decode(parts[1])?;
    let payload: Value = serde_json::from_slice(&payload_bytes)
        .map_err(|_| "malformed JWT payload JSON".to_string())?;
    if !payload.is_object() {
        return Err("JWT payload is not a JSON object".to_string());
    }
    let signature = b64url_decode(parts[2])?;
    Ok(Decoded {
        payload,
        signing_input: format!("{}.{}", parts[0], parts[1]),
        signature,
    })
}

/// Verified claims of one JWT: signature, issuer, and time fields all
/// checked. `nats` is the raw `nats` object for type-specific parsing.
#[derive(Debug, Clone)]
pub struct VerifiedClaims {
    pub iss: String,
    pub sub: String,
    pub name: Option<String>,
    pub claim_type: String,
    pub nats: Value,
}

fn int_field(payload: &Value, key: &str) -> Result<Option<i64>, String> {
    match payload.get(key) {
        None => Ok(None),
        Some(Value::Number(n)) => n
            .as_i64()
            .ok_or_else(|| format!("JWT field {key} is not an integer"))
            .map(Some),
        Some(_) => Err(format!("JWT field {key} is not an integer")),
    }
}

/// Verify a JWT whose issuer must be exactly `issuer`: header, Ed25519
/// signature against the issuer's NKey public key, `iss`, and time
/// fields (`exp`/`nbf`/`iat`). Returns the verified claims.
pub fn verify_jwt(token: &str, issuer: &str, now: i64) -> Result<VerifiedClaims, String> {
    let decoded = decode_unverified(token)?;
    let payload = &decoded.payload;
    let iss = payload
        .get("iss")
        .and_then(|v| v.as_str())
        .ok_or("JWT missing iss")?
        .to_string();
    if iss != issuer {
        return Err(format!("JWT issuer {iss} is not the expected issuer"));
    }
    let sub = payload
        .get("sub")
        .and_then(|v| v.as_str())
        .ok_or("JWT missing sub")?
        .to_string();
    if sub.is_empty() {
        return Err("JWT sub is empty".to_string());
    }
    let kp = nkeys::KeyPair::from_public_key(issuer)
        .map_err(|_| format!("JWT issuer {issuer} is not a valid NKey public key"))?;
    kp.verify(decoded.signing_input.as_bytes(), &decoded.signature)
        .map_err(|_| "JWT signature verification failed".to_string())?;
    if let Some(exp) = int_field(payload, "exp")? {
        if now >= exp {
            return Err("JWT is expired".to_string());
        }
    }
    if let Some(nbf) = int_field(payload, "nbf")? {
        if now < nbf {
            return Err("JWT is not yet valid".to_string());
        }
    }
    if let Some(iat) = int_field(payload, "iat")? {
        if iat > now + IAT_FUTURE_TOLERANCE_SECS {
            return Err("JWT iat is too far in the future".to_string());
        }
    }
    let nats = payload
        .get("nats")
        .cloned()
        .filter(|v| v.is_object())
        .ok_or("JWT missing nats object")?;
    let claim_type = nats
        .get("type")
        .and_then(|v| v.as_str())
        .ok_or("JWT nats object missing type")?
        .to_string();
    // Go reads the claim name from the top level; accept nats.name
    // as a fallback for hand-written tokens.
    let name = payload
        .get("name")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .or_else(|| {
            nats.get("name")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        });
    Ok(VerifiedClaims {
        iss,
        sub,
        name,
        claim_type,
        nats,
    })
}

fn str_list(v: Option<&Value>) -> Vec<String> {
    match v {
        Some(Value::String(s)) => vec![s.clone()],
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(|i| i.as_str().map(|s| s.to_string()))
            .collect(),
        _ => Vec::new(),
    }
}

fn valid_nkey(key: &str) -> bool {
    nkeys::KeyPair::from_public_key(key).is_ok()
}

/// Subject permissions from a Go-shaped `{"pub": {"allow": [...],
/// "deny": [...]}, "sub": {...}}` object (a bare string counts as a
/// one-element allow list, matching the .conf parser's leniency).
pub fn permissions_from_nats(nats: &Value) -> Permissions {
    let axis = |key: &str| -> (Option<Vec<String>>, Option<Vec<String>>) {
        let Some(obj) = nats.get(key) else {
            return (None, None);
        };
        if obj.is_string() || obj.is_array() {
            return (Some(str_list(Some(obj))), None);
        }
        let allow = obj.get("allow").map(|v| str_list(Some(v)));
        let deny = obj.get("deny").map(|v| str_list(Some(v)));
        (allow, deny)
    };
    let (publish_allow, publish_deny) = axis("pub");
    let (subscribe_allow, subscribe_deny) = axis("sub");
    Permissions {
        publish_allow,
        publish_deny,
        subscribe_allow,
        subscribe_deny,
    }
}

/// One trusted account: its JWT-derived identity plus the [`Account`]
/// the server routes and enforces imports/exports against.
#[derive(Debug, Clone, PartialEq)]
pub struct JwtAccount {
    /// Account public key (`A…`, the account JWT's `sub`).
    pub pubkey: String,
    /// Account name (JWT top-level `name`, else `nats.name`, else pubkey).
    pub name: String,
    /// Account signing keys (`A…` keys allowed to sign user JWTs for
    /// this account, in addition to the account public key itself).
    pub signing_keys: Vec<String>,
    /// Converted account: name, default permissions, imports/exports.
    /// `users` is always empty — users arrive via user JWTs.
    pub account: Account,
}

/// Operator trust: one operator public key and the account JWTs it
/// vouches for. Built once (config load) via [`OperatorTrust::from_jwts`],
/// which verifies the whole chain; per-connection work only verifies
/// the user JWT against this trust.
#[derive(Debug, Clone, PartialEq)]
pub struct OperatorTrust {
    pub pubkey: String,
    pub accounts: Vec<JwtAccount>,
}

impl OperatorTrust {
    /// Build trust from an operator JWT and account JWTs, verifying
    /// every signature and claim link. Any failure is an error — a
    /// partially trusted operator is never constructed.
    pub fn from_jwts(operator_jwt: &str, account_jwts: &[String]) -> Result<Self, String> {
        let now = now_unix();
        let op_unverified = decode_unverified(operator_jwt)?;
        let op_iss = op_unverified
            .payload
            .get("iss")
            .and_then(|v| v.as_str())
            .ok_or("operator JWT missing iss")?;
        let op_sub = op_unverified
            .payload
            .get("sub")
            .and_then(|v| v.as_str())
            .ok_or("operator JWT missing sub")?;
        if op_iss != op_sub {
            return Err("operator JWT iss != sub (operator JWTs are self-signed)".to_string());
        }
        if !op_sub.starts_with('O') {
            return Err("operator JWT sub is not an operator (O…) key".to_string());
        }
        let op = verify_jwt(operator_jwt, op_iss, now)?;
        if op.claim_type != "operator" {
            return Err(format!(
                "operator JWT nats.type is {:?}, not \"operator\"",
                op.claim_type
            ));
        }
        let mut trusted_issuers = vec![op.sub.clone()];
        for key in str_list(op.nats.get("signing_keys")) {
            if !key.starts_with('O') || !valid_nkey(&key) {
                return Err(format!(
                    "operator signing key {key} is not a valid operator key"
                ));
            }
            trusted_issuers.push(key);
        }
        // First pass: identities and names, so the second pass can
        // translate pubkey references to names.
        struct RawAccount {
            pubkey: String,
            name: String,
            signing_keys: Vec<String>,
            claims: VerifiedClaims,
        }
        let mut raw_accounts = Vec::new();
        for jwt in account_jwts {
            let unverified = decode_unverified(jwt)?;
            let iss = unverified
                .payload
                .get("iss")
                .and_then(|v| v.as_str())
                .ok_or("account JWT missing iss")?
                .to_string();
            if !trusted_issuers.iter().any(|t| t == &iss) {
                return Err(format!(
                    "account JWT issuer {iss} is not the trusted operator"
                ));
            }
            let sub = unverified
                .payload
                .get("sub")
                .and_then(|v| v.as_str())
                .ok_or("account JWT missing sub")?
                .to_string();
            if !sub.starts_with('A') {
                return Err("account JWT sub is not an account (A…) key".to_string());
            }
            let claims = verify_jwt(jwt, &iss, now)?;
            if claims.claim_type != "account" {
                return Err(format!(
                    "account JWT nats.type is {:?}, not \"account\"",
                    claims.claim_type
                ));
            }
            let name = claims.name.clone().unwrap_or_else(|| sub.clone());
            let mut signing_keys = Vec::new();
            for key in str_list(claims.nats.get("signing_keys")) {
                if !key.starts_with('A') || !valid_nkey(&key) {
                    return Err(format!(
                        "account signing key {key} is not a valid account key"
                    ));
                }
                signing_keys.push(key);
            }
            if raw_accounts.iter().any(|a: &RawAccount| a.pubkey == sub) {
                return Err(format!("duplicate account JWT for {sub}"));
            }
            if raw_accounts.iter().any(|a: &RawAccount| a.name == name) {
                return Err(format!("duplicate account JWT name {name}"));
            }
            raw_accounts.push(RawAccount {
                pubkey: sub,
                name,
                signing_keys,
                claims,
            });
        }
        let name_map: std::collections::HashMap<String, String> = raw_accounts
            .iter()
            .map(|a| (a.pubkey.clone(), a.name.clone()))
            .collect();
        let name_of = |pubkey: &str| -> Option<String> { name_map.get(pubkey).cloned() };
        // Second pass: convert to Accounts, translating pubkey
        // references to names and dropping references to accounts
        // outside this trust (fail-closed).
        let mut accounts = Vec::new();
        for raw in raw_accounts {
            let nats = &raw.claims.nats;
            let mut account = Account {
                name: raw.name.clone(),
                ..Default::default()
            };
            if let Some(dp) = nats.get("default_permissions") {
                account.permissions = permissions_from_nats(dp);
            }
            if let Some(Value::Array(items)) = nats.get("exports") {
                for item in items {
                    let subject = item
                        .get("subject")
                        .and_then(|v| v.as_str())
                        .or_else(|| item.get("service").and_then(|v| v.as_str()))
                        .or_else(|| item.get("stream").and_then(|v| v.as_str()))
                        .map(|s| s.to_string());
                    let kind = item
                        .get("type")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                        .or_else(|| {
                            if item.get("service").is_some() {
                                Some("service".to_string())
                            } else if item.get("stream").is_some() {
                                Some("stream".to_string())
                            } else {
                                None
                            }
                        });
                    let (Some(subject), Some(kind)) = (subject, kind) else {
                        continue;
                    };
                    let kind = match kind.as_str() {
                        "service" => ImportKind::Service,
                        "stream" => ImportKind::Stream,
                        _ => continue,
                    };
                    // Go-shaped exports carry no `accounts` field at
                    // all (jwt v2's Export has none): such an export
                    // authorizes any importer, matching Go's
                    // subject-match-only check. A present `accounts`
                    // list (the Rust/.conf extension) restricts to
                    // the listed accounts, with unmapped pubkeys
                    // dropped (fail-closed).
                    let mapped: Vec<String> = match item.get("accounts") {
                        None => vec!["*".to_string()],
                        Some(_) => str_list(item.get("accounts"))
                            .into_iter()
                            .filter_map(|pk| name_of(&pk))
                            .collect(),
                    };
                    account.exports.push(Export {
                        kind: Some(kind),
                        subject,
                        accounts: mapped,
                    });
                }
            }
            if let Some(Value::Array(items)) = nats.get("imports") {
                for item in items {
                    let remote = item
                        .get("subject")
                        .and_then(|v| v.as_str())
                        .or_else(|| item.get("service").and_then(|v| v.as_str()))
                        .or_else(|| item.get("stream").and_then(|v| v.as_str()))
                        .map(|s| s.to_string());
                    let kind = item
                        .get("type")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                        .or_else(|| {
                            if item.get("service").is_some() {
                                Some("service".to_string())
                            } else if item.get("stream").is_some() {
                                Some("stream".to_string())
                            } else {
                                None
                            }
                        });
                    let source = item
                        .get("account")
                        .and_then(|v| v.as_str())
                        .and_then(name_of);
                    let local = item
                        .get("to")
                        .and_then(|v| v.as_str())
                        .or_else(|| item.get("local_subject").and_then(|v| v.as_str()))
                        .map(|s| s.to_string());
                    let (Some(remote), Some(kind), Some(source), Some(local)) =
                        (remote, kind, source, local)
                    else {
                        continue;
                    };
                    let kind = match kind.as_str() {
                        "service" => ImportKind::Service,
                        "stream" => ImportKind::Stream,
                        _ => continue,
                    };
                    account.imports.push(Import {
                        kind: Some(kind),
                        source_account: source,
                        remote_subject: remote,
                        local_subject: local,
                    });
                }
            }
            accounts.push(JwtAccount {
                pubkey: raw.pubkey,
                name: raw.name,
                signing_keys: raw.signing_keys,
                account,
            });
        }
        Ok(OperatorTrust {
            pubkey: op.sub,
            accounts,
        })
    }

    /// Result of a successful user-JWT authentication.
    pub fn verify_user(
        &self,
        token: &str,
        expected_nkey: Option<&str>,
        nonce: Option<&[u8]>,
        sig_b64: Option<&str>,
        now: i64,
    ) -> Result<UserAuth, String> {
        let unverified = decode_unverified(token)?;
        let iss = unverified
            .payload
            .get("iss")
            .and_then(|v| v.as_str())
            .ok_or("user JWT missing iss")?
            .to_string();
        let sub = unverified
            .payload
            .get("sub")
            .and_then(|v| v.as_str())
            .ok_or("user JWT missing sub")?
            .to_string();
        if !iss.starts_with('A') {
            return Err("user JWT issuer is not an account (A…) key".to_string());
        }
        let jwt_account = self
            .accounts
            .iter()
            .find(|a| a.pubkey == iss || a.signing_keys.iter().any(|k| k == &iss))
            .ok_or_else(|| format!("user JWT issuer {iss} is not a trusted account"))?;
        let claims = verify_jwt(token, &iss, now)?;
        if claims.claim_type != "user" {
            return Err(format!(
                "user JWT nats.type is {:?}, not \"user\"",
                claims.claim_type
            ));
        }
        if claims.sub != sub {
            return Err("user JWT sub changed between parse and verify".to_string());
        }
        match expected_nkey {
            Some(nkey) if nkey == sub => {}
            Some(_) => {
                return Err("CONNECT nkey does not match the user JWT sub".to_string());
            }
            // No nkey field: derive the user key from the JWT sub
            // (nats.go UserJWT shape, Go server behaviour). The
            // nonce signature below is still verified against that
            // key, so a JWT alone never authenticates.
            None => {}
        }
        // Possession proof: the INFO nonce signed with the user key
        // (the JWT sub, whether or not CONNECT carried an nkey).
        // Missing nonce, missing sig, or a bad sig all reject — a JWT
        // alone never authenticates.
        let (Some(nonce), Some(sig)) = (nonce, sig_b64) else {
            return Err("user JWT authentication requires a nonce signature".to_string());
        };
        if !crate::config::verify_nkey_sig(&sub, nonce, sig) {
            return Err("user JWT nonce signature verification failed".to_string());
        }
        let user_name = claims.name.clone().unwrap_or_else(|| sub.clone());
        Ok(UserAuth {
            account: jwt_account.account.name.clone(),
            user: Some(user_name),
            permissions: permissions_from_nats(&claims.nats)
                .fallback(&jwt_account.account.permissions),
        })
    }
}

/// Successful user-JWT authentication (mapped to `AuthOutcome` by
/// `Config::authenticate`).
#[derive(Debug, Clone)]
pub struct UserAuth {
    pub account: String,
    pub user: Option<String>,
    pub permissions: Permissions,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn claims(iss: &str, sub: &str, typ: &str) -> Value {
        serde_json::json!({
            "iss": iss, "sub": sub, "iat": now_unix(),
            "nats": {"type": typ, "version": 2}
        })
    }

    #[test]
    fn roundtrip_and_tamper() {
        let kp = nkeys::KeyPair::new_user();
        let pubkey = kp.public_key();
        let token = encode_jwt(&claims(&pubkey, &pubkey, "user"), &kp).unwrap();
        // Self-signed user claim verifies as a token (type is checked
        // by callers, not by verify_jwt).
        assert!(verify_jwt(&token, &pubkey, now_unix()).is_ok());
        // Tamper with the payload: keep the signature, change a byte.
        let mut parts: Vec<&str> = token.split('.').collect();
        let mut payload = b64url_decode(parts[1]).unwrap();
        payload[10] ^= 1;
        parts[1] = Box::leak(b64url_encode(&payload).into_boxed_str());
        let tampered = parts.join(".");
        assert!(verify_jwt(&tampered, &pubkey, now_unix()).is_err());
    }

    #[test]
    fn rejects_bad_header_and_expired() {
        let kp = nkeys::KeyPair::new_user();
        let pubkey = kp.public_key();
        let mut c = claims(&pubkey, &pubkey, "user");
        c["exp"] = serde_json::json!(now_unix() - 10);
        let token = encode_jwt(&c, &kp).unwrap();
        assert!(verify_jwt(&token, &pubkey, now_unix()).is_err());
        // Wrong alg: re-encode with a hand-built header.
        let header = b64url_encode(b"{\"typ\":\"JWT\",\"alg\":\"none\"}");
        let payload = b64url_encode(c.to_string().as_bytes());
        let sig = kp.sign(format!("{header}.{payload}").as_bytes()).unwrap();
        let bad = format!("{header}.{payload}.{}", b64url_encode(&sig));
        assert!(verify_jwt(&bad, &pubkey, now_unix()).is_err());
    }

    #[test]
    fn full_chain_builds_trust() {
        let op = nkeys::KeyPair::new_operator();
        let acct = nkeys::KeyPair::new_account();
        let op_pub = op.public_key();
        let acct_pub = acct.public_key();
        let op_jwt = encode_jwt(
            &serde_json::json!({
                "iss": op_pub, "sub": op_pub, "iat": now_unix(),
                "nats": {"type": "operator", "version": 2, "name": "OP"}
            }),
            &op,
        )
        .unwrap();
        let acct_jwt = encode_jwt(
            &serde_json::json!({
                "iss": op_pub, "sub": acct_pub, "iat": now_unix(),
                "nats": {"type": "account", "version": 2, "name": "ACCT",
                         "default_permissions": {"pub": {"allow": ["dflt.>"]}}}
            }),
            &op,
        )
        .unwrap();
        let trust = OperatorTrust::from_jwts(&op_jwt, &[acct_jwt]).unwrap();
        assert_eq!(trust.pubkey, op_pub);
        assert_eq!(trust.accounts.len(), 1);
        assert_eq!(trust.accounts[0].name, "ACCT");
        // Account JWT signed by someone else is not trusted.
        let other = nkeys::KeyPair::new_operator();
        let bad_acct = encode_jwt(
            &serde_json::json!({
                "iss": other.public_key(), "sub": acct_pub, "iat": now_unix(),
                "nats": {"type": "account", "version": 2, "name": "ACCT"}
            }),
            &other,
        )
        .unwrap();
        assert!(OperatorTrust::from_jwts(&op_jwt, &[bad_acct]).is_err());
    }
}
