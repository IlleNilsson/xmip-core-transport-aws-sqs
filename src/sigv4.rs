//! Signature Version 4 for a service that is not S3: what an SQS, SNS or
//! Kinesis request carries to prove who sent it.
//!
//! The canonical request and the `x-amz-date` clock are the s3 technology's
//! — `transport_s3::sigv4::{canonical, now}` — and are used, not copied.
//! What is here is the part that technology keeps private: the scope, the
//! signing key derived through four HMACs, and the verification the far
//! end does. The one difference from S3 is the one that makes this file
//! exist: the scope names a service, and `transport_s3::sigv4::Signer`
//! bakes that name in as `s3`.
//!
//! **Lift:** give `transport_s3::sigv4::Signer` its service name —
//! `Signer::new(service, region, access_key, secret_key)`, with S3's own
//! client passing `"s3"` — and this file goes; aws-sns and aws-kinesis then
//! take the signer from the s3 technology as this crate does the canonical
//! request. The S3 signer also adds `x-amz-content-sha256`; this one does
//! not, because only S3 asks for it and AWS's own worked example for the
//! Query API (IAM `ListUsers`) signs without it.

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use transport::error::{Result, protocol_error};

use http::message::Request;
pub use transport_s3::sigv4::{amz_date, canonical, now};

const ALGORITHM: &str = "AWS4-HMAC-SHA256";

/// The service, region and credential a signature is made under.
#[derive(Clone, Debug)]
pub struct Signer {
    service: String,
    region: String,
    access_key: String,
    secret_key: String,
}

impl Signer {
    /// Sign for `service` — `sqs`, `sns`, `kinesis` — in `region` as
    /// `access_key`.
    #[must_use]
    pub fn new(service: &str, region: &str, access_key: &str, secret_key: &str) -> Self {
        Self {
            service: service.to_string(),
            region: region.to_string(),
            access_key: access_key.to_string(),
            secret_key: secret_key.to_string(),
        }
    }

    /// Sign `request` as of `at`, an `x-amz-date` such as [`now`] gives,
    /// adding `x-amz-date` and `Authorization`. Every header already on the
    /// request is signed, so `Host` goes on first.
    #[must_use]
    pub fn sign(&self, request: Request, at: &str) -> Request {
        let request = request.header("x-amz-date", at);
        let signed = signed_headers(&request.headers);
        let payload = hex(&Sha256::digest(&request.body));
        let scope = self.scope(at);
        let signature = self.signature(at, &scope, &canonical(&request, &signed, &payload));
        let authorization = format!(
            "{ALGORITHM} Credential={}/{scope}, SignedHeaders={signed}, Signature={signature}",
            self.access_key
        );
        request.header("Authorization", &authorization)
    }

    /// Whether `request` carries the signature this signer would have made.
    ///
    /// # Errors
    /// Where the request has no usable `Authorization`, names another
    /// credential or scope, or carries a signature that differs.
    pub fn verify(&self, request: &Request) -> Result<()> {
        let authorization = request
            .header_value("authorization")
            .ok_or_else(|| protocol_error("a request with no Authorization"))?;
        let (credential, signed, signature) = parts(authorization)?;
        let at = request
            .header_value("x-amz-date")
            .ok_or_else(|| protocol_error("a request with no x-amz-date"))?;
        let scope = self.scope(at);
        if credential != format!("{}/{scope}", self.access_key) {
            return Err(protocol_error("a credential this signer does not hold"));
        }
        let payload = hex(&Sha256::digest(&request.body));
        let expected = self.signature(at, &scope, &canonical(request, signed, &payload));
        if same(&expected, signature) {
            Ok(())
        } else {
            Err(protocol_error("a signature that does not match"))
        }
    }

    fn scope(&self, at: &str) -> String {
        format!(
            "{}/{}/{}/aws4_request",
            date_of(at),
            self.region,
            self.service
        )
    }

    fn signature(&self, at: &str, scope: &str, canonical: &str) -> String {
        let to_sign = format!(
            "{ALGORITHM}\n{at}\n{scope}\n{}",
            hex(&Sha256::digest(canonical.as_bytes()))
        );
        let key = [date_of(at), &self.region, &self.service, "aws4_request"]
            .iter()
            .fold(
                format!("AWS4{}", self.secret_key).into_bytes(),
                |key, step| hmac(&key, step.as_bytes()),
            );
        hex(&hmac(&key, to_sign.as_bytes()))
    }
}

fn date_of(at: &str) -> &str {
    at.get(..8).unwrap_or(at)
}

fn signed_headers(headers: &[(String, String)]) -> String {
    let mut names: Vec<String> = headers
        .iter()
        .map(|(name, _)| name.to_ascii_lowercase())
        .collect();
    names.sort();
    names.dedup();
    names.join(";")
}

fn parts(authorization: &str) -> Result<(&str, &str, &str)> {
    let rest = authorization
        .strip_prefix(ALGORITHM)
        .ok_or_else(|| protocol_error("an Authorization that is not Signature Version 4"))?;
    let mut credential = None;
    let mut signed = None;
    let mut signature = None;
    for part in rest.split(',') {
        match part.trim().split_once('=') {
            Some(("Credential", value)) => credential = Some(value),
            Some(("SignedHeaders", value)) => signed = Some(value),
            Some(("Signature", value)) => signature = Some(value),
            _ => {}
        }
    }
    match (credential, signed, signature) {
        (Some(credential), Some(signed), Some(signature)) => Ok((credential, signed, signature)),
        _ => Err(protocol_error(
            "an Authorization missing one of its three parts",
        )),
    }
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC takes a key of any length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes.iter().fold(String::new(), |mut out, byte| {
        write!(out, "{byte:02x}").expect("writing to a String cannot fail");
        out
    })
}

/// Equal, without the comparison's timing saying how far the two agreed.
fn same(expected: &str, given: &str) -> bool {
    expected.len() == given.len()
        && expected
            .bytes()
            .zip(given.bytes())
            .fold(0u8, |acc, (a, b)| acc | (a ^ b))
            == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    /// AWS's own worked example for the Query API: IAM `ListUsers`, in
    /// "Create a signed AWS API request".
    #[test]
    fn the_documented_example_signs_as_aws_says_it_does() {
        let signer = Signer::new(
            "iam",
            "us-east-1",
            "AKIDEXAMPLE",
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
        );
        let request = Request::new("GET", "/")
            .query("Action", "ListUsers")
            .query("Version", "2010-05-08")
            .header("Host", "iam.amazonaws.com")
            .header(
                "Content-Type",
                "application/x-www-form-urlencoded; charset=utf-8",
            );
        let sent = signer.sign(request, "20150830T123600Z");
        let authorization = sent.header_value("authorization").expect("signed");
        assert_eq!(
            authorization,
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/iam/aws4_request, \
             SignedHeaders=content-type;host;x-amz-date, \
             Signature=5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7"
        );
        signer.verify(&sent).expect("its own signature");
    }

    #[test]
    fn a_tampered_request_another_secret_or_another_service_does_not_verify() {
        let signer = Signer::new("sqs", "eu-north-1", "AKID", "secret");
        let sent = signer.sign(
            Request::new("POST", "/123456789012/orders")
                .header("Host", "127.0.0.1:9000")
                .body(b"Action=SendMessage&MessageBody=UNA"),
            &now(),
        );
        signer.verify(&sent).expect("verifies");
        let mut tampered = sent.clone();
        tampered.body = b"Action=SendMessage&MessageBody=UNB".to_vec();
        assert!(signer.verify(&tampered).is_err(), "the payload is signed");
        let mut tampered = sent.clone();
        tampered.path = "/123456789012/other".to_string();
        assert!(signer.verify(&tampered).is_err(), "the path is signed");
        let mut tampered = sent.clone();
        tampered.query.push(("y".to_string(), "2".to_string()));
        assert!(signer.verify(&tampered).is_err(), "the query is signed");
        let other = Signer::new("sqs", "eu-north-1", "AKID", "other");
        assert!(other.verify(&sent).is_err());
        let other = Signer::new("sns", "eu-north-1", "AKID", "secret");
        assert!(other.verify(&sent).is_err(), "the scope names the service");
        let other = Signer::new("sqs", "eu-north-1", "OTHER", "secret");
        assert!(other.verify(&sent).is_err());
        assert!(signer.verify(&Request::new("GET", "/")).is_err());
        let bare = Request::new("GET", "/").header("Authorization", "Basic x");
        assert!(signer.verify(&bare).is_err());
    }
}
