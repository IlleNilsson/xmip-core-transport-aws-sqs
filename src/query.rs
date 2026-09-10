//! The AWS Query API: an action and its parameters, form-encoded in one
//! `POST`, answered in flat XML. SQS and SNS both speak it, so both sides
//! of it are here — a Location forms a request and judges the answer, a far
//! end reads the parameters back and forms an error.
//!
//! The message body the two carry is text: SQS and SNS take a string, and
//! only the characters XML 1.0 permits — tab, line feed, carriage return
//! and the printable planes. Bytes that are not that are not carried, and
//! [`refusal`] says so before a request is formed rather than after the
//! service answers `InvalidMessageContents`.

use transport::error::{Result, TransportError};
use transport::xml::{escape, first};

use http::message::{Request, Response};
use http::percent::{decode, encode};

/// The form-encoded content type every Query request carries.
pub const CONTENT_TYPE: &str = "application/x-www-form-urlencoded; charset=utf-8";

/// One request: `parameters` as `name=value&…` in the body of a `POST` to
/// `path`, percent-encoded as AWS wants it — `%20`, never `+`.
#[must_use]
pub fn request(path: &str, parameters: &[(&str, &str)]) -> Request {
    let pairs: Vec<String> = parameters
        .iter()
        .map(|(name, value)| format!("{}={}", encode(name, false), encode(value, false)))
        .collect();
    Request::new("POST", path)
        .header("Content-Type", CONTENT_TYPE)
        .body(pairs.join("&").as_bytes())
}

/// The far end's side: the parameters a request's body carries, decoded.
#[must_use]
pub fn parameters(request: &Request) -> Vec<(String, String)> {
    String::from_utf8_lossy(&request.body)
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
            (decode(name), decode(value))
        })
        .collect()
}

/// One parameter's value.
#[must_use]
pub fn parameter<'a>(parameters: &'a [(String, String)], name: &str) -> Option<&'a str> {
    parameters
        .iter()
        .find(|(candidate, _)| candidate == name)
        .map(|(_, value)| value.as_str())
}

/// A 2xx answer as it is; anything else as a failure naming the status and
/// the code the service put in the body, retryable where it says come back.
///
/// # Errors
/// Where the status is not 2xx.
pub fn judge(service: &str, response: Response) -> Result<Response> {
    if (200..300).contains(&response.status) {
        return Ok(response);
    }
    let code = first(&response.text(), "Code").unwrap_or_default();
    let retryable = response.status >= 500
        || response.status == 408
        || response.status == 429
        || matches!(
            code.as_str(),
            "Throttling" | "RequestThrottled" | "ServiceUnavailable" | "InternalFailure"
        );
    Err(TransportError {
        message: format!("{service} answered {} {code}", response.status),
        retryable,
    })
}

/// The far end's answer that is not a result: an `ErrorResponse` naming
/// `code`, with `status`.
#[must_use]
pub fn error(status: u16, code: &str, message: &str) -> Response {
    let body = format!(
        "<?xml version=\"1.0\"?><ErrorResponse><Error><Type>Sender</Type>\
         <Code>{}</Code><Message>{}</Message></Error>\
         <RequestId>xmip</RequestId></ErrorResponse>",
        escape(code),
        escape(message)
    );
    Response::new(status)
        .header("Content-Type", "text/xml")
        .body(body.as_bytes())
}

/// Why `bytes` cannot travel as a message body, or `None` where they can:
/// a body is at least one character of UTF-8, every one of them permitted
/// by XML 1.0.
#[must_use]
pub fn refusal(bytes: &[u8]) -> Option<String> {
    if bytes.is_empty() {
        return Some("a message body is at least one character".to_string());
    }
    let Ok(text) = std::str::from_utf8(bytes) else {
        return Some("a message body is UTF-8 text, and these bytes are not".to_string());
    };
    text.chars().find(|c| !permitted(*c)).map(|c| {
        format!(
            "a message body is XML text, and U+{:04X} is not",
            u32::from(c)
        )
    })
}

/// The body as the text it is, or why it is not.
///
/// # Errors
/// As [`refusal`].
pub fn text(bytes: &[u8]) -> Result<&str> {
    match refusal(bytes) {
        Some(why) => Err(TransportError::permanent(why)),
        None => Ok(std::str::from_utf8(bytes).unwrap_or_default()),
    }
}

/// XML 1.0's `Char` production, which is what SQS and SNS permit.
fn permitted(c: char) -> bool {
    matches!(
        c,
        '\t' | '\n' | '\r' | '\u{20}'..='\u{D7FF}' | '\u{E000}'..='\u{FFFD}' | '\u{10000}'..
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_request_is_formed_as_the_query_api_wants_it_and_reads_back() {
        let request = request(
            "/123456789012/orders",
            &[("Action", "SendMessage"), ("MessageBody", "UNA:+.? 'a&b")],
        );
        assert_eq!(request.method, "POST");
        assert_eq!(request.header_value("content-type"), Some(CONTENT_TYPE));
        assert_eq!(
            request.body,
            b"Action=SendMessage&MessageBody=UNA%3A%2B.%3F%20%27a%26b"
        );
        let read = parameters(&request);
        assert_eq!(parameter(&read, "Action"), Some("SendMessage"));
        assert_eq!(parameter(&read, "MessageBody"), Some("UNA:+.? 'a&b"));
        assert_eq!(parameter(&read, "Absent"), None);
    }

    #[test]
    fn a_server_failure_is_worth_repeating_and_a_client_one_is_not() {
        assert!(
            judge("SQS", Response::new(503))
                .expect_err("server")
                .retryable
        );
        assert!(
            judge("SQS", Response::new(429))
                .expect_err("throttled")
                .retryable
        );
        let throttled = error(400, "Throttling", "Rate exceeded");
        assert!(judge("SQS", throttled).expect_err("throttling").retryable);
        let denied = judge("SNS", error(403, "SignatureDoesNotMatch", "")).expect_err("denied");
        assert!(!denied.retryable);
        assert_eq!(denied.message, "SNS answered 403 SignatureDoesNotMatch");
        assert_eq!(judge("SQS", Response::new(200)).expect("ok").status, 200);
    }

    #[test]
    fn text_is_carried_and_what_is_not_text_is_refused_with_a_reason() {
        assert_eq!(refusal(b"UNA:+.? '\r\n"), None);
        assert_eq!(text("r\u{e4}k".as_bytes()).expect("text"), "r\u{e4}k");
        assert!(refusal(b"").expect("empty").contains("at least one"));
        assert!(refusal(b"\xff\xfe").expect("not utf-8").contains("UTF-8"));
        assert_eq!(
            refusal(b"a\x00b").expect("a NUL"),
            "a message body is XML text, and U+0000 is not"
        );
        assert!(refusal(b"\x1b[0m").is_some(), "an escape is not XML text");
        assert!(!text(b"\x07").expect_err("a bell").retryable);
    }
}
