//! Xmip's side: the three calls a Location makes, each one signed Query
//! request over one connection to the queue's own URL.
//!
//! A queue URL is the whole address — `https://sqs.eu-north-1.amazonaws.com/
//! 123456789012/orders` in the cloud, `http://127.0.0.1:9324/queue/orders`
//! for a stand-in — so a request is a `POST` to its path at its authority,
//! and the `QueueUrl` parameter says it again as the API asks.

use std::time::Duration;

use transport::error::Result;

use crate::query::{self, parameter, text};
use crate::sigv4::{self, Signer};
use http::endpoint;
use http::message::{self, Request, Response};
use http::target::HttpTarget;
use transport::xml::texts;

/// The Query API version every request names.
pub const VERSION: &str = "2012-11-05";

/// The most one `ReceiveMessage` hands back.
pub const MAX_MESSAGES: u8 = 10;

/// One message as it came off the queue.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Message {
    pub id: String,
    pub receipt_handle: String,
    pub body: String,
}

pub struct Client {
    signer: Signer,
    timeout: Option<Duration>,
}

impl Client {
    /// Speak to queues in `region`, signing as `access_key`.
    #[must_use]
    pub fn new(region: &str, access_key: &str, secret_key: &str) -> Self {
        Self {
            signer: Signer::new("sqs", region, access_key, secret_key),
            timeout: None,
        }
    }

    /// Give up on an endpoint that stops answering after `timeout`.
    #[must_use]
    pub const fn timing_out_after(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Send `bytes` as one message to the queue at `queue_url`, and learn
    /// its id.
    ///
    /// # Errors
    /// Where the bytes are not a message body, or the endpoint refused or
    /// could not be reached.
    pub fn send_message(&self, queue_url: &str, bytes: &[u8]) -> Result<String> {
        let body = text(bytes)?;
        let parameters = [
            ("Action", "SendMessage"),
            ("Version", VERSION),
            ("QueueUrl", queue_url),
            ("MessageBody", body),
        ];
        let answer = self.call(queue_url, &parameters)?;
        Ok(transport::xml::first(&answer.text(), "MessageId").unwrap_or_default())
    }

    /// Up to [`MAX_MESSAGES`] messages from the queue at `queue_url`,
    /// waiting `wait` seconds for one where none is there — long polling.
    ///
    /// # Errors
    /// Where the endpoint refused, could not be reached, or did not answer
    /// with messages.
    pub fn receive_message(&self, queue_url: &str, wait: u8) -> Result<Vec<Message>> {
        let count = MAX_MESSAGES.to_string();
        let wait = wait.to_string();
        let parameters = [
            ("Action", "ReceiveMessage"),
            ("Version", VERSION),
            ("QueueUrl", queue_url),
            ("MaxNumberOfMessages", count.as_str()),
            ("WaitTimeSeconds", wait.as_str()),
        ];
        let xml = self.call(queue_url, &parameters)?.text();
        let ids = texts(&xml, "MessageId");
        let handles = texts(&xml, "ReceiptHandle");
        let bodies = texts(&xml, "Body");
        Ok(ids
            .into_iter()
            .zip(handles)
            .zip(bodies)
            .map(|((id, receipt_handle), body)| Message {
                id,
                receipt_handle,
                body,
            })
            .collect())
    }

    /// Delete the message `receipt_handle` was received with.
    ///
    /// # Errors
    /// Where the endpoint refused or could not be reached.
    pub fn delete_message(&self, queue_url: &str, receipt_handle: &str) -> Result<()> {
        let parameters = [
            ("Action", "DeleteMessage"),
            ("Version", VERSION),
            ("QueueUrl", queue_url),
            ("ReceiptHandle", receipt_handle),
        ];
        self.call(queue_url, &parameters).map(|_| ())
    }

    fn call(&self, queue_url: &str, parameters: &[(&str, &str)]) -> Result<Response> {
        let target = HttpTarget::parse(queue_url)?;
        let scheme = if target.secure { "https" } else { "http" };
        let endpoint = format!("{scheme}://{}", target.authority);
        let host = endpoint::authority(&endpoint)?;
        let request = query::request(target.path, parameters).header("Host", &host);
        let signed = self.signer.sign(request, &sigv4::now());
        let stream = endpoint::connect(&endpoint, self.timeout)?;
        query::judge("SQS", message::exchange(stream, &signed)?)
    }
}

/// The action a request asks for, as the far end reads it.
#[must_use]
pub fn action(request: &Request) -> Option<String> {
    parameter(&query::parameters(request), "Action").map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::{Event, Session};
    use transport::socket;

    #[test]
    fn the_three_calls_reach_a_session_and_come_back_shaped_as_sqs_shapes_them() {
        let (listener, address) = socket::bind_tcp("127.0.0.1:0").expect("bind");
        let far_end = std::thread::spawn(move || {
            let mut session = Session::new("eu-north-1", "AKID", "secret")
                .timing_out_after(Duration::from_secs(2));
            let events: Vec<Event> = (0..5)
                .map(|_| session.serve_one(&listener).expect("served"))
                .collect();
            (session, events)
        });
        let client =
            Client::new("eu-north-1", "AKID", "secret").timing_out_after(Duration::from_secs(2));
        let queue = format!("http://{address}/123456789012/orders");
        let id = client.send_message(&queue, b"UNA:+.? '").expect("sent");
        assert!(!id.is_empty(), "an id came back");
        client
            .send_message(&queue, "r\u{e4}k <&> \"b\"".as_bytes())
            .expect("sent");
        let messages = client.receive_message(&queue, 20).expect("received");
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].id, id);
        assert_eq!(messages[0].body, "UNA:+.? '");
        assert_eq!(messages[1].body, "r\u{e4}k <&> \"b\"");
        client
            .delete_message(&queue, &messages[0].receipt_handle)
            .expect("deleted");
        let missing = client
            .delete_message(&queue, "no such handle")
            .expect_err("gone");
        assert!(
            missing.message.contains("404 ReceiptHandleIsInvalid"),
            "{missing}"
        );
        assert!(!missing.retryable);
        let (session, events) = far_end.join().expect("thread");
        assert_eq!(session.messages().len(), 1, "one left in flight");
        assert!(matches!(
            &events[2],
            Event::Received {
                count: 2,
                wait: 20,
                ..
            }
        ));
        assert_eq!(events[3], Event::Deleted(format!("{queue}#{id}")));
        assert_eq!(
            events[4],
            Event::Refused("ReceiptHandleIsInvalid".to_string())
        );
    }

    #[test]
    fn what_is_not_a_message_body_or_a_queue_url_is_refused_before_a_wire_is_touched() {
        let client = Client::new("r", "a", "s");
        let refused = client
            .send_message("http://127.0.0.1:1/q", b"\x00")
            .expect_err("not text");
        assert!(!refused.retryable);
        assert!(refused.message.contains("U+0000"));
        let refused = client
            .send_message("sqs.local/q", b"x")
            .expect_err("no scheme");
        assert!(!refused.retryable);
        assert!(
            client
                .receive_message("http://127.0.0.1:1/q", 0)
                .expect_err("nobody")
                .retryable
        );
    }
}
