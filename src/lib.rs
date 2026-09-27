#![forbid(unsafe_code)]

//! Streams that arrive as messages on an SQS queue. One message is one
//! Stream, its id kept beside it.
//!
//! SQS is the queue of every organisation that lives in AWS, and its Query
//! API is three calls on a queue URL: send a message, receive up to ten
//! with long polling, delete one by its receipt handle. A Receive Location
//! receives, hands each body on as a Stream and deletes it once it is; a
//! Send Location sends a Stream as one message. Both are Signature Version
//! 4 over plain HTTP/1.1 on a socket — `https://` with the `tls` feature,
//! which is the http technology's TLS (ADR-0033).
//!
//! ```text
//! client.rs    Xmip's side: send, receive, delete
//! session.rs   the far end a test or the playground runs on loopback
//! loopback.rs  both ends of one exchange on this machine (ADR-0051)
//! ```
//!
//! The endpoint and HTTP itself come from the http
//! technology; Signature Version 4 and the Query API — the form, the
//! answer, the error, the text rule — from the AWS crate, the flat XML
//! scan from the capability (ADR-0044). The signer and the Query API lived
//! here until 2026-09-14, the signer importing the s3 technology and
//! aws-sns importing this one, and in the http technology until the
//! owner's ruling of 2026-09-22: what AWS speaks is shared through the AWS
//! crate, never sideways.
//!
//! A payload is bytes (ADR-0038, amendment 2026-09-26), and SQS carries a
//! message as text — one to 256 KiB of the characters XML permits, UTF-8.
//! Only the wire is text: the transport takes bytes and hands bytes up, and
//! turns them into text where the request is formed. What is not that text
//! is refused there with the reason, never replaced and called delivered.
//! [`ceiling`] and [`aws::query::refusal`] say both rules.
//!
//! A queue is not an artefact anyone claims: a received message is in
//! flight until it is deleted, which is the queue's own claim, so
//! [`Transport::claims`] answers `None`. The origin URI is the queue URL
//! with the message id as its fragment. A send target is a queue URL, or
//! empty for this transport's own queue.

pub mod client;
pub mod loopback;
pub mod session;

use std::time::Duration;

pub use client::{Client, Message};
use http::endpoint::Connections;
pub use session::{Event, Session};
use transport::ceiling;
use transport::error::Result;
use transport::{Arrived, Configured, Directions, Transport};
use xcore::settings::{Applies, Kind, Presence, Read, Setting, Settings};

/// The largest message SQS carries: 256 KiB.
#[must_use]
pub const fn ceiling() -> usize {
    256 * 1024
}

pub struct SqsTransport {
    queue_url: String,
    region: String,
    access_key: String,
    secret_key: String,
    wait: u8,
    timeout: Option<Duration>,
    /// The connections kept to the service, shared by every client this
    /// makes.
    connections: Connections,
}

impl SqsTransport {
    /// Speak to the queue at `queue_url` — `https://sqs.<region>.amazonaws.com/
    /// <account>/<queue>` in the cloud, `http://host:port/<account>/<queue>`
    /// for a stand-in — in `region`.
    #[must_use]
    pub fn new(queue_url: impl Into<String>, region: &str) -> Self {
        Self {
            queue_url: queue_url.into(),
            region: region.to_string(),
            access_key: String::new(),
            secret_key: String::new(),
            wait: 0,
            timeout: None,
            connections: Connections::new(),
        }
    }

    /// Sign as this access key.
    #[must_use]
    pub fn with_credentials(mut self, access_key: &str, secret_key: &str) -> Self {
        self.access_key = access_key.to_string();
        self.secret_key = secret_key.to_string();
        self
    }

    /// Long-poll: wait up to `seconds` — twenty at most — for a message
    /// where the queue is empty, rather than answering at once.
    #[must_use]
    pub const fn waiting(mut self, seconds: u8) -> Self {
        self.wait = if seconds > 20 { 20 } else { seconds };
        self
    }

    /// Give up on an endpoint that stops answering after `timeout`.
    #[must_use]
    pub const fn timing_out_after(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// The client this transport speaks through.
    #[must_use]
    pub fn client(&self) -> Client {
        let client = Client::new(&self.region, &self.access_key, &self.secret_key);
        let client = client.sharing(self.connections.clone());
        match self.timeout {
            Some(timeout) => client.timing_out_after(timeout),
            None => client,
        }
    }

    /// A far end that holds this transport's credentials, for a test or the
    /// playground to run on loopback.
    #[must_use]
    pub fn session(&self) -> Session {
        let session = Session::new(&self.region, &self.access_key, &self.secret_key);
        match self.timeout {
            Some(timeout) => session.timing_out_after(timeout),
            None => session,
        }
    }

    /// The queue a target names, or this transport's own where it names
    /// none.
    fn resolve<'a>(&'a self, target: &'a str) -> &'a str {
        if target.is_empty() {
            &self.queue_url
        } else {
            target
        }
    }
}

impl Transport for SqsTransport {
    fn name(&self) -> &'static str {
        "aws-sqs"
    }

    fn directions(&self) -> Directions {
        Directions::BOTH
    }

    /// Every message one receive hands back, each deleted once it is a
    /// Stream.
    fn receive(&self) -> Result<Vec<Arrived>> {
        let client = self.client();
        let mut arrived = Vec::new();
        for message in client.receive_message(&self.queue_url, self.wait)? {
            client.delete_message(&self.queue_url, &message.receipt_handle)?;
            arrived.push(Arrived::new(
                session::origin(&self.queue_url, &message.id),
                message.body,
            ));
        }
        Ok(arrived)
    }

    fn send(&self, target: &str, bytes: &[u8]) -> Result<()> {
        ceiling::within(bytes.len(), ceiling(), "one SQS message carries")?;
        self.client()
            .send_message(self.resolve(target), bytes)
            .map(|_| ())
    }
}

impl Configured for SqsTransport {
    /// The address is the queue URL, `https://sqs.<region>.amazonaws.com/
    /// <account>/<queue>`. The access key and its secret are the Location's
    /// credentials, not settings: a secret never is.
    const SETTINGS: &'static Settings = &Settings {
        technology: env!("CARGO_PKG_NAME"),
        settings: &[
            Setting {
                name: "region",
                kind: Kind::Text,
                presence: Presence::Required,
                meaning: "The AWS region requests are signed for, eu-north-1.",
                applies: Applies::Both,
            },
            Setting {
                name: "wait_seconds",
                kind: Kind::Integer {
                    minimum: 0,
                    maximum: 20,
                },
                presence: Presence::Optional,
                meaning: "How many seconds a receive long-polls an empty queue for a \
                          message; it answers at once when left out.",
                applies: Applies::Receive,
            },
            Setting {
                name: "timeout",
                kind: Kind::Duration,
                presence: Presence::Optional,
                meaning: "How long an endpoint that stops answering is waited on; unbounded \
                          when left out.",
                applies: Applies::Both,
            },
        ],
    };

    fn configured(address: &str, settings: &Read) -> Result<Self> {
        // The access key and secret come through the Location's credentials.
        let mut transport = Self::new(address, settings.text("region"));
        if let Some(seconds) = settings.optional_integer("wait_seconds") {
            // The declaration holds it to 0..=20; waiting caps it again.
            transport = transport.waiting(u8::try_from(seconds).unwrap_or(u8::MAX));
        }
        Ok(match settings.optional_duration("timeout") {
            Some(timeout) => transport.timing_out_after(timeout),
            None => transport,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use std::thread::JoinHandle;
    use transport::loopback::Loopback;
    use transport::socket;

    fn node(queue_url: &str, secret: &str) -> SqsTransport {
        SqsTransport::new(queue_url, "eu-north-1")
            .with_credentials("AKID", secret)
            .waiting(1)
            .timing_out_after(Duration::from_secs(2))
    }

    #[test]
    fn sqs_declares_its_settings_and_reads_through_them() {
        use xcore::settings::Given;
        assert_eq!(SqsTransport::SETTINGS.problems(), Vec::<String>::new());
        let queue_url = "https://sqs.eu-north-1.amazonaws.com/123456789012/orders";
        let given = [
            ("region".to_string(), Given::Text("eu-north-1".to_string())),
            ("wait_seconds".to_string(), Given::Integer(20)),
            ("timeout".to_string(), Given::Text("30s".to_string())),
        ];
        let received = SqsTransport::open(queue_url, Applies::Receive, &given).expect("built");
        assert_eq!(received.queue_url, queue_url);
        assert_eq!(
            (received.region.as_str(), received.wait),
            ("eu-north-1", 20)
        );
        assert_eq!(received.timeout, Some(Duration::from_secs(30)));
        let Err(refused) = SqsTransport::open(queue_url, Applies::Send, &given) else {
            panic!("a Send Location does not long-poll");
        };
        assert!(refused.message.contains("\"wait_seconds\""), "{refused}");
    }

    fn serve(
        mut session: Session,
        listener: TcpListener,
        requests: usize,
    ) -> JoinHandle<(Session, Vec<Event>)> {
        std::thread::spawn(move || {
            let events = (0..requests)
                .map(|_| session.serve_one(&listener).expect("served"))
                .collect();
            (session, events)
        })
    }

    #[test]
    fn what_is_sent_to_a_session_is_received_back_and_deleted() {
        let (listener, address) = socket::bind_tcp("127.0.0.1:0").expect("bind");
        let queue = format!("http://{address}/123456789012/orders");
        let near = node(&queue, "secret");
        // Two sends, one receive, then a delete per message.
        let far_end = serve(near.session(), listener, 5);
        near.send("", b"UNA:+.? '").expect("its own queue");
        near.send(&queue, "r\u{e4}k\r\n".as_bytes())
            .expect("a queue URL");
        let arrived = near.receive().expect("received");
        assert_eq!(arrived.len(), 2);
        assert_eq!(arrived[0].bytes, b"UNA:+.? '");
        assert_eq!(arrived[1].bytes, "r\u{e4}k\r\n".as_bytes());
        assert!(arrived[0].origin_uri.starts_with(&format!("{queue}#")));
        let (session, events) = far_end.join().expect("thread");
        assert!(session.messages().is_empty(), "deleted after receive");
        assert_eq!(
            events[0],
            Event::Sent(Arrived::new(
                arrived[0].origin_uri.clone(),
                b"UNA:+.? '".to_vec()
            ))
        );
        assert!(matches!(
            events[2],
            Event::Received {
                count: 2,
                wait: 1,
                ..
            }
        ));
        assert_eq!(
            events
                .iter()
                .filter(|e| matches!(e, Event::Deleted(_)))
                .count(),
            2
        );
    }

    #[test]
    fn a_message_rounds_through_the_loopback_session() {
        let loopback = SqsTransport::loopback();
        let arrived = loopback.round(b"UNA:+.? '").expect("round");
        assert_eq!(arrived.bytes, b"UNA:+.? '");
        assert!(
            arrived.origin_uri.starts_with("http://127.0.0.1:"),
            "{}",
            arrived.origin_uri
        );
        assert!(arrived.origin_uri.contains("/123456789012/orders#"));
        assert_eq!(loopback.ceiling(), Some(ceiling()));
        assert!(loopback.refuses(b"text").is_none());
        assert!(loopback.refuses(&[0xff]).is_some());
    }

    #[test]
    fn the_loopback_returns_the_edges_whole_and_refuses_what_is_not_text() {
        let loopback = SqsTransport::loopback();
        let edges: [(&str, Vec<u8>); 8] = [
            ("empty", Vec::new()),
            ("one byte", vec![0x2a]),
            ("every byte", (0..=255).collect()),
            ("nul run", vec![0; 512]),
            ("high bytes", vec![0xff; 512]),
            ("crlf storm", b"\r\n".repeat(400)),
            ("brim", vec![b'x'; ceiling()]),
            ("over", vec![b'x'; ceiling() + 1]),
        ];
        for (name, payload) in edges {
            let refused = loopback.refuses(&payload).is_some() || payload.len() > ceiling();
            match loopback.round(&payload) {
                Ok(arrived) => {
                    assert!(!refused, "{name} should have been refused");
                    assert_eq!(arrived.bytes, payload, "{name}");
                }
                Err(error) => {
                    assert!(refused, "{name}: {error}");
                    assert!(error.message.starts_with("send failed:"), "{name}: {error}");
                }
            }
        }
    }

    #[test]
    fn a_wrong_secret_is_refused_with_sqss_own_status_and_code() {
        let (listener, address) = socket::bind_tcp("127.0.0.1:0").expect("bind");
        let far_end = serve(node("http://x/q", "secret").session(), listener, 1);
        let failure = node(&format!("http://{address}/123456789012/orders"), "wrong")
            .send("", b"x")
            .expect_err("refused");
        assert!(
            failure.message.contains("403 SignatureDoesNotMatch"),
            "{failure}"
        );
        assert!(!failure.retryable);
        let (_, events) = far_end.join().expect("thread");
        assert_eq!(
            events,
            vec![Event::Refused("SignatureDoesNotMatch".to_string())]
        );
    }

    #[test]
    fn a_queue_is_not_claimed_and_an_unreachable_endpoint_is_retryable() {
        let near = node("http://127.0.0.1:1/123456789012/orders", "secret");
        assert!(near.claims().is_none());
        assert_eq!(near.name(), "aws-sqs");
        assert!(near.directions().receives() && near.directions().sends());
        assert!(near.receive().expect_err("nothing listening").retryable);
        assert!(
            !node("orders.local", "s")
                .send("", b"x")
                .expect_err("no scheme")
                .retryable
        );
        assert_eq!(node("http://x/q", "s").waiting(90).wait, 20);
    }

    #[test]
    fn what_sqs_does_not_carry_is_refused_before_the_wire_with_the_reason() {
        let near = node("http://127.0.0.1:1/123456789012/orders", "secret");
        let over = vec![b'x'; ceiling() + 1];
        let failure = near.send("", &over).expect_err("over the ceiling");
        assert!(!failure.retryable);
        assert!(failure.message.contains("262144"), "{failure}");
        let failure = near.send("", b"").expect_err("empty");
        assert!(!failure.retryable);
        assert!(failure.message.contains("at least one"), "{failure}");
        let failure = near.send("", &[b'r', 0xe4, b'k']).expect_err("Latin-1");
        assert!(!failure.retryable, "{failure}");
        assert!(
            failure
                .message
                .contains("SQS carries a message body as text")
                && failure.message.contains("UTF-8"),
            "{failure}"
        );
        assert!(aws::query::refusal(&[0xff]).is_some());
        assert!(aws::query::refusal(b"text").is_none());
    }
}
