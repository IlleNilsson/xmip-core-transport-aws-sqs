//! The far end: enough of SQS to answer one Location, and what a test or
//! the playground puts on loopback.
//!
//! Not SQS. One session holds the messages of every queue it is asked
//! about in memory, verifies every request against one credential, and
//! answers the three calls with the shapes SQS answers them — the message
//! id, the messages with their receipt handles, the error with its code. A
//! `ReceiveMessage` that finds nothing answers at once and records the wait
//! it was asked for rather than holding the connection; a received message
//! stays in flight until it is deleted, as SQS keeps it. There is no
//! `MD5OfBody`: a Location does not read it, and the estate carries no MD5.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::net::TcpListener;
use std::time::Duration;

use codec::xml::escape;
use transport::Arrived;
use transport::error::Result;

use crate::client::VERSION;
use aws::query::{self, parameter};
use aws::sigv4::Signer;
use http::server;
use net::http::{Request, Response};

/// What the client did, as [`Session::serve_one`] reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// The client sent a message; here is the Stream, its origin the queue
    /// URL and the id it was given.
    Sent(Arrived),
    /// The client received `count` messages from `queue`, asking to wait
    /// `wait` seconds where there were none.
    Received {
        queue: String,
        count: usize,
        wait: u8,
    },
    /// The client deleted this message.
    Deleted(String),
    /// The client was answered with this error code.
    Refused(String),
}

/// One message held, by its queue.
#[derive(Clone, Debug)]
struct Held {
    id: String,
    body: String,
    in_flight: bool,
}

pub struct Session {
    signer: Signer,
    queues: BTreeMap<String, Vec<Held>>,
    next: usize,
    timeout: Option<Duration>,
}

impl Session {
    /// Answer requests signed in `region` as `access_key` with `secret_key`.
    #[must_use]
    pub fn new(region: &str, access_key: &str, secret_key: &str) -> Self {
        Self {
            signer: Signer::new("sqs", region, access_key, secret_key),
            queues: BTreeMap::new(),
            next: 1,
            timeout: None,
        }
    }

    /// Give up on a client that stops mid-request after `timeout`.
    #[must_use]
    pub const fn timing_out_after(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Every message held now, keyed `queue_url#id`, in flight or not.
    #[must_use]
    pub fn messages(&self) -> BTreeMap<String, String> {
        self.queues
            .iter()
            .flat_map(|(queue, held)| {
                held.iter()
                    .map(move |m| (origin(queue, &m.id), m.body.clone()))
            })
            .collect()
    }

    /// Accept one connection on `listener`, answer its one request, and say
    /// what it was.
    ///
    /// # Errors
    /// Where the connection could not be accepted, broke, or sent nothing.
    pub fn serve_one(&mut self, listener: &TcpListener) -> Result<Event> {
        server::serve_one(listener, self.timeout, |request| self.answer(request))
    }

    fn answer(&mut self, request: &Request) -> (Event, Response) {
        if let Err(failure) = self.signer.verify(request) {
            return refused(403, "SignatureDoesNotMatch", &failure.message);
        }
        let parameters = query::parameters(request);
        if parameter(&parameters, "Version") != Some(VERSION) {
            return refused(
                400,
                "InvalidParameterValue",
                "a version this session does not speak",
            );
        }
        let Some(queue) = parameter(&parameters, "QueueUrl").map(str::to_string) else {
            return refused(400, "MissingParameter", "a request naming no QueueUrl");
        };
        match parameter(&parameters, "Action") {
            Some("SendMessage") => self.send(&queue, &parameters),
            Some("ReceiveMessage") => self.receive(&queue, &parameters),
            Some("DeleteMessage") => self.delete(&queue, &parameters),
            _ => refused(400, "InvalidAction", "not one of the three calls"),
        }
    }

    fn send(&mut self, queue: &str, parameters: &[(String, String)]) -> (Event, Response) {
        let Some(body) = parameter(parameters, "MessageBody") else {
            return refused(400, "MissingParameter", "a request with no MessageBody");
        };
        if let Some(why) = query::refusal(body.as_bytes()) {
            return refused(400, "InvalidMessageContents", &why);
        }
        let id = format!("{:08x}-xmip", self.next);
        self.next += 1;
        self.queues
            .entry(queue.to_string())
            .or_default()
            .push(Held {
                id: id.clone(),
                body: body.to_string(),
                in_flight: false,
            });
        let xml = format!(
            "<SendMessageResponse><SendMessageResult><MessageId>{id}</MessageId>\
             </SendMessageResult></SendMessageResponse>"
        );
        (
            Event::Sent(Arrived::new(origin(queue, &id), body.as_bytes())),
            answer(&xml),
        )
    }

    fn receive(&mut self, queue: &str, parameters: &[(String, String)]) -> (Event, Response) {
        let wait = parameter(parameters, "WaitTimeSeconds")
            .and_then(|w| w.parse().ok())
            .unwrap_or(0);
        let most = parameter(parameters, "MaxNumberOfMessages")
            .and_then(|n| n.parse().ok())
            .unwrap_or(1);
        let mut messages = String::new();
        let mut count = 0;
        for held in self.queues.entry(queue.to_string()).or_default() {
            if held.in_flight || count == most {
                continue;
            }
            held.in_flight = true;
            count += 1;
            write!(
                messages,
                "<Message><MessageId>{}</MessageId><ReceiptHandle>rh-{}</ReceiptHandle>\
                 <Body>{}</Body></Message>",
                held.id,
                held.id,
                escape(&held.body)
            )
            .expect("writing to a String cannot fail");
        }
        let xml = format!(
            "<ReceiveMessageResponse><ReceiveMessageResult>{messages}\
             </ReceiveMessageResult></ReceiveMessageResponse>"
        );
        (
            Event::Received {
                queue: queue.to_string(),
                count,
                wait,
            },
            answer(&xml),
        )
    }

    fn delete(&mut self, queue: &str, parameters: &[(String, String)]) -> (Event, Response) {
        let id = parameter(parameters, "ReceiptHandle")
            .and_then(|handle| handle.strip_prefix("rh-"))
            .map(str::to_string);
        let held = self.queues.entry(queue.to_string()).or_default();
        let at = id
            .as_ref()
            .and_then(|id| held.iter().position(|m| &m.id == id && m.in_flight));
        match (id, at) {
            (Some(id), Some(at)) => {
                held.remove(at);
                let xml = "<DeleteMessageResponse><ResponseMetadata><RequestId>xmip\
                           </RequestId></ResponseMetadata></DeleteMessageResponse>";
                (Event::Deleted(origin(queue, &id)), answer(xml))
            }
            _ => refused(
                404,
                "ReceiptHandleIsInvalid",
                "The receipt handle is not valid.",
            ),
        }
    }
}

/// The message `id` in `queue`, as an origin says it.
#[must_use]
pub fn origin(queue: &str, id: &str) -> String {
    format!("{queue}#{id}")
}

fn answer(xml: &str) -> Response {
    Response::new(200)
        .header("Content-Type", "text/xml")
        .body(format!("<?xml version=\"1.0\"?>{xml}").as_bytes())
}

fn refused(status: u16, code: &str, message: &str) -> (Event, Response) {
    (
        Event::Refused(code.to_string()),
        query::error(status, code, message),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const AT: &str = "20260910T000000Z";
    const QUEUE: &str = "http://sqs.local/123456789012/orders";

    fn signed(parameters: &[(&str, &str)]) -> Request {
        let mut all = vec![("Version", VERSION), ("QueueUrl", QUEUE)];
        all.extend_from_slice(parameters);
        Signer::new("sqs", "r", "AKID", "secret").sign(
            query::request("/123456789012/orders", &all).header("Host", "sqs.local"),
            AT,
        )
    }

    #[test]
    fn a_session_answers_in_sqss_shapes_and_refuses_a_bad_signature() {
        let mut session = Session::new("r", "AKID", "secret");
        let sent = signed(&[("Action", "SendMessage"), ("MessageBody", "a<b")]);
        let (event, response) = session.answer(&sent);
        assert_eq!(response.status, 200);
        assert!(
            response
                .text()
                .contains("<MessageId>00000001-xmip</MessageId>")
        );
        let origin = format!("{QUEUE}#00000001-xmip");
        assert_eq!(
            event,
            Event::Sent(Arrived::new(origin.clone(), b"a<b".to_vec()))
        );
        let received = signed(&[("Action", "ReceiveMessage"), ("WaitTimeSeconds", "5")]);
        let (event, response) = session.answer(&received);
        assert!(response.text().contains("<Body>a&lt;b</Body>"));
        assert!(matches!(
            event,
            Event::Received {
                count: 1,
                wait: 5,
                ..
            }
        ));
        let (event, response) = session.answer(&received);
        assert!(
            !response.text().contains("<Message>"),
            "in flight, not offered again"
        );
        assert!(matches!(event, Event::Received { count: 0, .. }));
        let deleted = signed(&[
            ("Action", "DeleteMessage"),
            ("ReceiptHandle", "rh-00000001-xmip"),
        ]);
        let (event, response) = session.answer(&deleted);
        assert_eq!((event, response.status), (Event::Deleted(origin), 200));
        assert!(session.messages().is_empty());
        let (event, response) = session.answer(&deleted);
        assert_eq!(event, Event::Refused("ReceiptHandleIsInvalid".to_string()));
        assert_eq!(response.status, 404);
        let empty = signed(&[("Action", "SendMessage"), ("MessageBody", "")]);
        let (event, _) = session.answer(&empty);
        assert_eq!(event, Event::Refused("InvalidMessageContents".to_string()));
        let (_, response) = session.answer(&signed(&[("Action", "PurgeQueue")]));
        assert_eq!(response.status, 400);
        let other = Signer::new("sqs", "r", "AKID", "wrong")
            .sign(query::request("/q", &[]).header("Host", "sqs.local"), AT);
        let (event, response) = session.answer(&other);
        assert_eq!(event, Event::Refused("SignatureDoesNotMatch".to_string()));
        assert_eq!(response.status, 403);
    }
}
