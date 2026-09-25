//! Both ends of one SQS exchange on this machine (ADR-0051): a [`Session`]
//! at an ephemeral local port takes the one message a transport sends to a
//! queue on it, and the message id it answers is the origin. The ceiling
//! and the refusal are the ones every send is held to: 256 KiB, and text
//! XML permits.

use std::net::TcpListener;

use net::Endpoint;
use transport::Transport;
use transport::error::{Result, protocol_error};
use transport::listening::Listening;
use transport::loopback::{FarEnd, LOOPBACK_TIMEOUT, Loopback, poke};
use transport::socket;

use crate::session::Event;
use crate::{SqsTransport, ceiling};
use aws::query;

/// The queue a loopback message goes to, under the far end's address.
pub const QUEUE: &str = "123456789012/orders";
/// The credential either end holds.
const ACCESS_KEY: &str = "AKID";
const SECRET_KEY: &str = "secret";

impl SqsTransport {
    /// Both ends on this machine: one credential either side, the loopback
    /// timeout on both. The queue is under whatever port the far end binds,
    /// so the URL here is a placeholder the round replaces.
    #[must_use]
    pub fn loopback() -> Self {
        Self::new(format!("http://127.0.0.1:0/{QUEUE}"), "eu-north-1")
            .with_credentials(ACCESS_KEY, SECRET_KEY)
            .timing_out_after(LOOPBACK_TIMEOUT)
    }
}

impl Loopback for SqsTransport {
    fn ceiling(&self) -> Option<usize> {
        Some(ceiling())
    }

    fn refuses(&self, payload: &[u8]) -> Option<String> {
        query::refusal(payload)
    }

    fn far_end(&self) -> Result<Box<dyn FarEnd>> {
        let mut session = self.session();
        let (listener, bound) = socket::bind_tcp("127.0.0.1:0")?;
        Ok(Box::new(Listening::new(
            move |listener: &TcpListener| match session.serve_one(listener)? {
                Event::Sent(arrived) => Ok(arrived),
                Event::Refused(code) => Err(protocol_error(format!("the session refused: {code}"))),
                other => Err(protocol_error(format!("not a send: {other:?}"))),
            },
            (listener, format!("http://{bound}/{QUEUE}")),
        )))
    }

    /// `address` is the queue URL, which is what an SQS address is.
    fn send_to(&self, address: &str, payload: &[u8]) -> Result<()> {
        let near =
            Self::new(address, &self.region).with_credentials(&self.access_key, &self.secret_key);
        match self.timeout {
            Some(timeout) => near.timing_out_after(timeout),
            None => near,
        }
        .send("", payload)
    }

    /// The listener is behind the queue URL's authority.
    fn unblock(&self, address: &str) {
        if let Ok(endpoint) = Endpoint::parse(address) {
            poke(&endpoint.address());
        }
    }
}
