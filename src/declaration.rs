//! This technology's declaration: the transport guide's "The declaration",
//! `module/core/capability/transport/doc/adding-a-transport.md`.
//! `access_key_id` is the AWS crate's, taken by that crate's own
//! `declaration` feature.

use xcore::settings::{Applies, Kind, Presence, Setting, Settings};

/// Every setting a Location takes beyond its address, the queue URL,
/// `https://sqs.<region>.amazonaws.com/<account>/<queue>`. The access key
/// ID is `access_key_id`; its secret access key is the Location's
/// `secret`, never a setting.
pub const SETTINGS: &Settings = &Settings {
    technology: env!("CARGO_PKG_NAME"),
    settings: &[
        Setting {
            name: "region",
            kind: Kind::Text,
            presence: Presence::Required,
            meaning: "The AWS region requests are signed for, eu-north-1.",
            applies: Applies::Both,
        },
        aws::access_key::ACCESS_KEY_ID,
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
        transport::configured::timeout(
            Applies::Both,
            None,
            "How long an endpoint that stops answering is waited on; unbounded \
             when left out.",
        ),
    ],
};

/// This technology's schemes, as [`transport::Configured::SCHEMES`] defines
/// them.
pub const SCHEMES: &[&str] = &["aws-sqs"];
