# xmip-core-transport-aws-sqs

Amazon SQS transport: Signature Version 4 over the Query API — send a Stream as a message, receive with long polling and delete each message once it is a Stream — a queue URL is a Location. A technology of [xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

Signature Version 4 and the Query API come from [xmip-core-transport-aws](https://github.com/IlleNilsson/xmip-core-transport-aws), where every AWS technology shares what AWS speaks over HTTP (ADR-0044, amendment 2026-09-24); HTTP itself comes from [xmip-core-transport-http](https://github.com/IlleNilsson/xmip-core-transport-http).

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
