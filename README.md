# xmip-core-transport-aws-sqs

Amazon SQS transport: Signature Version 4 over the Query API — send a Stream as a message, receive with long polling and delete each message once the runtime accepts it — a queue URL is a Location. A technology of [xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

Signature Version 4 and the Query API come from [xmip-core-transport-aws](https://github.com/IlleNilsson/xmip-core-transport-aws), where every AWS technology shares what AWS speaks over HTTP (ADR-0044, amendment 2026-09-24); HTTP itself comes from [xmip-core-transport-http](https://github.com/IlleNilsson/xmip-core-transport-http).

A Stream is bytes and SQS carries a message body as text (ADR-0038, amendment 2026-09-26): the transport takes and hands up bytes, and turns them into UTF-8 text only on the wire. A payload that is not UTF-8 of the characters XML permits is refused at send with the reason, never replaced.

Requests go on connections kept between them (`http::endpoint::Connections`, offering HTTP/1.1): the transport holds them and hands them to every client it makes, so a call costs one exchange and not a connect, a TLS handshake and a `Connection: close`, as it did until 2026-09-27.

## How a received message is acknowledged

A receive deletes nothing: every message it hands on stays in flight, whole, with its receipt handle kept beside it, until the runtime gives its verdict after the whole receive cycle (runtime-model section 5). Accepted deletes the message (`DeleteMessage`). Refused deletes it too: SQS has no call that rejects or dead-letters one message — a queue's redrive policy dead-letters a message only after it was received `maxReceiveCount` times — so a refused message is deleted and not received again; the runtime has audited the refusal, and from Message creation on the Stream is kept in Xmip (ADR-0013). Failed makes it visible again at once (`ChangeMessageVisibility` to zero seconds), so the next receive gets it rather than waiting out the queue's visibility timeout. A crash before the verdict leaves the message to that timeout: at-least-once, never a loss. The delete is the one the receive made until 2026-10-02; the release is one more request on a kept connection, made only for a failed cycle.

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
