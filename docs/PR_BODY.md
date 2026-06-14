# feat(deja): feature-gated record/replay instrumentation + Kafka recording sink

> **Draft.** Opening early for direction/feedback. Gated entirely behind a new
> `deja` cargo feature — **zero impact on the default build** (no new
> dependencies, no codegen, no runtime cost when the feature is off).

## What this is

[Déjà](https://github.com/<public-deja-lib>) is a deterministic record/replay
harness for service boundaries. This PR adds the **record-side integration** to
Hyperswitch: with `--features deja` and `DEJA_MODE=record`, every storage/cache/
crypto/id/time boundary call emits a structured `SemanticEvent`, published to
Kafka and landed in object storage. A separate harness then replays a recorded
request stream against a candidate build and scores divergences — a regression
gate that catches behavior changes the response alone wouldn't reveal (e.g. a
dropped cache write, an altered DB insert payload).

## What's in it

1. **Feature-gated instrumentation** across the db (diesel `generic_*`), redis,
   crypto, id, and time seams, plus correlation/request-id propagation and an
   optional execution-graph layer. All annotations are attribute macros behind
   `#[cfg(feature = "deja")]` — no-ops when the feature is off.
2. **Kafka record sink + boot wiring + envelope v2.** A deja-owned `rdkafka`
   producer (`acks=all`, idempotent, real `flush()`) publishes
   `deja.artifact_record/v2` envelopes (producer/capture/code provenance) to a
   recording topic; `deja_boot` installs it as the sole record sink in record
   mode. Loss accounting rides the same topic as `deja_sink_marker` records.
3. **Opt-in record-transport overlay** — a `docker-compose.deja.yml` + Vector
   config that stand up the Kafka → Vector → S3 pipeline alongside the stock
   stack. (Can be dropped from the PR if you'd rather it live out-of-tree.)

## Impact when off (the important part)

- `deja` is an **optional** dependency; default builds don't compile or link it.
- Every instrumentation site is `#[cfg(feature = "deja")]` — identical codegen
  to upstream when the feature is absent.
- No change to default behavior, configs, or the public API.

## Dependency

`deja` is consumed as a **rev-pinned git dependency** on the public deja-lib
repo (not vendored into this tree). See `crates/router/Cargo.toml`.

## Not in this PR (follow-up)

The Superposition-driven **ingress sampling hook** (decide-at-ingress whether to
record a given request) lands as a second PR; this one is instrumentation +
hardened sink + envelope v2.

## Status / asks

- [ ] Direction: is a feature-gated record/replay hook something you'd take
      upstream, or prefer maintained as a fork/out-of-tree integration?
- [ ] Should the compose/Vector overlay live in-tree (item 3) or separately?
- [ ] `Cargo.lock` delta — keep in the PR or split?
