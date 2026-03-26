# Section 3 Checklist

Current baseline against `master_plan.md` Section 3.

## 3.1 Core Components

- `done` Retreivr Core remains the deterministic worker/acquisition engine.
- `done` Community Cache exists as the canonical public mapping dataset.
- `partial` Resolution API exists with resolve, bulk resolve, submit, verify, stats, health, snapshot, and diff.
- `not_started` Jellyfin plugin.
- `not_started` Plex / VLC / Home Assistant integrations.

## 3.2 Resolution Priority Order

- `done` Local cache is checked before remote/shared cache paths.
- `partial` Resolution API is present but still needs more live operator validation as the shared network layer.
- `done` Deterministic search remains the fallback acquisition path.
- `done` New successful resolutions can be submitted/published back into the shared dataset.
- `partial` Verification pipeline exists; automatic multi-node verification behavior is still being hardened.
- `partial` Propagation exists through local cache sync primitives and local-first persistence, but still needs live end-to-end validation.

## 3.3 Availability Model

- `done` Resolution API now targets one normalized status model:
  - `verified`
  - `pending`
  - `not_found`
  - `local_only`
- `partial` Downstream UI/plugin consumers still need to adopt these statuses consistently.

## 3.4 Artist Radio / Instant Streaming

- `done` Bulk resolve endpoint exists.
- `partial` Availability-first API surface exists for instant-play consumers.
- `not_started` Artist radio consumer workflow.
- `not_started` Instant streaming consumer integration.

## 3.5 Decentralization Readiness

- `done` Node identity model exists.
- `done` Verification counts and contributor tracking exist.
- `done` Snapshot / diff sync primitives exist.
- `partial` Multi-node operational workflow is scaffolded but not yet proven in production.
- `not_started` Signatures / reputation / anchoring / P2P federation.

## 3.6 Build Order Status

- `done` Deterministic acquisition and canonical tagging pipeline.
- `done` Community cache repo and publish/PR contribution flow.
- `partial` Local cache sync as operator workflow.
- `partial` Auto-submit / auto-verify behavior.
- `partial` Resolution API as first-class infrastructure.
- `next` Normalize availability usage across all consumers.
- `next` Harden unresolved queue behavior and background resolution follow-up.
- `next` Validate the full server -> cache repo -> sync -> resolve loop end to end.
