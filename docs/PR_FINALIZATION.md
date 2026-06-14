# Upstream PR finalization — deja-lean → juspay/hyperswitch (DRAFT)

What's prepared (local, nothing pushed) and the exact push-time steps. The PR
goes **directly to `juspay/hyperswitch` upstream, as a DRAFT**, and depends on a
**public** `deja-lib` (a private repo is not fetchable by reviewers/CI, so the
git dependency would not resolve).

## Done (ready in the repo)

- **`deja-pr`** (vendor branch `vendor/hyperswitch-deja-clean@deja-pr`): a clean
  3-commit series on the pristine upstream tag `2026.04.21.0`, single author
  `Uzair Khan <uzair.khan@juspay.in>`, **no Claude trailers**, the
  `(not for PR)` local commit dropped (`DEJA_ARCHITECTURE.md` + `banks/*.db`
  removed). Content is byte-identical to the demo-proven `deja-lean` minus that
  junk, so it builds (verified by equivalence + the green regression matrix).
  - `feat` instrumentation across db/redis/crypto/id/time seams
  - Kafka record sink + boot + envelope v2 (hardened, marker-audited)
  - record-transport overlay (compose + vector)
- **Hygiene audit of the PR diff (`2026.04.21.0..deja-pr`)**: no hardcoded
  secrets (the only `STRIPE_*` hit is the env *placeholder*
  `STRIPE_API_KEY: ${STRIPE_API_KEY:-}`); every changed file is recognizable
  deja/HS integration; `Cargo.lock` delta included (reviewers may want it split
  out — optional).
- **`deja-lean` is intact** (path dep, buildable) and the vendor dir is checked
  out on it, so the local demo/matrix work exactly as before. Safety refs:
  `vendor: backup/deja-lean-pre-pr`, `parent: backup/parent-pre-pr`.
- **deja-lib is git-dep-consumable as-is** once public: the `deja` facade has no
  internal feature gates, so a git dep needs no `[features]`; HS gates everything
  behind its own `deja` feature → `dep:deja`.

## The one thing that can't be done pre-push

The git dependency rev can only be pinned once `deja-lib` is pushed public, so
`deja-pr` still carries the **path dep** (to stay buildable). The push-time
swap in `vendor/.../crates/router/Cargo.toml` is one line:

```diff
-deja = { path = "../../../../crates/deja", optional = true, default-features = false }
+deja = { git = "https://github.com/<public-deja-lib>", rev = "<pushed-sha>", optional = true, default-features = false }
```

## Finalization sequence (when you decide to push)

1. **Rotate the Stripe test key.** It lived only in untracked `demo/.env` and
   gitignored `harness-state/` recordings (not in the tracked tree), but rotate
   it before anything goes public — the recordings captured it.

2. **Publish `deja-lib` PUBLIC with clean history** (a private repo is not
   consumable):
   ```bash
   scripts/export-public.sh /tmp/deja-lib-public   # fresh single commit, crates-only, vendor excluded; aborts if it finds a real sk_/whsec_ key
   git -C /tmp/deja-lib-public grep -i 'sk_test\|/home/' && echo "LEAK — stop" || echo clean
   # create a PUBLIC repo, then:
   git -C /tmp/deja-lib-public remote add origin git@github.com:<you>/deja-lib.git
   git -C /tmp/deja-lib-public push -u origin main
   ```
   Note the pushed `main` SHA — that's the git-dep `rev`.

3. **Pin the git dep** on `deja-pr` (the one-line swap above) with the public URL
   + pushed SHA, then build-verify:
   ```bash
   ( cd vendor/hyperswitch-deja-clean && git checkout deja-pr && cargo check -p router --features deja,v1 )
   ```

4. **Push `deja-pr`** to your fork of `juspay/hyperswitch`:
   ```bash
   git -C vendor/hyperswitch-deja-clean push <your-hs-fork> deja-pr
   ```

5. **Open the DRAFT PR** against upstream:
   ```bash
   gh pr create --draft --repo juspay/hyperswitch \
     --base main --head <you>:deja-pr \
     --title "feat(deja): feature-gated record/replay instrumentation + Kafka recording sink" \
     --body-file docs/PR_BODY.md
   ```

## Follow-up PR (not in this one)

The Superposition **sampling hook** (§2.2) ships as a second PR — this first one
is instrumentation + hardened Kafka-only sink + envelope v2 only.
