# Cross-version candidate patches

These are **V2 candidate** patches for the cross-version mode of
`demo/run-deja-demo.sh`:

```
demo/run-deja-demo.sh --cross-version benign-line-shift   # expect PASS  (no false divergence)
demo/run-deja-demo.sh --cross-version real-change         # expect DIVERGENCE (the gate works)
# or:  --candidate-patch demo/cross-version/<file>.patch
```

## How it works

The demo records on **V1** (the current source), then — between record and replay —
applies one of these patches to `vendor/hyperswitch-deja-clean`, rebuilds the host
`router` binary, and replays **that V2 binary** against the V1 recording. The
Dockerfile bakes the host binary, so the replay container runs V2 while the still-
running V1 record container stays pinned to its V1 image. The patch is reverted on
every exit (success / failure / Ctrl-C).

## Constraints (enforced by the script)

- **Vendor-only.** A patch must NOT touch the parent `crates/deja*` instrumentation —
  it must be byte-identical across V1 and V2, or a divergence would be an
  instrumentation artifact rather than a real version diff. The script rejects any
  patch whose diff headers reference `crates/deja`.
- **Must change the binary.** The script asserts the rebuilt router's sha256 differs
  from V1; a no-op patch (which would cache-hit the Docker `COPY` layer and silently
  replay V1) fails loudly.
- Patches are generated against a CLEAN target file (`git -C vendor diff -- <file>`)
  so they apply additively on top of the dirty vendor tree and reverse cleanly.

## The patches

- **`benign-line-shift.patch`** — inserts a comment block above the `PaymentCreate`
  operation in `payment_create.rs`. Every `#[track_caller]` boundary line below it
  shifts, so the rank-5 `SourceLocation` address differs between V1 and V2 — yet every
  boundary still resolves via the version-stable rank-2 `LogicalContext` / rank-3
  `SyntacticHash`. No args change → **no false divergence**.
- **`real-change.patch`** — changes the `payment_attempt` insert's `updated_by`
  column from `""` to `"v2-candidate"`. That value flows into the recorded `db`
  insert, so its `args_hash` differs from V1's — the recorded result is not found and
  the candidate falls through to a live insert: a genuine **`NovelCall` divergence**
  the gate must catch.

## The regression matrix (`run-deja-matrix.sh`)

The matrix records ONE golden V1 baseline and replays every candidate against it.
Beyond `self`/`benign`/`real`, four scenarios each exercise a **distinct detector
cell** (classification × boundary) — together they prove the gate catches every
shape of regression, not just one. (Each correlation is an independent test case;
see the platform design on per-case isolation / parallel replay.)

| patch | change | divergence cell | signature |
|---|---|---|---|
| `real-change` | arg into the **attempt** insert | modified pair · db | novel+omitted at `insert_payment_attempt`; cascade → HTTP 400 |
| `earlier-fork` | arg into the **intent** insert (fires *before* the attempt) | modified pair · db | novel+omitted at `insert_payment_intent` — fork origin **earlier** than `real` |
| `dropped-write` | candidate skips a fire-and-forget redis cache populate (`if false`) | **omitted-only** · redis | ≥1 omitted `set_key`, **0 novel, 0 HTTP diff** — a *silent* lost write |
| `response-only` | overrides one response field (`amount`), no boundary call touched | **HTTP body** · http_incoming | body mismatch with **0 side-effect divergences** |
| `extra-call` | candidate issues a `db` find V1 never made | **novel-only** · db | 1 novel, no omitted pair |

The `dropped-write` and `response-only` cases are the important ones: a regression
that's **invisible in the HTTP response** (a dropped side-effect) and one that's
**invisible in the side-effects** (a wrong response value) — the gate must catch
both, and they have cleanly opposite signatures.
