use std::collections::BTreeSet;

use proc_macro2::TokenStream;
use quote::quote;
use syn::{
    parenthesized,
    parse::{Parse, ParseStream},
    parse_quote, Expr, FnArg, Ident, ItemFn, LitStr, Pat, Result, Token,
};

pub fn generate(args: InstrumentArgs, func: ItemFn) -> TokenStream {
    generate_with_boundary(args, func, None)
}

pub fn generate_with_boundary(
    args: InstrumentArgs,
    func: ItemFn,
    default_boundary: Option<&'static str>,
) -> TokenStream {
    generate_with_preset(args, func, default_boundary, Preset::None)
}

/// Declarative-boundary PRESET defaults a macro entry point supplies. A preset
/// pre-fills the declared semantics (`deja::id`/`time`/`http` stay one word per
/// the design table §1); an EXPLICIT `channel`/`effect`/`strategy` argument still
/// overrides the preset default. `Preset::None` declares nothing — the boundary
/// stays undeclared and falls back to the runtime heuristics.
#[derive(Clone, Copy)]
pub enum Preset {
    /// No declared defaults (`#[deja::redis]`, `#[deja::instrument]`, ...).
    None,
    /// `deja::id` ⇒ `Channel::Entropy(EntropySource::Id)`, no effect.
    Id,
    /// `deja::time` ⇒ `Channel::Entropy(EntropySource::Clock)`, no effect.
    Time,
    /// `deja::http(outgoing)` ⇒ `Channel::Egress`, no effect.
    HttpOutgoing,
    /// `deja::http(incoming)` ⇒ NO declared channel. Ingress is the replay DRIVER
    /// (correlation seed), not a reconstructed effect, so it is outside the effect
    /// taxonomy now that the `Ingress` channel variant is dropped. We still record
    /// the `http_incoming` event exactly as before (it drives replay), but declare
    /// nothing — the boundary stays undeclared and the heuristic fallback applies.
    HttpIncoming,
}

pub fn generate_with_preset(
    mut args: InstrumentArgs,
    func: ItemFn,
    default_boundary: Option<&'static str>,
    preset: Preset,
) -> TokenStream {
    if args.boundary.is_none() {
        args.boundary = default_boundary.map(lit_str);
    }

    generate_inner(args, func, preset)
}

pub fn generate_http(attr: proc_macro::TokenStream, item: proc_macro::TokenStream) -> TokenStream {
    let (boundary, args) = parse_http_args(attr);
    // `deja::http(incoming)` is the replay DRIVER (correlation seed): it still
    // records the `http_incoming` event exactly as before, but declares NO channel
    // (ingress is outside the effect taxonomy now). Every other http boundary is
    // the Egress preset.
    let preset = if boundary == "http_incoming" {
        Preset::HttpIncoming
    } else {
        Preset::HttpOutgoing
    };
    match syn::parse::<ItemFn>(item) {
        Ok(func) => generate_with_preset(args, func, Some(boundary), preset),
        Err(error) => error.to_compile_error(),
    }
}

fn generate_inner(args: InstrumentArgs, mut func: ItemFn, preset: Preset) -> TokenStream {
    let sig = &func.sig;
    let vis = &func.vis;
    let block = &func.block;

    let replay = args.replay;
    let replay_ok = args.replay_ok;
    let replay_with = args.replay_with;
    let boundary = args.boundary.unwrap_or_else(|| lit_str("function"));
    let boundary_str = boundary.value();
    let component = args
        .component
        .map_or_else(|| quote!(module_path!()), |value| quote!(#value));
    // The string form of `operation`, known at expansion time: the explicit
    // `operation = "..."` literal, or the function's own name. Used to derive a
    // stable syntactic hash and the rank-3 occurrence scope.
    let operation_str = args
        .operation
        .as_ref()
        .map_or_else(|| sig.ident.to_string(), |value| value.value());
    let operation = args.operation.map_or_else(
        || {
            let name = sig.ident.to_string();
            quote!(#name)
        },
        |value| quote!(#value),
    );

    // DECLARATIVE BOUNDARY MODEL: resolve the declared semantics (preset default
    // ⊕ explicit args) into the `BoundarySpec` constructor expression used at
    // every emit site. When NOTHING is declared this is exactly
    // `BoundarySpec::new(#boundary, #component, #operation)` — byte-identical
    // tokens to before this slice, so undeclared boundaries are unchanged. On a
    // declaration error (bad variant, or RMW without `strategy`) this is a
    // `compile_error!` token we surface immediately.
    let boundary_spec_expr = match build_boundary_spec_expr(
        &boundary,
        &component,
        &operation,
        args.channel.as_ref(),
        args.effect.as_ref(),
        args.strategy.as_ref(),
        preset,
        sig,
    ) {
        Ok(expr) => expr,
        Err(err) => return err,
    };

    let args_expr = args
        .args
        .unwrap_or_else(|| inferred_args_expr(sig, &args.skip, args.skip_all, &args.fields));

    // --- CallsiteIdentity (rank-2 LogicalContext + rank-3 SyntacticHash + rank-4
    //     LexicalPath) ------------------------------------------------------------
    //
    // A proc-macro attribute sees the function DEFINITION tokens, not the
    // invocation site, so it cannot hash the true call-site syntax. What it CAN
    // hash deterministically — and what stays stable across source line shifts
    // AND benign function-signature edits — is `boundary :: operation`.
    //
    // We DELIBERATELY do NOT fold the signature into the hash. Deja's purpose is
    // CROSS-VERSION regression (record on V1, replay on V2): a benign signature
    // edit (param rename/reorder, a type-alias change, a return-type tweak) must
    // NOT change a call-site's syntactic-hash identity, or that address would
    // miss on V2 and the call would silently demote to a weaker/positional rank —
    // a false regression. This matches the hand-written DB path, which already
    // hashes `boundary::component::operation` with no signature (deja/src/lib.rs).
    //
    // We compute the FNV-1a hash here at expansion time and emit it as a `u64`
    // literal (the rank-3 SyntacticHash address). The rank-4 lexical path is the
    // runtime `module_path!()`, and the rank-2 LogicalContext is the runtime
    // span-path (`current_logical_span_path()`, stamped below). Identity emission
    // is ADDITIVE: rank-5 (caller location) and rank-6 (positional sequence)
    // remain intact as fallbacks via `addresses_for`.
    let syntax_hash_input = format!("{}::{}", boundary_str, operation_str);
    let syntax_hash_value = syntactic_hash(&syntax_hash_input);
    // The occurrence scope key: per-method, matching the delegate (recordable)
    // path's `"{trait}::{method}"` granularity.
    let identity_scope_expr = quote! { format!("{}::{}", #component, #operation) };
    // Build the `CallsiteIdentity` ONCE per invocation. `occurrence` is the only
    // runtime field: it is allocated EXACTLY ONCE here via
    // `next_boundary_occurrence` and then reused for BOTH the replay lookup and
    // the recorded event, keeping record/replay occurrence numbering aligned.
    // The correlation id used for the occurrence bucket is the explicit
    // correlation (if any) falling back to the ambient one — the same value the
    // recorded event carries — so the renderer and hook bucket identically.
    let identity_build: TokenStream = quote! {
        let __deja_identity_scope: ::std::string::String = { #identity_scope_expr };
        let __deja_identity_correlation: ::std::option::Option<::std::string::String> =
            match &__deja_boundary_correlation_id {
                ::std::option::Option::Some(c) => ::std::option::Option::Some(c.clone()),
                ::std::option::Option::None => ::deja::__private::current_correlation_id(),
            };
        let __deja_identity = ::deja::__private::CallsiteIdentity {
            version: 1,
            source: ::deja::__private::CallsiteSource::SyntacticHash,
            id: ::std::option::Option::None,
            scope: ::std::option::Option::Some(__deja_identity_scope.clone()),
            occurrence: ::deja::__private::next_boundary_occurrence(
                __deja_identity_correlation.as_deref(),
                ::deja::__private::CallsiteSource::SyntacticHash,
                ::std::option::Option::Some(__deja_identity_scope.as_str()),
            ),
            caller_function: ::std::option::Option::Some(::std::module_path!().to_string()),
            lexical_path: ::std::option::Option::Some(::std::module_path!().to_string()),
            syntax_hash: ::std::option::Option::Some(#syntax_hash_value),
            logical_context: ::deja::__private::current_logical_span_path(),
        };
    };

    // A replayable boundary must record its result LOSSLESSLY so replay can
    // reconstruct it: `result_serialize` (whole value) for `replay`,
    // `result_serialize_ok` (Ok arm only) for `replay_ok`. `replay_with`
    // boundaries supply their own `result =` shape. Non-replay keeps the cheap
    // (unrecoverable) Debug capture.
    let result_expr: Expr = args.result.unwrap_or_else(|| {
        if replay {
            parse_quote!(::deja::value::result_serialize(__deja_result))
        } else if replay_ok {
            parse_quote!(::deja::value::result_serialize_ok(__deja_result))
        } else {
            parse_quote!(::deja::value::result_debug(__deja_result))
        }
    });
    let correlation_expr = args
        .correlation
        .unwrap_or_else(|| parse_quote!(None::<String>));

    // The function's return type, used to deserialize a replayed result.
    let ret_ty: TokenStream = match &sig.output {
        syn::ReturnType::Default => quote!(()),
        syn::ReturnType::Type(_, ty) => quote!(#ty),
    };

    // The type the reconstruct closure targets — i.e. the `T` that
    // `dispatch`/`dispatch_async` resolves to. For sync and `async fn` bodies
    // that is the return type itself. For a `future = "boxed"` body the seam
    // resolves the FUTURE'S OUTPUT (the macro re-wraps it in `Box::pin`), so the
    // reconstruct target is that inner output, not the un-deserializable
    // `Pin<Box<dyn Future>>`.
    let recon_ty: TokenStream = if matches!(args.future, Some(FutureMode::Boxed)) {
        boxed_future_output_ty(&sig.output).unwrap_or_else(|| ret_ty.clone())
    } else {
        ret_ty.clone()
    };

    if !func
        .attrs
        .iter()
        .any(|attr| attr.path().is_ident("track_caller"))
    {
        func.attrs.push(parse_quote!(#[track_caller]));
    }
    let attrs = &func.attrs;

    // The reconstruct closure handed to `dispatch`/`dispatch_async`. It is the
    // type-erased deserializer (design §3): on a lookup HIT, `dispatch` calls it
    // to turn the recorded JSON back into the return type, returning `None` to
    // FALL THROUGH to live execution (the V1 "skip error arms" policy — a recorded
    // `Err` sentinel or a deserialize failure becomes `None`).
    //
    //  - replay_with: a user expr yielding `Option<ReturnType>` (e.g. rebuild a
    //    reqwest::Response from recorded parts) — used directly as the closure body.
    //  - replay_ok: Result Ok-only — deserialize the recorded value into the Ok
    //    type `R` (first generic arg) and return `Some(Ok(R))`; never touches the
    //    (possibly non-serde) error type.
    //  - replay: direct — deserialize into the whole return type.
    //  - non-replay (record-only): `|_| None` — `dispatch` never reaches it
    //    because the lookup seam returns `None` for a recording hook, AND the
    //    `DeserializeOwned` capability is confined to this closure so record-only
    //    return types need no bound.
    //
    // The `DeserializeOwned` requirement therefore lives ONLY in this closure on
    // the replay path; record-only boundaries emit `|_| None` and compile without
    // any serde-deserialize bound on the return type.
    let reconstruct_closure: TokenStream = if let Some(expr) = &replay_with {
        quote! {
            |__deja_recorded: ::serde_json::Value| -> ::std::option::Option<#recon_ty> {
                let _ = &__deja_recorded;
                { #expr }
            }
        }
    } else if replay_ok {
        // The Ok type to deserialize into: the first generic arg of the
        // reconstruct target (`CustomResult<R, E>` → `R`). For boxed bodies that
        // target is the future's output, so `replay_ok` reaches the right Result.
        let ok_ty = match first_generic_arg_of_output(&sig.output, args.future) {
            Some(ty) => ty,
            None => {
                return syn::Error::new_spanned(
                    &sig.ident,
                    "`replay_ok` requires a Result-like return type with a generic Ok argument (e.g. CustomResult<R, E>)",
                )
                .to_compile_error();
            }
        };
        quote! {
            |__deja_recorded: ::serde_json::Value| -> ::std::option::Option<#recon_ty> {
                match ::serde_json::from_value::<#ok_ty>(__deja_recorded) {
                    ::std::result::Result::Ok(__deja_replayed) =>
                        ::std::option::Option::Some(::std::result::Result::Ok(__deja_replayed)),
                    ::std::result::Result::Err(_) => ::std::option::Option::None,
                }
            }
        }
    } else if replay {
        quote! {
            |__deja_recorded: ::serde_json::Value| -> ::std::option::Option<#recon_ty> {
                ::serde_json::from_value::<#recon_ty>(__deja_recorded).ok()
            }
        }
    } else {
        // Record-only: the lookup path is never hit (the recording hook returns
        // `None`), so this closure is unreachable; it imposes NO bound on
        // `#recon_ty` / the return type.
        quote! {
            |_: ::serde_json::Value| -> ::std::option::Option<#recon_ty> {
                ::std::option::Option::None
            }
        }
    };

    if sig.asyncness.is_some() {
        if args.future.is_some() {
            return syn::Error::new_spanned(
                &sig.ident,
                "`future = \"boxed\"` is only valid on non-async functions that return a boxed future",
            )
            .to_compile_error();
        }

        // ONE shape: build identity (occurrence allocated once), then call the
        // single `dispatch_async` seam. The macro names NO replay-only operation:
        // it hands `dispatch_async` the args thunk, the block as a run thunk, a
        // typed reconstruct closure, and the lossless result extractor. All of
        // run/skip/shadow/record control flow lives inside the seam.
        quote! {
            #(#attrs)*
            #vis #sig {
                let __deja_boundary_correlation_id: Option<String> = { #correlation_expr };
                #identity_build
                ::deja::__private::dispatch_async(
                    ::deja::__private::CrossingObservation::with_correlation(
                        #boundary_spec_expr,
                        __deja_identity,
                        ::std::panic::Location::caller(),
                        __deja_boundary_correlation_id,
                    ),
                    {
                        // Evaluate args EAGERLY into an owned value (ending any
                        // borrow it holds, e.g. `&request`) BEFORE the run thunk
                        // moves that value; then hand `dispatch` an owning thunk.
                        // Gated so the inactive path never runs the args expr.
                        let __deja_boundary_args = if ::deja::__private::capture_is_active() {
                            #args_expr
                        } else {
                            ::serde_json::Value::Null
                        };
                        move || __deja_boundary_args
                    },
                    move || async move #block,
                    #reconstruct_closure,
                    move |__deja_result| { #result_expr },
                ).await
            }
        }
    } else if matches!(args.future, Some(FutureMode::Boxed)) {
        // Boxed-future shape: the fn is sync but returns `Pin<Box<dyn Future>>`.
        // `dispatch_async` resolves the inner future and yields the inner output;
        // the macro wraps that in `Box::pin`. The run thunk evaluates the block
        // (which yields the inner future) and awaits it.
        quote! {
            #(#attrs)*
            #vis #sig {
                let __deja_boundary_correlation_id: Option<String> = { #correlation_expr };
                #identity_build
                ::std::boxed::Box::pin(::deja::__private::dispatch_async(
                    ::deja::__private::CrossingObservation::with_correlation(
                        #boundary_spec_expr,
                        __deja_identity,
                        ::std::panic::Location::caller(),
                        __deja_boundary_correlation_id,
                    ),
                    // `move` on the args thunk too: the whole `dispatch_async`
                    // future is `Box::pin`-returned from this sync fn, so every
                    // capture must be owned (mirrors the pre-`dispatch` boxed
                    // shape, which bound args to an owned `Value` before moving
                    // the block into the returned future).
                    {
                        // Eager owned args (ends borrows like `&request`) before
                        // the boxed future captures/moves them; gated for the
                        // zero-cost inactive path.
                        let __deja_boundary_args = if ::deja::__private::capture_is_active() {
                            #args_expr
                        } else {
                            ::serde_json::Value::Null
                        };
                        move || __deja_boundary_args
                    },
                    move || async move { #block.await },
                    #reconstruct_closure,
                    move |__deja_result| { #result_expr },
                ))
            }
        }
    } else {
        // Sync shape: the single `dispatch` seam, block as a sync run thunk.
        quote! {
            #(#attrs)*
            #vis #sig {
                let __deja_boundary_correlation_id: Option<String> = { #correlation_expr };
                #identity_build
                ::deja::__private::dispatch(
                    ::deja::__private::CrossingObservation::with_correlation(
                        #boundary_spec_expr,
                        __deja_identity,
                        ::std::panic::Location::caller(),
                        __deja_boundary_correlation_id,
                    ),
                    {
                        // Eager owned args (ends borrows like `&request`) before
                        // the run thunk moves that value; gated so the inactive
                        // path never runs the args expr.
                        let __deja_boundary_args = if ::deja::__private::capture_is_active() {
                            #args_expr
                        } else {
                            ::serde_json::Value::Null
                        };
                        move || __deja_boundary_args
                    },
                    || #block,
                    #reconstruct_closure,
                    move |__deja_result| { #result_expr },
                )
            }
        }
    }
}

/// Build the `BoundarySpec` constructor expression for the declarative boundary
/// model. Combines the PRESET defaults (`deja::id`/`time`/`http`) with the
/// EXPLICIT `channel`/`effect`/`strategy` arguments (explicit wins), validates
/// each variant identifier, ENFORCES the locked RMW rule (a `ReadModifyWrite`
/// effect MUST declare `strategy`), and emits either:
///   - `BoundarySpec::new(b, c, o)` when NOTHING is declared (byte-identical to
///     the pre-slice tokens — undeclared boundaries unchanged), or
///   - `BoundarySpec::with_semantics(b, c, o, BoundarySemantics { .. })` when any
///     field is declared.
///
/// Returns `Err(compile_error_tokens)` on a bad variant or the RMW violation.
#[allow(clippy::too_many_arguments)]
fn build_boundary_spec_expr(
    boundary: &LitStr,
    component: &TokenStream,
    operation: &TokenStream,
    channel: Option<&Ident>,
    effect: Option<&Ident>,
    strategy: Option<&Ident>,
    preset: Preset,
    sig: &syn::Signature,
) -> std::result::Result<TokenStream, TokenStream> {
    // Preset channel default (overridden by an explicit `channel` arg). Carried as
    // a full `Channel::*` TokenStream because the entropy presets embed an
    // `EntropySource` payload. `HttpIncoming` declares NOTHING — it is the replay
    // driver, outside the effect taxonomy. Effect has NO preset default (Effect is
    // a State-only concept; the entropy/egress presets declare no effect).
    let preset_channel: Option<TokenStream> = match preset {
        Preset::None | Preset::HttpIncoming => None,
        Preset::Id => Some(entropy_channel("Id")),
        Preset::Time => Some(entropy_channel("Clock")),
        Preset::HttpOutgoing => Some(quote!(::deja::__private::Channel::Egress)),
    };

    // Resolve the channel: explicit arg → validated variant tokens; else preset
    // default; else None (undeclared).
    let channel_tokens = match channel {
        Some(id) => Some(channel_variant(id)?),
        None => preset_channel,
    };

    // The effect VARIANT NAME (string) is needed for the RMW rule; the tokens for
    // emission. Closure form of `effect` is OUT OF SCOPE (deferred) — only the
    // constant identifier form is accepted here. Effect has no preset default.
    let (effect_name, effect_tokens): (Option<String>, Option<TokenStream>) = match effect {
        Some(id) => {
            let tokens = effect_variant(id)?;
            (Some(id.to_string()), Some(tokens))
        }
        None => (None, None),
    };

    let strategy_tokens = match strategy {
        Some(id) => Some(strategy_variant(id)?),
        None => None,
    };

    // LOCKED RULE: a `ReadModifyWrite` effect MUST declare `strategy`. The macro
    // emits a COMPILE ERROR (no default) — safe-by-construction (design Decision 1).
    if effect_name.as_deref() == Some("ReadModifyWrite") && strategy.is_none() {
        return Err(syn::Error::new_spanned(
            &sig.ident,
            "a ReadModifyWrite boundary must declare strategy = LookupAndSeed | SeedAndExecute",
        )
        .to_compile_error());
    }

    // Nothing declared → emit the byte-identical legacy constructor.
    if channel_tokens.is_none() && effect_tokens.is_none() && strategy_tokens.is_none() {
        return Ok(quote! {
            ::deja::__private::BoundarySpec::new(#boundary, #component, #operation)
        });
    }

    let channel_field = option_field(channel_tokens);
    let effect_field = option_field(effect_tokens);
    let strategy_field = option_field(strategy_tokens);

    Ok(quote! {
        ::deja::__private::BoundarySpec::with_semantics(
            #boundary,
            #component,
            #operation,
            ::deja::__private::BoundarySemantics {
                channel: #channel_field,
                effect: #effect_field,
                strategy: #strategy_field,
            },
        )
    })
}

/// Wrap declared-variant tokens in `Some(..)`, or emit `None` when absent.
fn option_field(tokens: Option<TokenStream>) -> TokenStream {
    match tokens {
        Some(t) => quote!(::std::option::Option::Some(#t)),
        None => quote!(::std::option::Option::None),
    }
}

/// `Channel::Entropy(EntropySource::<src>)` tokens for the entropy presets
/// (`deja::id` → `Id`, `deja::time` → `Clock`).
fn entropy_channel(src: &str) -> TokenStream {
    let ident = Ident::new(src, proc_macro2::Span::call_site());
    quote!(::deja::__private::Channel::Entropy(
        ::deja::__private::EntropySource::#ident
    ))
}

/// Validate + map a `channel = <Ident>` to `Channel::<Variant>` tokens. A bare
/// `channel = Entropy` (no source) defaults to
/// `Channel::Entropy(EntropySource::Other("unspecified"))` — the presets
/// (`deja::id`/`time`) supply the real source.
fn channel_variant(id: &Ident) -> std::result::Result<TokenStream, TokenStream> {
    match id.to_string().as_str() {
        "State" => Ok(quote!(::deja::__private::Channel::State)),
        "Egress" => Ok(quote!(::deja::__private::Channel::Egress)),
        "Entropy" => Ok(quote!(::deja::__private::Channel::Entropy(
            ::deja::__private::EntropySource::Other("unspecified".to_string())
        ))),
        _ => Err(syn::Error::new_spanned(
            id,
            "unknown channel; expected one of State | Entropy | Egress",
        )
        .to_compile_error()),
    }
}

/// Validate + map an `effect = <Ident>` (CONSTANT form) to `Effect::<Variant>`
/// tokens. Effect is a STATE-ONLY concept. The closure form is deferred (out of
/// scope for this slice).
fn effect_variant(id: &Ident) -> std::result::Result<TokenStream, TokenStream> {
    match id.to_string().as_str() {
        "Read" | "Write" | "ReadModifyWrite" | "Append" | "VolatileRead" | "Opaque" => {
            Ok(quote!(::deja::__private::Effect::#id))
        }
        _ => Err(syn::Error::new_spanned(
            id,
            "unknown effect; expected one of Read | Write | ReadModifyWrite | Append | VolatileRead | Opaque",
        )
        .to_compile_error()),
    }
}

/// Validate + map a `strategy = <Ident>` to `Strategy::<Variant>` tokens.
fn strategy_variant(id: &Ident) -> std::result::Result<TokenStream, TokenStream> {
    match id.to_string().as_str() {
        "Lookup" | "SeedAndExecute" | "LookupAndSeed" => {
            Ok(quote!(::deja::__private::Strategy::#id))
        }
        _ => Err(syn::Error::new_spanned(
            id,
            "unknown strategy; expected Lookup | SeedAndExecute | LookupAndSeed",
        )
        .to_compile_error()),
    }
}

fn inferred_args_expr(
    sig: &syn::Signature,
    skipped: &[Ident],
    skip_all: bool,
    fields: &[FieldArg],
) -> Expr {
    let skipped: BTreeSet<String> = skipped.iter().map(ToString::to_string).collect();
    let mut inserts = Vec::new();

    if !skip_all {
        for input in &sig.inputs {
            let FnArg::Typed(pat_type) = input else {
                continue;
            };
            let Pat::Ident(pat_ident) = pat_type.pat.as_ref() else {
                continue;
            };
            let ident = &pat_ident.ident;
            let ident_string = ident.to_string();
            if ident_string == "_" || skipped.contains(&ident_string) {
                continue;
            }
            let key = ident_string;
            inserts.push(quote! {
                __deja_boundary_map.insert(
                    #key.to_string(),
                    ::deja::value::debug(&#ident),
                );
            });
        }
    }

    for field in fields {
        let key = field.name.value();
        let expr = &field.expr;
        inserts.push(quote! {
            __deja_boundary_map.insert(
                #key.to_string(),
                ::deja::value::debug(&(#expr)),
            );
        });
    }

    parse_quote!({
        let mut __deja_boundary_map = ::serde_json::Map::new();
        #(#inserts)*
        ::serde_json::Value::Object(__deja_boundary_map)
    })
}

fn parse_http_args(attr: proc_macro::TokenStream) -> (&'static str, InstrumentArgs) {
    let attr_ts: TokenStream = attr.into();
    if attr_ts.is_empty() {
        return ("http_outgoing", InstrumentArgs::default());
    }

    let tokens: Vec<_> = attr_ts.clone().into_iter().collect();
    let Some(proc_macro2::TokenTree::Ident(first)) = tokens.first() else {
        let args = syn::parse2(attr_ts).unwrap_or_default();
        return ("http_outgoing", args);
    };

    let boundary = match first.to_string().as_str() {
        "incoming" => Some("http_incoming"),
        "outgoing" => Some("http_outgoing"),
        _ => None,
    };

    let Some(boundary) = boundary else {
        let args = syn::parse2(attr_ts).unwrap_or_default();
        return ("http_outgoing", args);
    };

    let rest = if matches!(
        tokens.get(1),
        Some(proc_macro2::TokenTree::Punct(punct)) if punct.as_char() == ','
    ) {
        tokens[2..].iter().cloned().collect()
    } else {
        TokenStream::new()
    };
    let args = syn::parse2(rest).unwrap_or_default();
    (boundary, args)
}

fn lit_str(value: &'static str) -> LitStr {
    LitStr::new(value, proc_macro2::Span::call_site())
}

/// FNV-1a hash of a string, computed at macro-expansion time and emitted as a
/// `u64` literal for `CallsiteIdentity::syntax_hash` (rank-3
/// `Address::SyntacticHash`).
///
/// MUST stay byte-for-byte identical to `deja_record::stable_callsite_hash`
/// (FNV-1a over the bytes, then a `0xff` terminator) so a hash computed here at
/// compile time matches one computed at runtime for the same input. FNV-1a is
/// chosen over `std::hash::DefaultHasher` because it is fully specified and
/// stable across rustc/syn versions — the input string (which includes the
/// function signature tokens) never changes its hash for a given logical
/// boundary, so record and replay agree regardless of source line shifts.
pub(crate) fn syntactic_hash(input: &str) -> u64 {
    const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = FNV_OFFSET_BASIS;
    for byte in input.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    // Terminator byte, matching `fnv1a_str` in deja-record.
    hash ^= u64::from(0xffu8);
    hash.wrapping_mul(FNV_PRIME)
}

/// Extract the first generic type argument of a function's return type — e.g.
/// `R` from `CustomResult<R, E>` or `StorageResult<R>`. Used by `replay_ok` to
/// find the `Ok` type to deserialize into, without requiring the (possibly
/// non-serde) error type.
fn first_generic_arg(output: &syn::ReturnType) -> Option<TokenStream> {
    let ty = match output {
        syn::ReturnType::Type(_, ty) => ty.as_ref(),
        syn::ReturnType::Default => return None,
    };
    first_generic_arg_of_type(ty)
}

/// `replay_ok`'s Ok-type extractor, aware of the `future = "boxed"` shape. For a
/// boxed body the reconstruct target is the future's OUTPUT type (the macro
/// re-wraps in `Box::pin`), so the Ok type is the first generic of THAT, not of
/// the `Pin<Box<dyn Future>>`. For all other bodies it is the first generic of
/// the return type.
fn first_generic_arg_of_output(
    output: &syn::ReturnType,
    future: Option<FutureMode>,
) -> Option<TokenStream> {
    if matches!(future, Some(FutureMode::Boxed)) {
        let inner = boxed_future_output_ty(output)?;
        let parsed: syn::Type = syn::parse2(inner).ok()?;
        first_generic_arg_of_type(&parsed)
    } else {
        first_generic_arg(output)
    }
}

/// Extract the first generic type argument of a type — e.g. `R` from
/// `CustomResult<R, E>` or `StorageResult<R>`.
fn first_generic_arg_of_type(ty: &syn::Type) -> Option<TokenStream> {
    if let syn::Type::Path(type_path) = ty {
        if let Some(segment) = type_path.path.segments.last() {
            if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                for arg in &args.args {
                    if let syn::GenericArgument::Type(inner) = arg {
                        return Some(quote!(#inner));
                    }
                }
            }
        }
    }
    None
}

/// Extract `X` from a `future = "boxed"` return type of the shape
/// `Pin<Box<dyn Future<Output = X> (+ Send)? (+ 'lt)?>>`. This is the type the
/// `dispatch_async` seam resolves to for a boxed body (the macro re-wraps it in
/// `Box::pin`), and hence the reconstruct closure's target type. Returns `None`
/// if the return type does not match that shape, in which case the caller falls
/// back to the whole return type (record-only boxed bodies never reach the
/// reconstruct path, so the fallback is harmless there).
fn boxed_future_output_ty(output: &syn::ReturnType) -> Option<TokenStream> {
    fn find_future_output(ty: &syn::Type) -> Option<TokenStream> {
        match ty {
            // `Pin<...>` / `Box<...>` — descend into the angle-bracketed arg.
            syn::Type::Path(type_path) => {
                let segment = type_path.path.segments.last()?;
                if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                    for arg in &args.args {
                        match arg {
                            syn::GenericArgument::Type(inner) => {
                                if let Some(found) = find_future_output(inner) {
                                    return Some(found);
                                }
                            }
                            // `Output = X` on a `dyn Future` bound.
                            syn::GenericArgument::AssocType(assoc) if assoc.ident == "Output" => {
                                let bound = &assoc.ty;
                                return Some(quote!(#bound));
                            }
                            _ => {}
                        }
                    }
                }
                None
            }
            // `dyn Future<Output = X> + ...` / `impl Future<Output = X>`.
            syn::Type::TraitObject(obj) => {
                for bound in &obj.bounds {
                    if let Some(found) = future_output_from_bound(bound) {
                        return Some(found);
                    }
                }
                None
            }
            syn::Type::ImplTrait(it) => {
                for bound in &it.bounds {
                    if let Some(found) = future_output_from_bound(bound) {
                        return Some(found);
                    }
                }
                None
            }
            _ => None,
        }
    }

    fn future_output_from_bound(bound: &syn::TypeParamBound) -> Option<TokenStream> {
        if let syn::TypeParamBound::Trait(trait_bound) = bound {
            let segment = trait_bound.path.segments.last()?;
            if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                for arg in &args.args {
                    if let syn::GenericArgument::AssocType(assoc) = arg {
                        if assoc.ident == "Output" {
                            let bound_ty = &assoc.ty;
                            return Some(quote!(#bound_ty));
                        }
                    }
                }
            }
        }
        None
    }

    let ty = match output {
        syn::ReturnType::Type(_, ty) => ty.as_ref(),
        syn::ReturnType::Default => return None,
    };
    find_future_output(ty)
}

#[derive(Default)]
pub struct InstrumentArgs {
    pub boundary: Option<LitStr>,
    pub component: Option<LitStr>,
    pub operation: Option<LitStr>,
    pub args: Option<Expr>,
    pub result: Option<Expr>,
    pub correlation: Option<Expr>,
    pub future: Option<FutureMode>,
    pub skip_all: bool,
    pub skip: Vec<Ident>,
    pub fields: Vec<FieldArg>,
    pub ret: bool,
    pub err: bool,
    /// Opt into replay substitution: emit a lookup-table replay branch (return
    /// the recorded value without executing) and record results LOSSLESSLY.
    /// Requires the function's return type to be `Serialize + DeserializeOwned`.
    pub replay: bool,
    /// Like `replay`, but for `Result`-returning boundaries whose error type is
    /// NOT serde (e.g. `error_stack::Report`). Records/replays ONLY the `Ok`
    /// arm: the macro extracts the Ok type `R` (first generic arg of the return
    /// type), records via `result_serialize_ok`, and on replay deserializes the
    /// recorded value into `R` and returns `Ok(R)`. Requires `R: Serialize +
    /// DeserializeOwned`. A recorded `Err` falls through to live execution.
    pub replay_ok: bool,
    /// Custom replay reconstruction (e.g. rebuilding a `reqwest::Response` from
    /// recorded parts). The expr has `__deja_recorded: serde_json::Value` in
    /// scope and must evaluate to `Option<ReturnType>`; on `Some(v)` the
    /// boundary returns `v` without executing. Pair with an explicit `result =`
    /// expr for the lossless recording shape.
    pub replay_with: Option<Expr>,
    /// DECLARATIVE BOUNDARY MODEL (additive). The declared intrinsic semantics
    /// of this boundary, in CONSTANT form (`channel = State`, `effect = Read`,
    /// `strategy = SeedAndExecute`). Each is the bare enum-variant identifier; the
    /// macro maps it to a `deja::__private::{Channel,Effect,Strategy}` value.
    /// `None` means UNDECLARED — the runtime falls back to the string heuristics
    /// so the boundary behaves byte-identically. The closure form of `effect` is
    /// OUT OF SCOPE for this slice (deferred).
    pub channel: Option<Ident>,
    pub effect: Option<Ident>,
    pub strategy: Option<Ident>,
}

#[derive(Clone, Copy)]
pub enum FutureMode {
    Boxed,
}

pub struct FieldArg {
    name: LitStr,
    expr: Expr,
}

impl Parse for InstrumentArgs {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let mut args = Self::default();

        while !input.is_empty() {
            let key: Ident = input.parse()?;
            let key_string = key.to_string();

            match key_string.as_str() {
                "skip" => {
                    let content;
                    parenthesized!(content in input);
                    while !content.is_empty() {
                        args.skip.push(content.parse()?);
                        if content.peek(Token![,]) {
                            content.parse::<Token![,]>()?;
                        }
                    }
                }
                "fields" => {
                    let content;
                    parenthesized!(content in input);
                    while !content.is_empty() {
                        let field_name: Ident = content.parse()?;
                        content.parse::<Token![=]>()?;
                        let expr: Expr = content.parse()?;
                        args.fields.push(FieldArg {
                            name: LitStr::new(&field_name.to_string(), field_name.span()),
                            expr,
                        });
                        if content.peek(Token![,]) {
                            content.parse::<Token![,]>()?;
                        }
                    }
                }
                "skip_all" => args.skip_all = true,
                "ret" => args.ret = true,
                "err" => args.err = true,
                "replay" => args.replay = true,
                "replay_ok" => args.replay_ok = true,
                _ => {
                    input.parse::<Token![=]>()?;
                    match key_string.as_str() {
                        "boundary" => args.boundary = Some(input.parse()?),
                        "component" | "trait_name" => args.component = Some(input.parse()?),
                        "operation" | "method_name" => args.operation = Some(input.parse()?),
                        "args" => args.args = Some(input.parse()?),
                        "result" => args.result = Some(input.parse()?),
                        "replay_with" => args.replay_with = Some(input.parse()?),
                        // Declarative boundary model — CONSTANT form. Each value
                        // is the bare enum-variant identifier (e.g. `Read`). The
                        // closure form of `effect` is deferred (out of scope for
                        // this slice).
                        "channel" => args.channel = Some(input.parse()?),
                        "effect" => args.effect = Some(input.parse()?),
                        "strategy" => args.strategy = Some(input.parse()?),
                        "correlation" | "correlation_id" => args.correlation = Some(input.parse()?),
                        "future" => {
                            let value: LitStr = input.parse()?;
                            match value.value().as_str() {
                                "boxed" => args.future = Some(FutureMode::Boxed),
                                _ => {
                                    return Err(syn::Error::new(
                                        value.span(),
                                        "unsupported future mode; expected `future = \"boxed\"`",
                                    ));
                                }
                            }
                        }
                        _ => {
                            return Err(syn::Error::new(
                                key.span(),
                                "unsupported deja instrument argument",
                            ));
                        }
                    }
                }
            }

            if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
            }
        }

        Ok(args)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ret(ts: proc_macro2::TokenStream) -> syn::ReturnType {
        syn::parse2(quote!(-> #ts)).expect("return type")
    }

    /// The reconstruct-target extraction for a `future = "boxed"` body must peel
    /// `Pin<Box<dyn Future<Output = X> + ...>>` down to `X` — the type
    /// `dispatch_async` resolves to and the reconstruct closure deserializes into.
    /// Getting this wrong is what made the boxed shape emit
    /// `Option<Pin<Box<..>>>` and fail to type-check.
    #[test]
    fn boxed_future_output_is_the_inner_type() {
        let output = ret(quote!(
            ::core::pin::Pin<Box<dyn ::core::future::Future<Output = Result<u64, String>> + Send>>
        ));
        let inner = boxed_future_output_ty(&output).expect("inner output type");
        assert_eq!(inner.to_string(), quote!(Result<u64, String>).to_string());
    }

    /// A non-future return type has no boxed-future output to extract.
    #[test]
    fn non_future_return_has_no_boxed_output() {
        assert!(boxed_future_output_ty(&ret(quote!(u64))).is_none());
    }

    /// `replay_ok` reaches the right `Result` Ok type even through the boxed shape:
    /// `Pin<Box<dyn Future<Output = CustomResult<R, E>>>>` → `R`.
    #[test]
    fn replay_ok_ok_type_through_boxed_future() {
        let output = ret(quote!(
            ::core::pin::Pin<Box<dyn ::core::future::Future<Output = CustomResult<MyRow, MyErr>>>>
        ));
        let ok = first_generic_arg_of_output(&output, Some(FutureMode::Boxed))
            .expect("ok type via boxed");
        assert_eq!(ok.to_string(), quote!(MyRow).to_string());
    }

    /// For a plain (non-boxed) `Result`-like return, the Ok type is its first
    /// generic argument.
    #[test]
    fn replay_ok_ok_type_for_plain_return() {
        let output = ret(quote!(CustomResult<MyRow, MyErr>));
        let ok = first_generic_arg_of_output(&output, None).expect("ok type");
        assert_eq!(ok.to_string(), quote!(MyRow).to_string());
    }

    /// The macro expands (does not error) for sync, async, and boxed shapes, and
    /// the emitted tokens route through the single `dispatch` / `dispatch_async`
    /// seam — naming NONE of the removed replay-only operations.
    #[test]
    fn generated_shapes_call_the_single_seam_and_name_no_replay_ops() {
        let cases: [proc_macro2::TokenStream; 3] = [
            quote!(fn s(x: u64) -> u64 { x + 1 }),
            quote!(async fn a(x: u64) -> u64 { x + 1 }),
            quote!(
                fn b(x: u64) -> ::core::pin::Pin<Box<dyn ::core::future::Future<Output = u64>>> {
                    Box::pin(async move { x + 1 })
                }
            ),
        ];
        let futures = [None, None, Some(FutureMode::Boxed)];

        for (src, future) in cases.into_iter().zip(futures) {
            let func: ItemFn = syn::parse2(src).expect("parse fn");
            let args = InstrumentArgs {
                future,
                ..InstrumentArgs::default()
            };
            let expanded = generate(args, func).to_string();

            // Routes through the single seam.
            assert!(
                expanded.contains("dispatch"),
                "expansion must call the dispatch seam: {expanded}"
            );
            // Names ZERO replay-only operations (the decoupling test, design §1.2).
            for banned in [
                "replay_boundary",
                "boundary_execute_mode",
                "execute_shadow_peek_boundary",
                "execute_shadow_observe_boundary",
            ] {
                assert!(
                    !expanded.contains(banned),
                    "macro must NOT name the replay-only op `{banned}`: {expanded}"
                );
            }
        }
    }

    // -----------------------------------------------------------------------
    // Declarative boundary model — macro params, presets, RMW compile error.
    // -----------------------------------------------------------------------

    fn parse_args(attr: proc_macro2::TokenStream) -> InstrumentArgs {
        syn::parse2(attr).expect("parse InstrumentArgs")
    }

    fn parse_fn(src: proc_macro2::TokenStream) -> ItemFn {
        syn::parse2(src).expect("parse fn")
    }

    /// The new declarative params parse and expand into a `with_semantics`
    /// descriptor carrying the declared `Channel`/`Effect`.
    #[test]
    fn declarative_params_expand_into_with_semantics() {
        let args = parse_args(quote!(
            boundary = "redis",
            effect = Read,
            channel = State
        ));
        let func = parse_fn(quote!(fn get(key: String) -> u64 { 0 }));
        let expanded = generate(args, func).to_string();
        assert!(
            expanded.contains("with_semantics"),
            "declared boundary must emit with_semantics: {expanded}"
        );
        assert!(expanded.contains("Channel :: State"), "{expanded}");
        assert!(expanded.contains("Effect :: Read"), "{expanded}");
        // `determinism` is gone from the model — never emitted.
        assert!(!expanded.contains("Determinism"), "{expanded}");
    }

    /// A bare `channel = Entropy` (no source) defaults the source to
    /// `EntropySource::Other("unspecified")` (the presets supply the real source).
    #[test]
    fn bare_entropy_channel_defaults_unspecified_source() {
        let args = parse_args(quote!(channel = Entropy));
        let func = parse_fn(quote!(fn draw() -> u64 { 0 }));
        let expanded = generate(args, func).to_string();
        assert!(expanded.contains("Channel :: Entropy"), "{expanded}");
        assert!(
            expanded.contains("EntropySource :: Other") && expanded.contains("unspecified"),
            "bare Entropy must default to Other(\"unspecified\"): {expanded}"
        );
    }

    /// An UNDECLARED boundary keeps emitting the legacy `BoundarySpec::new`
    /// constructor — byte-identical to before this slice (additive guarantee).
    #[test]
    fn undeclared_boundary_emits_plain_new() {
        let func = parse_fn(quote!(fn get(key: String) -> u64 { 0 }));
        let expanded = generate(InstrumentArgs::default(), func).to_string();
        assert!(
            expanded.contains("BoundarySpec :: new"),
            "undeclared boundary must emit BoundarySpec::new: {expanded}"
        );
        assert!(
            !expanded.contains("with_semantics"),
            "undeclared boundary must NOT emit with_semantics: {expanded}"
        );
    }

    /// The deprecated `replay`/`replay_ok`/`replay_with` aliases still parse and
    /// drive the lossless reconstruct path (vendor still uses them).
    #[test]
    fn deprecated_replay_aliases_still_parse() {
        let args = parse_args(quote!(boundary = "redis", replay_ok));
        let func = parse_fn(quote!(fn get(k: String) -> CustomResult<u64, E> { Ok(0) }));
        let expanded = generate(args, func).to_string();
        // `replay_ok` deserializes into the Ok type via from_value.
        assert!(expanded.contains("from_value"), "{expanded}");

        let args = parse_args(quote!(boundary = "redis", replay));
        let func = parse_fn(quote!(fn get(k: String) -> u64 { 0 }));
        assert!(generate(args, func).to_string().contains("from_value"));
    }

    /// PRESETS: `deja::id` ⇒ `Channel::Entropy(EntropySource::Id)`, no effect;
    /// `deja::time` ⇒ `Channel::Entropy(EntropySource::Clock)`, no effect;
    /// `deja::http(outgoing)` ⇒ `Channel::Egress`, no effect; `deja::http(incoming)`
    /// ⇒ NO declared channel (replay driver, outside the taxonomy).
    #[test]
    fn presets_inject_declared_defaults() {
        // id preset.
        let func = parse_fn(quote!(fn nonce() -> u64 { 0 }));
        let id = generate_with_preset(InstrumentArgs::default(), func, Some("id"), Preset::Id)
            .to_string();
        assert!(id.contains("Channel :: Entropy"), "id channel: {id}");
        assert!(
            id.contains("EntropySource :: Id"),
            "id entropy source: {id}"
        );
        // Effect is State-only; the id preset declares none.
        assert!(!id.contains("Effect ::"), "id must declare no effect: {id}");

        // time preset.
        let func = parse_fn(quote!(fn now() -> u64 { 0 }));
        let time =
            generate_with_preset(InstrumentArgs::default(), func, Some("time"), Preset::Time)
                .to_string();
        assert!(time.contains("Channel :: Entropy"), "time channel: {time}");
        assert!(
            time.contains("EntropySource :: Clock"),
            "time entropy source: {time}"
        );
        assert!(!time.contains("Effect ::"), "time must declare no effect: {time}");

        // http outgoing preset: Egress, no effect.
        let func = parse_fn(quote!(fn send() -> u64 { 0 }));
        let http = generate_with_preset(
            InstrumentArgs::default(),
            func,
            Some("http_outgoing"),
            Preset::HttpOutgoing,
        )
        .to_string();
        assert!(http.contains("Channel :: Egress"), "http channel: {http}");
        assert!(!http.contains("Effect ::"), "http_outgoing declares no effect: {http}");

        // http incoming preset: declares NOTHING (the replay driver), so it emits
        // the plain `BoundarySpec::new` constructor — but STILL records its event
        // (the boundary tuple is unchanged).
        let func = parse_fn(quote!(fn recv() -> u64 { 0 }));
        let http_in = generate_with_preset(
            InstrumentArgs::default(),
            func,
            Some("http_incoming"),
            Preset::HttpIncoming,
        )
        .to_string();
        assert!(
            http_in.contains("BoundarySpec :: new"),
            "http_incoming declares nothing → plain new: {http_in}"
        );
        assert!(
            !http_in.contains("with_semantics"),
            "http_incoming must NOT declare a channel: {http_in}"
        );
        assert!(http_in.contains("http_incoming"), "still records the event: {http_in}");
    }

    /// An EXPLICIT arg overrides a preset default (e.g. declaring `channel = State`
    /// on an http boundary overrides the preset `Egress`).
    #[test]
    fn explicit_arg_overrides_preset() {
        let args = parse_args(quote!(channel = State));
        let func = parse_fn(quote!(fn send() -> u64 { 0 }));
        let out = generate_with_preset(args, func, Some("http_outgoing"), Preset::HttpOutgoing)
            .to_string();
        assert!(out.contains("Channel :: State"), "{out}");
        assert!(!out.contains("Channel :: Egress"), "preset Egress must be overridden: {out}");
    }

    /// LOCKED RULE: an `effect = ReadModifyWrite` WITHOUT `strategy = ...` emits a
    /// `compile_error!` (no default). With a strategy it expands fine.
    #[test]
    fn rmw_without_strategy_is_a_compile_error() {
        let args = parse_args(quote!(boundary = "redis", effect = ReadModifyWrite));
        let func = parse_fn(quote!(fn incr(k: String) -> u64 { 0 }));
        let out = generate(args, func).to_string();
        assert!(
            out.contains("compile_error"),
            "RMW without strategy must compile_error: {out}"
        );
        assert!(
            out.contains("must declare strategy"),
            "compile error must mention the strategy requirement: {out}"
        );

        // With a strategy → no compile error, emits the declared descriptor.
        let args = parse_args(quote!(
            boundary = "redis",
            effect = ReadModifyWrite,
            strategy = SeedAndExecute
        ));
        let func = parse_fn(quote!(fn incr(k: String) -> u64 { 0 }));
        let out = generate(args, func).to_string();
        assert!(!out.contains("compile_error"), "RMW with strategy must compile: {out}");
        assert!(out.contains("Effect :: ReadModifyWrite"), "{out}");
        assert!(out.contains("Strategy :: SeedAndExecute"), "{out}");
    }

    /// An unknown variant identifier for a declared field is a `compile_error!`.
    #[test]
    fn unknown_variant_is_a_compile_error() {
        let args = parse_args(quote!(boundary = "redis", effect = Frobnicate));
        let func = parse_fn(quote!(fn x(k: String) -> u64 { 0 }));
        let out = generate(args, func).to_string();
        assert!(out.contains("compile_error"), "unknown effect must error: {out}");
        assert!(out.contains("unknown effect"), "{out}");
    }
}
