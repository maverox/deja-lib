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
    mut args: InstrumentArgs,
    func: ItemFn,
    default_boundary: Option<&'static str>,
) -> TokenStream {
    if args.boundary.is_none() {
        args.boundary = default_boundary.map(lit_str);
    }

    generate_inner(args, func)
}

pub fn generate_http(attr: proc_macro::TokenStream, item: proc_macro::TokenStream) -> TokenStream {
    let (boundary, args) = parse_http_args(attr);
    match syn::parse::<ItemFn>(item) {
        Ok(func) => generate_with_boundary(args, func, Some(boundary)),
        Err(error) => error.to_compile_error(),
    }
}

fn generate_inner(args: InstrumentArgs, mut func: ItemFn) -> TokenStream {
    let sig = &func.sig;
    let vis = &func.vis;
    let block = &func.block;

    let replay = args.replay;
    let replay_ok = args.replay_ok;
    let replay_with = args.replay_with;
    let any_replay = replay || replay_ok || replay_with.is_some();
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

    if !func
        .attrs
        .iter()
        .any(|attr| attr.path().is_ident("track_caller"))
    {
        func.attrs.push(parse_quote!(#[track_caller]));
    }
    let attrs = &func.attrs;

    // How to turn the recorded JSON (`__deja_recorded`) back into the return
    // value on a replay hit. Three modes:
    //  - replay_with: a user expr yielding `Option<ReturnType>` (e.g. rebuild a
    //    reqwest::Response from recorded parts).
    //  - replay_ok: Result Ok-only — deserialize the recorded value into the Ok
    //    type `R` (first generic arg) and return `Ok(R)`; never touches the
    //    (possibly non-serde) error type.
    //  - replay: direct — deserialize into the whole return type.
    // Any deserialize failure (e.g. a recorded `Err` sentinel) falls through to
    // live execution (the V1 "skip error arms" policy).
    let reconstruct: TokenStream = if let Some(expr) = &replay_with {
        quote! {
            if let ::std::option::Option::Some(__deja_replayed) = { #expr } {
                return __deja_replayed;
            }
        }
    } else if replay_ok {
        let ok_ty = match first_generic_arg(&sig.output) {
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
            if let ::std::result::Result::Ok(__deja_replayed) =
                ::serde_json::from_value::<#ok_ty>(__deja_recorded)
            {
                return ::std::result::Result::Ok(__deja_replayed);
            }
        }
    } else {
        quote! {
            if let ::std::result::Result::Ok(__deja_replayed) =
                ::serde_json::from_value::<#ret_ty>(__deja_recorded)
            {
                return __deja_replayed;
            }
        }
    };

    // Replay prelude (opt-in): try the lookup table first and, on a hit, return
    // the recorded value WITHOUT executing. `replay_boundary` returns None in
    // record / no-op mode, so this is inert there. Args are computed ONCE here
    // and moved into the recording path, so a replay miss records exactly as
    // before.
    let (replay_prelude, start_args): (TokenStream, TokenStream) = if any_replay {
        (
            quote! {
                let __deja_boundary_args: ::serde_json::Value = { #args_expr };
                if let ::std::option::Option::Some(__deja_recorded) =
                    ::deja::__private::replay_boundary(
                        ::std::panic::Location::caller(),
                        ::deja::__private::BoundarySpec::new(#boundary, #component, #operation),
                        &__deja_boundary_args,
                        ::std::option::Option::Some(&__deja_identity),
                    )
                {
                    #reconstruct
                }
            },
            quote! { move || __deja_boundary_args },
        )
    } else {
        (quote! {}, quote! { || { #args_expr } })
    };

    if sig.asyncness.is_some() {
        if args.future.is_some() {
            return syn::Error::new_spanned(
                &sig.ident,
                "`future = \"boxed\"` is only valid on non-async functions that return a boxed future",
            )
            .to_compile_error();
        }

        quote! {
            #(#attrs)*
            #vis #sig {
                let __deja_boundary_correlation_id: Option<String> = { #correlation_expr };
                #identity_build
                #replay_prelude
                let __deja_boundary_event = ::deja::__private::start_boundary_event_lazy(
                    ::std::panic::Location::caller(),
                    ::deja::__private::BoundarySpec::new(#boundary, #component, #operation),
                    __deja_boundary_correlation_id,
                    #start_args,
                    ::std::option::Option::Some(__deja_identity),
                );
                let __deja_boundary_output = async move #block.await;
                ::deja::__private::finish_boundary_event(
                    __deja_boundary_event,
                    &__deja_boundary_output,
                    move |__deja_result| { #result_expr },
                );
                __deja_boundary_output
            }
        }
    } else if matches!(args.future, Some(FutureMode::Boxed)) {
        quote! {
            #(#attrs)*
            #vis #sig {
                let __deja_boundary_correlation_id: Option<String> = { #correlation_expr };
                #identity_build
                #replay_prelude
                let __deja_boundary_event = ::deja::__private::start_boundary_event_lazy(
                    ::std::panic::Location::caller(),
                    ::deja::__private::BoundarySpec::new(#boundary, #component, #operation),
                    __deja_boundary_correlation_id,
                    #start_args,
                    ::std::option::Option::Some(__deja_identity),
                );
                let __deja_boundary_future = #block;
                ::std::boxed::Box::pin(async move {
                    let __deja_boundary_output = __deja_boundary_future.await;
                    ::deja::__private::finish_boundary_event(
                        __deja_boundary_event,
                        &__deja_boundary_output,
                        move |__deja_result| { #result_expr },
                    );
                    __deja_boundary_output
                })
            }
        }
    } else {
        quote! {
            #(#attrs)*
            #vis #sig {
                let __deja_boundary_correlation_id: Option<String> = { #correlation_expr };
                #identity_build
                #replay_prelude
                let __deja_boundary_event = ::deja::__private::start_boundary_event_lazy(
                    ::std::panic::Location::caller(),
                    ::deja::__private::BoundarySpec::new(#boundary, #component, #operation),
                    __deja_boundary_correlation_id,
                    #start_args,
                    ::std::option::Option::Some(__deja_identity),
                );
                let __deja_boundary_output = (|| #block)();
                ::deja::__private::finish_boundary_event(
                    __deja_boundary_event,
                    &__deja_boundary_output,
                    move |__deja_result| { #result_expr },
                );
                __deja_boundary_output
            }
        }
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
