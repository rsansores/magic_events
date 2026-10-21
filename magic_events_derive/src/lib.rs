//! `#[derive(Event)]` for [magic_events]: one type is one event, and says its
//! type, its version and its subject. Use it through `magic_events`, which
//! re-exports it; every attribute is documented there, on
//! [`magic_events::Event`].
//!
//! Everything the derive is told is checked where it is written: a subject
//! token no subject may hold, a `{field}` that is no field, a field that is
//! not `Display`, a type two variants share — each is a compile error
//! pointing at what says it.
//!
//! [magic_events]: https://docs.rs/magic_events
//! [`magic_events::Event`]: https://docs.rs/magic_events/latest/magic_events/derive.Event.html

#![warn(missing_docs)]

use proc_macro::TokenStream as Tokens;
use proc_macro2::{Span, TokenStream};
use quote::{format_ident, quote, quote_spanned, ToTokens};
use std::collections::BTreeMap;
use syn::meta::ParseNestedMeta;
use syn::spanned::Spanned;
use syn::{parse_macro_input, Attribute, Data, DeriveInput, Fields, Ident, LitInt, LitStr};

/// Implements `magic_events::Event` for a struct that is one event, or an
/// enum whose every variant is one. Use it as `magic_events::Event`: the
/// attributes it reads (`#[event(…)]`) are documented there.
#[proc_macro_derive(Event, attributes(event))]
pub fn derive_event(input: Tokens) -> Tokens {
    let input = parse_macro_input!(input as DeriveInput);
    event(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// The keys `#[event(…)]` takes on a struct, or on a variant.
const EVENT_KEYS: &str = "`type`, `version`, `subject`, `ce_subject` or `crate`";

/// What one `#[event(…)]` says.
#[derive(Default)]
struct Args {
    ty: Option<LitStr>,
    version: Option<LitInt>,
    subject: Option<LitStr>,
    ce_subject: Option<LitStr>,
    krate: Option<LitStr>,
}

impl Args {
    /// The `#[event(…)]` keys among `attrs`. `only_crate`: an enum's own,
    /// whose variants say the rest.
    fn parse(attrs: &[Attribute], only_crate: bool) -> syn::Result<Args> {
        let mut args = Args::default();
        for attr in attrs.iter().filter(|a| a.path().is_ident("event")) {
            attr.parse_nested_meta(|meta| {
                let key = meta.path.get_ident().map(Ident::to_string);
                match key.as_deref() {
                    Some("crate") => once(&mut args.krate, &meta),
                    Some(_) if only_crate => Err(meta.error(
                        "on an enum, `#[event(…)]` takes only `crate`: each variant says its \
                         own `type`, `version` and `subject`",
                    )),
                    Some("type") => once(&mut args.ty, &meta),
                    Some("version") => once(&mut args.version, &meta),
                    Some("subject") => once(&mut args.subject, &meta),
                    Some("ce_subject") => once(&mut args.ce_subject, &meta),
                    _ => Err(meta.error(format!("expected {EVENT_KEYS}"))),
                }
            })?;
        }
        Ok(args)
    }
}

/// Sets `slot` to the literal `meta` carries, refusing it a second time.
fn once<T: syn::parse::Parse>(slot: &mut Option<T>, meta: &ParseNestedMeta) -> syn::Result<()> {
    let name = meta.path.to_token_stream().to_string();
    if slot.is_some() {
        return Err(meta.error(format!("`{name}` is given twice")));
    }
    let value = meta
        .value()
        .map_err(|_| meta.error(format!("`{name}` takes a value: `{name} = …`")))?;
    *slot = Some(value.parse()?);
    Ok(())
}

/// One part of a subject token, or of a `ce_subject`: text as written, or a
/// field's value.
enum Part {
    Text(String),
    Field(String),
}

/// `template`'s parts: text, and `{field}`s.
fn parts(template: &LitStr) -> syn::Result<Vec<Part>> {
    let text = template.value();
    let mut parts = Vec::new();
    let mut rest = text.as_str();
    while !rest.is_empty() {
        let open = rest.find('{');
        let close = rest.find('}');
        match (open, close) {
            (None, None) => {
                parts.push(Part::Text(rest.to_owned()));
                break;
            }
            (Some(o), Some(c)) if o < c => {
                if o > 0 {
                    parts.push(Part::Text(rest[..o].to_owned()));
                }
                let name = &rest[o + 1..c];
                if syn::parse_str::<Ident>(name).is_err() {
                    return Err(syn::Error::new(
                        template.span(),
                        format!("`{{{name}}}` names no field: a `{{…}}` holds a field's name"),
                    ));
                }
                parts.push(Part::Field(name.to_owned()));
                rest = &rest[c + 1..];
            }
            _ => {
                return Err(syn::Error::new(
                    template.span(),
                    format!(
                        "`{text}` has a brace that opens or closes no `{{field}}`: a subject \
                         holds text and `{{field}}`s"
                    ),
                ))
            }
        }
    }
    Ok(parts)
}

/// Whether `c` may stand in a subject token: printable ASCII, and none of
/// the separator `.` or the wildcards `*` and `>`.
fn token_char(c: char) -> bool {
    matches!(c, '!'..='~') && !matches!(c, '.' | '*' | '>')
}

/// The subject `template`, token by token, each checked: a token is not
/// empty, and its text holds only what a subject token may.
fn subject(template: &LitStr) -> syn::Result<Vec<Vec<Part>>> {
    let text = template.value();
    if text.is_empty() {
        return Err(syn::Error::new(
            template.span(),
            "an empty subject: an event says where it is published, as `orders.{store}.shipped`",
        ));
    }
    let mut tokens = Vec::new();
    for token in text.split('.') {
        if token.is_empty() {
            return Err(syn::Error::new(
                template.span(),
                format!("`{text}` has an empty token: tokens are joined by single dots"),
            ));
        }
        let lit = LitStr::new(token, template.span());
        let token_parts = parts(&lit)?;
        for part in &token_parts {
            if let Part::Text(t) = part {
                if let Some(bad) = t.chars().find(|c| !token_char(*c)) {
                    return Err(syn::Error::new(
                        template.span(),
                        format!(
                            "`{text}` holds {bad:?}: a subject token is printable ASCII, with \
                             no whitespace and no `*` or `>`"
                        ),
                    ));
                }
            }
        }
        tokens.push(token_parts);
    }
    Ok(tokens)
}

/// The longest event type, in bytes: what `emit` takes.
const LONGEST_TYPE: usize = 255;

/// The type, checked: printable ASCII with no whitespace, 255 bytes at most.
fn event_type(ty: &LitStr) -> syn::Result<String> {
    let text = ty.value();
    if text.is_empty() || text.len() > LONGEST_TYPE || !text.chars().all(|c| matches!(c, '!'..='~'))
    {
        return Err(syn::Error::new(
            ty.span(),
            format!(
                "`{text}` is no event type: one is printable ASCII, with no whitespace, 255 \
                 bytes at most, as `order.shipped`"
            ),
        ));
    }
    Ok(text)
}

/// The serde attributes of a type that make its JSON something other than
/// its fields: refused on an event, whose body is an object of its fields.
const REFUSED_SHAPES: [&str; 5] = ["transparent", "into", "from", "try_from", "remote"];

/// The version, checked: a whole number from 1.
fn version(version: &LitInt) -> syn::Result<u32> {
    match version.base10_parse::<u32>() {
        Ok(v) if v >= 1 => Ok(v),
        _ => Err(syn::Error::new(
            version.span(),
            "a version is a whole number from 1",
        )),
    }
}

/// How the fields of one event are reached: by name off `self`, or off the
/// one value a newtype variant wraps.
enum Reach {
    /// Each field bound to a variable of its name.
    Named,
    /// A newtype variant's value, bound to `__inner`: its fields are its own.
    Inner,
}

/// One event of the type: what the derive renders for it.
struct One {
    ty: String,
    version: u32,
    subject: Vec<Vec<Part>>,
    subject_span: Span,
    ce_subject: Option<(Vec<Part>, Span)>,
}

impl One {
    fn of(args: Args, at: Span, what: &str) -> syn::Result<One> {
        let missing = |key: &str| {
            syn::Error::new(
                at,
                format!("{what} needs `#[event({key} = …)]`: every event says its `type`, `version` and `subject`"),
            )
        };
        let ty = args.ty.ok_or_else(|| missing("type"))?;
        let ver = args.version.ok_or_else(|| missing("version"))?;
        let subj = args.subject.ok_or_else(|| missing("subject"))?;
        let ce_subject = match &args.ce_subject {
            Some(lit) => Some((parts(lit)?, lit.span())),
            None => None,
        };
        Ok(One {
            ty: event_type(&ty)?,
            version: version(&ver)?,
            subject: subject(&subj)?,
            subject_span: subj.span(),
            ce_subject,
        })
    }

    /// Every field the event's subject and `ce_subject` name, checked
    /// against `fields`: each is one of them.
    fn named(&self) -> Vec<(&str, Span)> {
        let mut named = Vec::new();
        for token in &self.subject {
            for part in token {
                if let Part::Field(f) = part {
                    named.push((f.as_str(), self.subject_span));
                }
            }
        }
        if let Some((parts, span)) = &self.ce_subject {
            for part in parts {
                if let Part::Field(f) = part {
                    named.push((f.as_str(), *span));
                }
            }
        }
        named
    }

    /// The `Described` of this event, its fields reached as `fields` says.
    fn describe(&self, krate: &TokenStream, fields: &Reach, variant: Option<u32>) -> TokenStream {
        let render = |parts: &[Part], span: Span| {
            let pushes = parts.iter().map(|part| match part {
                Part::Text(t) => quote! { __s.push_str(#t); },
                Part::Field(f) => {
                    let value = match fields {
                        Reach::Named => {
                            let f = Ident::new(f, span);
                            quote_spanned! {span=> #f }
                        }
                        Reach::Inner => {
                            let f = Ident::new(f, span);
                            quote_spanned! {span=> &__inner.#f }
                        }
                    };
                    quote_spanned! {span=> #krate::__private::push(&mut __s, #value); }
                }
            });
            quote! {{
                let mut __s = ::std::string::String::new();
                #(#pushes)*
                __s
            }}
        };
        let tokens = self
            .subject
            .iter()
            .map(|token| render(token, self.subject_span));
        let ce_subject = match &self.ce_subject {
            Some((parts, span)) => {
                let rendered = render(parts, *span);
                quote! { ::std::option::Option::Some(#rendered) }
            }
            None => quote! { ::std::option::Option::None },
        };
        let ty = &self.ty;
        let version = self.version;
        let variant = match variant {
            Some(i) => quote! { ::std::option::Option::Some(#i) },
            None => quote! { ::std::option::Option::None },
        };
        quote! {
            #krate::__private::Described {
                r#type: #ty,
                version: #version,
                variant: #variant,
                subject: ::std::vec![#(#tokens),*],
                ce_subject: #ce_subject,
            }
        }
    }
}

/// Refuses a `{field}` that is not one of `fields`.
fn check_named(one: &One, fields: &[Ident], of: &str) -> syn::Result<()> {
    for (name, span) in one.named() {
        if !fields.iter().any(|f| f == name) {
            return Err(syn::Error::new(
                span,
                format!("`{{{name}}}` names no field of `{of}`"),
            ));
        }
    }
    Ok(())
}

/// The serde attributes of `attrs` the derive cannot honour, by name: the
/// first one found among `refused`.
fn serde_refused(attrs: &[Attribute], refused: &[&str]) -> Option<(String, Span)> {
    let mut found = None;
    for attr in attrs.iter().filter(|a| a.path().is_ident("serde")) {
        let _ = attr.parse_nested_meta(|meta| {
            if let Some(name) = meta.path.get_ident().map(Ident::to_string) {
                if found.is_none() && refused.contains(&name.as_str()) {
                    found = Some((name, meta.path.span()));
                }
            }
            // A value, if any, is not read.
            if meta.input.peek(syn::Token![=]) {
                let _: syn::Expr = meta.value()?.parse()?;
            } else if meta.input.peek(syn::token::Paren) {
                let _ = meta.parse_nested_meta(|inner| {
                    if inner.input.peek(syn::Token![=]) {
                        let _: syn::Expr = inner.value()?.parse()?;
                    }
                    Ok(())
                });
            }
            Ok(())
        });
    }
    found
}

fn event(input: &DeriveInput) -> syn::Result<TokenStream> {
    let name = &input.ident;
    let of = name.to_string();
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    if let Some((attr, span)) = serde_refused(&input.attrs, &REFUSED_SHAPES) {
        return Err(syn::Error::new(
            span,
            format!(
                "`#[serde({attr})]`: an event's body is a JSON object of its own fields, as \
                 serde writes them by default"
            ),
        ));
    }
    let (types, body, krate) = match &input.data {
        Data::Struct(data) => {
            let args = Args::parse(&input.attrs, false)?;
            let krate = krate(args.krate.as_ref())?;
            let Fields::Named(named) = &data.fields else {
                return Err(syn::Error::new(
                    name.span(),
                    "an event is a struct with named fields, or an enum of such events: its \
                     body is a JSON object",
                ));
            };
            let one = One::of(args, name.span(), &format!("`{of}`"))?;
            let fields: Vec<Ident> = named.named.iter().filter_map(|f| f.ident.clone()).collect();
            check_named(&one, &fields, &of)?;
            let used = used(&one, &fields);
            let describe = one.describe(&krate, &Reach::Named, None);
            let (ty, version) = (&one.ty, one.version);
            (
                quote! { &[(#ty, #version)] },
                quote! {
                    let Self { #(#used,)* .. } = self;
                    #describe
                },
                krate,
            )
        }
        Data::Enum(data) => {
            let args = Args::parse(&input.attrs, true)?;
            let krate = krate(args.krate.as_ref())?;
            if let Some((attr, span)) = serde_refused(&input.attrs, &["tag", "content", "untagged"])
            {
                return Err(syn::Error::new(
                    span,
                    format!(
                        "`#[serde({attr})]`: an event enum is serialized as serde does by \
                         default (externally tagged), and its body is the variant's own fields"
                    ),
                ));
            }
            if data.variants.is_empty() {
                return Err(syn::Error::new(
                    name.span(),
                    "an enum of no variant is no event",
                ));
            }
            let mut types = Vec::new();
            let mut arms = Vec::new();
            let mut seen: BTreeMap<(String, u32), Ident> = BTreeMap::new();
            for (index, variant) in data.variants.iter().enumerate() {
                let vname = &variant.ident;
                if let Some((attr, span)) = serde_refused(
                    &variant.attrs,
                    &[
                        "skip",
                        "skip_serializing",
                        "skip_deserializing",
                        "untagged",
                        "other",
                    ],
                ) {
                    return Err(syn::Error::new(
                        span,
                        format!(
                            "`#[serde({attr})]` on `{vname}`: every variant of an event enum is \
                             an event, serialized and read back as written"
                        ),
                    ));
                }
                let args = Args::parse(&variant.attrs, false)?;
                if let Some(k) = &args.krate {
                    return Err(syn::Error::new(
                        k.span(),
                        "`crate` is said once, on the enum",
                    ));
                }
                let one = One::of(args, vname.span(), &format!("`{of}::{vname}`"))?;
                let key = (one.ty.clone(), one.version);
                if let Some(first) = seen.get(&key) {
                    return Err(syn::Error::new(
                        vname.span(),
                        format!(
                            "`{vname}` is type `{}` version {}, as `{first}` is: each variant \
                             is an event of its own",
                            one.ty, one.version
                        ),
                    ));
                }
                seen.insert(key, vname.clone());
                let index = u32::try_from(index).expect("fewer variants than u32::MAX");
                let arm = match &variant.fields {
                    Fields::Named(named) => {
                        for field in &named.named {
                            if let Some((attr, span)) = serde_refused(&field.attrs, &["flatten"]) {
                                return Err(syn::Error::new(
                                    span,
                                    format!(
                                        "`#[serde({attr})]` in a variant: its body is its own \
                                         fields, one JSON object, which the derive writes field \
                                         by field"
                                    ),
                                ));
                            }
                        }
                        let fields: Vec<Ident> =
                            named.named.iter().filter_map(|f| f.ident.clone()).collect();
                        check_named(&one, &fields, &format!("{of}::{vname}"))?;
                        let used = used(&one, &fields);
                        let describe = one.describe(&krate, &Reach::Named, Some(index));
                        quote! { Self::#vname { #(#used,)* .. } => #describe, }
                    }
                    Fields::Unnamed(unnamed) if unnamed.unnamed.len() == 1 => {
                        let describe = one.describe(&krate, &Reach::Inner, Some(index));
                        if one.named().is_empty() {
                            quote! { Self::#vname(_) => #describe, }
                        } else {
                            quote! { Self::#vname(__inner) => #describe, }
                        }
                    }
                    _ => {
                        return Err(syn::Error::new(
                            vname.span(),
                            format!(
                                "`{vname}` carries no fields of its own: a variant of an event \
                                 enum has named fields, or wraps one struct, as its body"
                            ),
                        ))
                    }
                };
                arms.push(arm);
                let (ty, version) = (&one.ty, one.version);
                types.push(quote! { (#ty, #version) });
            }
            (
                quote! { &[#(#types),*] },
                quote! {
                    match self {
                        #(#arms)*
                    }
                },
                krate,
            )
        }
        Data::Union(_) => {
            return Err(syn::Error::new(
                name.span(),
                "a union is no event: derive `Event` on a struct with named fields, or an enum",
            ))
        }
    };
    let is_enum = matches!(input.data, Data::Enum(_));
    Ok(quote! {
        impl #impl_generics #krate::__private::Sealed for #name #ty_generics #where_clause {}

        impl #impl_generics #krate::Event for #name #ty_generics #where_clause {
            #[doc(hidden)]
            const __TYPES: &'static [(&'static str, u32)] = #types;

            #[doc(hidden)]
            const __ENUM: bool = #is_enum;

            #[doc(hidden)]
            fn __describe(&self) -> #krate::__private::Described {
                #body
            }
        }
    })
}

/// The fields of `fields` the event's templates name, each once, as bound.
fn used(one: &One, fields: &[Ident]) -> Vec<Ident> {
    let named = one.named();
    fields
        .iter()
        .filter(|f| named.iter().any(|(n, _)| f == n))
        .map(|f| format_ident!("{}", f))
        .collect()
}

/// The path to the crate: `crate = "…"`, or `::magic_events`.
fn krate(given: Option<&LitStr>) -> syn::Result<TokenStream> {
    match given {
        Some(path) => Ok(path.parse::<syn::Path>()?.into_token_stream()),
        None => Ok(quote! { ::magic_events }),
    }
}
