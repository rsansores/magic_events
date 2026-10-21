//! The stream the relay publishes to: created and kept to the settings, or,
//! when the operator manages it, required to take every subject under the
//! prefix.

use crate::settings::{Settings, Storage};
use crate::Error;
use async_nats::jetstream::context::{CreateStreamErrorKind, GetStreamErrorKind};
use async_nats::jetstream::stream::{Config, StorageType};
use async_nats::jetstream::{Context, ErrorCode};

/// The subjects the relay publishes on: every one under the prefix.
pub(crate) fn subjects(settings: &Settings) -> String {
    format!("{}.>", settings.subject_prefix)
}

fn storage(storage: Storage) -> StorageType {
    match storage {
        Storage::File => StorageType::File,
        Storage::Memory => StorageType::Memory,
    }
}

/// Brings the stream to what `settings` say, or with `manage_stream` off,
/// refuses one that is not there or does not take the relay's subjects.
pub(crate) async fn ensure(jetstream: &Context, settings: &Settings) -> Result<(), Error> {
    let name = &settings.stream;
    let wanted = subjects(settings);
    let found = match jetstream.get_stream(name).await {
        Ok(stream) => Some(stream.cached_info().config.clone()),
        Err(e) => match e.kind() {
            GetStreamErrorKind::JetStream(api)
                if api.error_code() == ErrorCode::STREAM_NOT_FOUND =>
            {
                None
            }
            _ => {
                return Err(Error::Broker {
                    reason: format!("the stream `{name}` could not be read: {e}"),
                    transient: true,
                })
            }
        },
    };
    if !settings.manage_stream {
        let Some(config) = found else {
            return Err(Error::Misuse(format!(
                "there is no stream `{name}`, and the settings leave the stream to the operator \
                 (`manage_stream(false)`): create it, taking `{wanted}`"
            )));
        };
        if !config.subjects.iter().any(|s| covers(s, &wanted)) {
            return Err(Error::Misuse(format!(
                "the stream `{name}` takes {:?}, not every subject under `{wanted}`: the relay \
                 publishes there",
                config.subjects
            )));
        }
        return Ok(());
    }
    let mut config = found.clone().unwrap_or_else(|| Config {
        name: name.clone(),
        storage: storage(settings.storage),
        ..Config::default()
    });
    if config.storage != storage(settings.storage) {
        return Err(Error::Misuse(format!(
            "the stream `{name}` keeps its messages in {:?}, and the settings say {:?}: a \
             stream's storage does not change",
            config.storage, settings.storage
        )));
    }
    if !config.subjects.iter().any(|s| covers(s, &wanted)) {
        config.subjects.push(wanted);
    }
    config.max_age = settings.retention;
    config.duplicate_window = settings.duplicate_window;
    config.num_replicas = settings.replicas;
    let refused = |e: &dyn std::fmt::Display, api: Option<&async_nats::jetstream::Error>| {
        if api.is_some() {
            Error::Misuse(format!(
                "the broker refuses the stream `{name}` as the settings say it: {e}"
            ))
        } else {
            Error::Broker {
                reason: format!("the stream `{name}` could not be set up: {e}"),
                transient: true,
            }
        }
    };
    match found {
        None => {
            if let Err(e) = jetstream.create_stream(config).await {
                let api = match e.kind() {
                    CreateStreamErrorKind::JetStream(api) => Some(api),
                    _ => None,
                };
                return Err(refused(&e, api.as_ref()));
            }
            tracing::info!(stream = %name, "events: the stream is created");
        }
        Some(before) if same(&before, &config) => {}
        Some(_) => {
            if let Err(e) = jetstream.update_stream(&config).await {
                let api = std::error::Error::source(&e)
                    .and_then(|s| s.downcast_ref::<async_nats::jetstream::Error>());
                return Err(refused(&e, api));
            }
            tracing::info!(stream = %name, "events: the stream is brought to the settings");
        }
    }
    Ok(())
}

/// Whether two configurations agree on what the relay sets.
fn same(a: &Config, b: &Config) -> bool {
    a.subjects == b.subjects
        && a.max_age == b.max_age
        && a.duplicate_window == b.duplicate_window
        && a.num_replicas == b.num_replicas
}

/// Whether the subject filter `filter` takes every subject `subject` (a
/// filter itself) does: token by token, `*` any one token, `>` the rest.
pub(crate) fn covers(filter: &str, subject: &str) -> bool {
    let mut wanted = subject.split('.');
    for token in filter.split('.') {
        if token == ">" {
            return wanted.next().is_some();
        }
        match wanted.next() {
            Some(">") | None => return false,
            Some(w) if token == "*" || token == w => {}
            Some(_) => return false,
        }
    }
    wanted.next().is_none()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_filter_covers_what_it_takes_all_of() {
        for (filter, subject, covered) in [
            ("shop.>", "shop.>", true),
            (">", "shop.>", true),
            ("*.>", "shop.>", true),
            ("shop.*", "shop.>", false),
            ("shop.orders.>", "shop.>", false),
            ("shop", "shop.>", false),
            ("store.>", "shop.>", false),
            ("shop.events.>", "shop.events.>", true),
            ("shop.*.>", "shop.events.>", true),
            ("shop.orders", "shop.orders", true),
        ] {
            assert_eq!(covers(filter, subject), covered, "{filter} {subject}");
        }
    }
}
