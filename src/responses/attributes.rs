use std::num::NonZeroUsize;

use serde::{de::Error, Deserialize, Deserializer, Serialize};

use crate::options::{DEFAULT_CACHE_CAPACITY, DEFAULT_FETCH_SIZE};

/// Struct representing attributes related to the connection with the Exasol server.
/// These can either be returned by an explicit `getAttributes` call or as part of any response.
///
/// Note that some of these are *read-only*!
/// See the [specification](<https://github.com/exasol/websocket-api/blob/master/docs/WebsocketAPIV1.md#attributes-session-and-database-properties>)
/// for more details.
///
/// Moreover, we store in this other custom connection related attributes, specific to the driver.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExaAttributes {
    // ##########################################################
    // ############# Database read-write attributes #############
    // ##########################################################
    pub(crate) autocommit: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) current_schema: Option<String>,
    pub(crate) feedback_interval: u64,
    pub(crate) numeric_characters: String,
    pub(crate) query_timeout: u64,
    pub(crate) snapshot_transactions_enabled: bool,
    pub(crate) timestamp_utc_enabled: bool,
    // ##########################################################
    // ############# Database read-only attributes ##############
    // ##########################################################
    #[serde(skip_serializing)]
    pub(crate) compression_enabled: bool,
    #[serde(skip_serializing)]
    pub(crate) date_format: String,
    #[serde(skip_serializing)]
    pub(crate) date_language: String,
    #[serde(skip_serializing)]
    pub(crate) datetime_format: String,
    #[serde(skip_serializing)]
    pub(crate) default_like_escape_character: String,
    #[serde(skip_serializing)]
    pub(crate) open_transaction: bool,
    #[serde(skip_serializing)]
    pub(crate) timezone: String,
    #[serde(skip_serializing)]
    pub(crate) timezone_behavior: String,
    // ##########################################################
    // ############# Driver specific attributes #################
    // ##########################################################
    #[serde(skip_serializing)]
    pub(crate) fetch_size: usize,
    #[serde(skip_serializing)]
    pub(crate) encryption_enabled: bool,
    #[serde(skip_serializing)]
    pub(crate) statement_cache_capacity: NonZeroUsize,
}

/// Sensible attribute defaults that anyway get overwritten after connection login.
impl Default for ExaAttributes {
    fn default() -> Self {
        Self {
            autocommit: true,
            current_schema: None,
            feedback_interval: 1,
            numeric_characters: ".,".to_owned(),
            query_timeout: 0,
            snapshot_transactions_enabled: false,
            timestamp_utc_enabled: false,
            compression_enabled: false,
            date_format: "YYYY-MM-DD".to_owned(),
            date_language: "ENG".to_owned(),
            datetime_format: "YYYY-MM-DD HH24:MI:SS.FF6".to_owned(),
            default_like_escape_character: "\\".to_owned(),
            open_transaction: false,
            timezone: "UNIVERSAL".to_owned(),
            timezone_behavior: "INVALID SHIFT AMBIGUOUS ST".to_owned(),
            fetch_size: DEFAULT_FETCH_SIZE,
            encryption_enabled: true,
            statement_cache_capacity: NonZeroUsize::new(DEFAULT_CACHE_CAPACITY).unwrap(),
        }
    }
}

impl ExaAttributes {
    pub fn autocommit(&self) -> bool {
        self.autocommit
    }

    pub fn current_schema(&self) -> Option<&str> {
        self.current_schema.as_deref()
    }

    pub fn feedback_interval(&self) -> u64 {
        self.feedback_interval
    }

    pub fn numeric_characters(&self) -> &str {
        &self.numeric_characters
    }

    pub fn query_timeout(&self) -> u64 {
        self.query_timeout
    }

    pub fn snapshot_transactions_enabled(&self) -> bool {
        self.snapshot_transactions_enabled
    }

    pub fn timestamp_utc_enabled(&self) -> bool {
        self.timestamp_utc_enabled
    }

    pub fn compression_enabled(&self) -> bool {
        self.compression_enabled
    }

    pub fn date_format(&self) -> &str {
        &self.date_format
    }

    pub fn date_language(&self) -> &str {
        &self.date_language
    }

    pub fn datetime_format(&self) -> &str {
        &self.datetime_format
    }

    pub fn default_like_escape_character(&self) -> &str {
        &self.default_like_escape_character
    }

    pub fn open_transaction(&self) -> bool {
        self.open_transaction
    }

    pub fn timezone(&self) -> &str {
        &self.timezone
    }

    pub fn timezone_behavior(&self) -> &str {
        &self.timezone_behavior
    }

    pub fn fetch_size(&self) -> usize {
        self.fetch_size
    }

    pub fn encryption_enabled(&self) -> bool {
        self.encryption_enabled
    }

    pub fn statement_cache_capacity(&self) -> NonZeroUsize {
        self.statement_cache_capacity
    }

    pub(crate) fn update(&mut self, other: Attributes) {
        macro_rules! other_or_prev {
            ($field:tt) => {
                if let Some(new) = other.$field {
                    self.$field = new;
                }
            };
        }

        if let Some(schema) = other.current_schema {
            self.current_schema = Some(schema);
        }

        other_or_prev!(autocommit);
        other_or_prev!(feedback_interval);
        other_or_prev!(numeric_characters);
        other_or_prev!(query_timeout);
        other_or_prev!(snapshot_transactions_enabled);
        other_or_prev!(timestamp_utc_enabled);
        other_or_prev!(compression_enabled);
        other_or_prev!(date_format);
        other_or_prev!(date_language);
        other_or_prev!(datetime_format);
        other_or_prev!(default_like_escape_character);
        other_or_prev!(open_transaction);
        other_or_prev!(timezone);
        other_or_prev!(timezone_behavior);
    }
}

/// Struct representing only the attributes returned from Exasol.
#[allow(dead_code)]
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Attributes {
    // ##########################################################
    // ############# Database read-write attributes #############
    // ##########################################################
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) autocommit: Option<bool>,
    #[serde(deserialize_with = "Attributes::deserialize_current_schema")]
    pub(crate) current_schema: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) feedback_interval: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) numeric_characters: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) query_timeout: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) snapshot_transactions_enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) timestamp_utc_enabled: Option<bool>,
    // ##########################################################
    // ############# Database read-only attributes ##############
    // ##########################################################
    #[serde(skip_serializing)]
    pub(crate) compression_enabled: Option<bool>,
    #[serde(skip_serializing)]
    pub(crate) date_format: Option<String>,
    #[serde(skip_serializing)]
    pub(crate) date_language: Option<String>,
    #[serde(skip_serializing)]
    pub(crate) datetime_format: Option<String>,
    #[serde(skip_serializing)]
    pub(crate) default_like_escape_character: Option<String>,
    #[serde(skip_serializing)]
    #[serde(default)]
    #[serde(deserialize_with = "Attributes::deserialize_open_transaction")]
    pub(crate) open_transaction: Option<bool>,
    #[serde(skip_serializing)]
    pub(crate) timezone: Option<String>,
    #[serde(skip_serializing)]
    pub(crate) timezone_behavior: Option<String>,
}

impl Attributes {
    /// For some reason Exasol returns this as a number, although it's listed as bool.
    fn deserialize_open_transaction<'de, D>(deserializer: D) -> Result<Option<bool>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let Some(value) = Option::deserialize(deserializer)? else {return Ok(None)};

        match value {
            0 => Ok(Some(false)),
            1 => Ok(Some(true)),
            v => Err(D::Error::custom(format!(
                "Invalid value for 'open_transaction' field: {v}"
            ))),
        }
    }

    /// Exasol returns an empty string if no schema was selected.
    fn deserialize_current_schema<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let Some(value) = Option::deserialize(deserializer)? else {return Ok(None)};

        match String::is_empty(&value) {
            true => Ok(None),
            false => Ok(Some(value)),
        }
    }
}
