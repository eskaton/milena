use std::panic;
use std::path::Path;
use std::time::Duration;

use chrono::DateTime;
use clap::ArgMatches;
use regex::Regex;

use crate::{DEFAULT_GROUP_ID, error};
use crate::args::{ARG_ALL_PARTITIONS, ARG_BATCH_SIZE, ARG_BOOTSTRAP_SERVER, ARG_CONSUMER_GROUP, ARG_COUNT, ARG_EARLIEST, ARG_EXTRA_PROPERTIES, ARG_EXTRA_PROPERTIES_FILE, ARG_FOLLOW, ARG_GET, ARG_HEADER_REGEX, ARG_HEADERS, ARG_INCLUDE_DEFAULTS, ARG_JSON_BATCH, ARG_KEY, ARG_KEY_FILE, ARG_KEY_REGEX, ARG_LAGS, ARG_LATEST, ARG_NO_HEADERS, ARG_NO_KEY, ARG_NO_PAYLOAD, ARG_NO_TIMESTAMP, ARG_OFFSETS, ARG_PARTITION, ARG_PARTITIONS, ARG_PAYLOAD_FILE, ARG_REPLICATION, ARG_SET, ARG_TAIL, ARG_TIMEOUT, ARG_TIMESTAMP_AFTER, ARG_TIMESTAMP_BEFORE, ARG_TOPIC, ARG_WITH_OFFSETS, OP_ALTER, OP_CREATE, OP_DELETE, OP_DESCRIBE, OP_LIST};
use crate::error::MilenaError::GenericError;
use crate::error::Result;
use crate::MilenaError::ArgError;
use crate::utils::Swap;

pub struct BaseConfig {
    pub servers: Vec<String>,
    pub properties: Option<Vec<(String, String)>>,
    pub timeout: Duration,
}

impl BaseConfig {
    pub fn new(matches: &ArgMatches) -> Result<Self> {
        let servers = matches.values_of(ARG_BOOTSTRAP_SERVER)
            .map(|v| v.map(|s| s.to_string()).collect::<Vec<String>>()).unwrap();
        let properties = BaseConfig::get_properties(matches)?;
        let timeout = matches.value_of(ARG_TIMEOUT)
            .map(|s| parse_timeout(&s.to_string()))
            .swap()?
            .map(Duration::from_millis).unwrap();

        Ok(Self { servers, properties, timeout })
    }

    fn get_properties(matches: &ArgMatches) -> Result<Option<Vec<(String, String)>>> {
        if matches.is_present(ARG_EXTRA_PROPERTIES) {
            let values = matches.values_of(ARG_EXTRA_PROPERTIES).unwrap();
            let nv_pairs = values
                .map(|s| split_name_value_pair("property", s))
                .collect::<Vec<Result<(String, String)>>>().swap()?;

            return Ok(Option::from(nv_pairs));
        } else if matches.is_present(ARG_EXTRA_PROPERTIES_FILE) {
            let path = matches.value_of(ARG_EXTRA_PROPERTIES_FILE).unwrap();

            if !Path::new(path).exists() {
                return Err(GenericError(format!("File {} doesn't exist", path)));
            }

            return Ok(Option::from(dotproperties::parse_from_file(path).unwrap()));
        }

        return Ok(None);
    }
}

#[derive(PartialEq)]
pub enum ConfigMode {
    GET,
    SET,
}

pub struct GroupConfig {
    pub base: BaseConfig,
    pub consumer_group: Option<String>,
}

impl GroupConfig {
    pub fn new(matches: &ArgMatches) -> error::Result<Self> {
        let base = BaseConfig::new(matches)?;
        let consumer_group = matches.value_of(ARG_CONSUMER_GROUP).map(|s| s.to_string());

        Ok(Self { base, consumer_group })
    }
}

pub struct ConfigConfig {
    pub base: BaseConfig,
    pub topic: Option<String>,
    pub mode: ConfigMode,
    pub pattern: Option<String>,
    pub values: Option<Vec<(String, String)>>,
    pub include_defaults: bool,
}

impl ConfigConfig {
    pub fn new(matches: &ArgMatches) -> Result<Self> {
        let base = BaseConfig::new(matches)?;
        let topic = matches.value_of(ARG_TOPIC).map(|s| s.to_string());
        let mode = match matches.is_present(ARG_SET) {
            true => ConfigMode::SET,
            _ => ConfigMode::GET
        };
        let pattern = matches.value_of(ARG_GET).map(|s| s.to_string());
        let values = match matches.values_of(ARG_SET)
            .map(|v| v.map(|s| split_name_value_pair("configuration", s))
                .collect::<Vec<_>>()).map(|s| s.swap()) {
            Some(v) => {
                let x = v?;
                Some(x)
            }
            None => None
        };
        let include_defaults = matches.contains_id(ARG_INCLUDE_DEFAULTS);

        Ok(Self { base, topic, mode, pattern, values, include_defaults })
    }
}

#[derive(PartialEq)]
pub enum OffsetMode {
    LIST,
    ALTER,
}

pub struct OffsetsConfig {
    pub base: BaseConfig,
    pub mode: OffsetMode,
    pub topic: Option<String>,
    pub consumer_group: Option<String>,
    pub partitions: Option<Vec<i32>>,
    pub offsets: Option<Vec<i64>>,
    pub earliest: bool,
    pub latest: bool,
    pub lags: bool,
}

impl OffsetsConfig {
    pub fn new(matches: &ArgMatches) -> Result<Self> {
        let (matches, mode) = match matches.subcommand() {
            Some((OP_LIST, matches)) => (matches, OffsetMode::LIST),
            Some((OP_ALTER, matches)) => (matches, OffsetMode::ALTER),
            _ => panic!("Invalid subcommand")
        };

        let base = BaseConfig::new(matches)?;
        let topic = matches.value_of(ARG_TOPIC).map(|s| s.to_string());
        let consumer_group = matches.value_of(ARG_CONSUMER_GROUP).map(|s| s.to_string());
        let partitions = matches.try_get_many::<String>(ARG_PARTITIONS)
            .ok()
            .unwrap_or(None)
            .map(|v| v.map(|s| parse_partition(s)).collect::<Result<Vec<i32>>>()).swap()?;
        let offsets = matches.try_get_many::<String>(ARG_OFFSETS)
            .ok()
            .unwrap_or(None)
            .map(|v| v.map(|s| parse_offset(s)).collect::<Result<Vec<i64>>>()).swap()?;
        let earliest = matches.try_contains_id(ARG_EARLIEST).unwrap_or(false);
        let latest = matches.try_contains_id(ARG_LATEST).unwrap_or(false);
        let lags = matches.try_contains_id(ARG_LAGS).unwrap_or(false);

        Ok(Self {
            base,
            mode,
            topic,
            consumer_group,
            partitions,
            offsets,
            earliest,
            latest,
            lags,
        })
    }
}

#[derive(PartialEq)]
pub enum TopicMode {
    LIST,
    DESCRIBE,
    CREATE,
    DELETE,
    ALTER,
}

pub struct TopicConfig {
    pub base: BaseConfig,
    pub mode: TopicMode,
    pub topic: Option<String>,
    pub with_offsets: bool,
    pub partitions: Option<i32>,
    pub replication: Option<i32>,
}

impl TopicConfig {
    pub fn new(matches: &ArgMatches) -> Result<Self> {
        let (matches, mode) = match matches.subcommand() {
            Some((OP_LIST, matches)) => (matches, TopicMode::LIST),
            Some((OP_DESCRIBE, matches)) => (matches, TopicMode::DESCRIBE),
            Some((OP_ALTER, matches)) => (matches, TopicMode::ALTER),
            Some((OP_CREATE, matches)) => (matches, TopicMode::CREATE),
            Some((OP_DELETE, matches)) => (matches, TopicMode::DELETE),
            _ => panic!("Invalid subcommand")
        };

        let base = BaseConfig::new(matches)?;
        let topic = matches.value_of(ARG_TOPIC).map(|s| s.to_string());
        let with_offsets = matches.try_contains_id(ARG_WITH_OFFSETS).unwrap_or(false);
        let partitions = matches.try_get_one(ARG_PARTITIONS).unwrap_or(None).map(|s: &String| parse_partition(s)).swap()?;
        let replication = matches.try_get_one(ARG_REPLICATION).unwrap_or(None).map(|s: &String| parse_replication(s)).swap()?;

        Ok(Self {
            base,
            mode,
            topic,
            with_offsets,
            partitions,
            replication,
        })
    }
}

pub struct ConsumeConfig {
    pub base: BaseConfig,
    pub topic: String,
    pub partitions: Vec<i32>,
    pub all_partitions: bool,
    pub offsets: Option<Vec<i64>>,
    pub consumer_group: String,
    pub follow: bool,
    pub no_headers: bool,
    pub no_timestamp: bool,
    pub no_key: bool,
    pub no_payload: bool,
    pub tail: Option<i64>,
    pub count: Option<usize>,
    pub json_batch: bool,
    pub key_regex: Option<Regex>,
    pub header_regexes: Option<Vec<(Regex, Regex)>>,
    pub timestamp_before: Option<i64>,
    pub timestamp_after: Option<i64>,
    pub latest: bool,
}

impl ConsumeConfig {
    pub fn new(matches: &ArgMatches) -> Result<Self> {
        let base = BaseConfig::new(matches)?;
        let topic = matches.value_of(ARG_TOPIC).map(|s| s.to_string()).unwrap();
        let partitions_unsorted = matches.values_of(ARG_PARTITIONS)
            .map(|v| v.map(|s| parse_partition(&s.to_string()))
                .collect::<Result<Vec<i32>>>()).swap()?.unwrap();
        let all_partitions = matches.is_present(ARG_ALL_PARTITIONS);
        let maybe_offsets_unsorted = matches.values_of(ARG_OFFSETS)
            .map(|v| v.map(|s| parse_offset(&s.to_string()))
                .collect::<Result<Vec<i64>>>()).swap()?;
        let consumer_group = matches.value_of(ARG_CONSUMER_GROUP)
            .map(|s| s.to_string()).unwrap_or(DEFAULT_GROUP_ID.to_string());
        let follow = matches.is_present(ARG_FOLLOW);
        let no_headers = matches.is_present(ARG_NO_HEADERS);
        let no_timestamp = matches.is_present(ARG_NO_TIMESTAMP);
        let no_key = matches.is_present(ARG_NO_KEY);
        let no_payload = matches.is_present(ARG_NO_PAYLOAD);
        let tail = matches.value_of(ARG_TAIL).map(|s| parse_number(&s.to_string())).swap()?;
        let count = matches.value_of(ARG_COUNT).map(|s| parse_count(&s.to_string())).swap()?;
        let json_batch = matches.is_present(ARG_JSON_BATCH);
        let key_regex = matches.value_of(ARG_KEY_REGEX).map(|s| Regex::new(s).unwrap());
        let header_regexes = matches.values_of(ARG_HEADER_REGEX)
            .map(|values| values.map(|s| parse_header_regex(s))
                .collect()).swap()?;
        let timestamp_before = matches.value_of(ARG_TIMESTAMP_BEFORE).map(|s| parse_timestamp(s)).swap()?;
        let timestamp_after = matches.value_of(ARG_TIMESTAMP_AFTER).map(|s| parse_timestamp(s)).swap()?;
        let latest = matches.is_present(ARG_LATEST);

        if let Some(opt_count) = count {
            if opt_count < 1 {
                return Err(GenericError("Count must be > 0".to_string()));
            }
        }

        if let (Some(opt_ts_before), Some(opt_ts_after)) = (timestamp_before, timestamp_after) {
            if opt_ts_before - opt_ts_after > 0 {
                return Err(ArgError("The timestamps are overlapping".to_string()));
            }
        }

        let mut partitions_sorted: Vec<i32>;
        let offsets_sorted: Option<Vec<i64>>;

        if maybe_offsets_unsorted.is_some() {
            let partitions_len = partitions_unsorted.len();
            let offsets_unsorted = maybe_offsets_unsorted.unwrap();
            let offsets_len = offsets_unsorted.len();

            if partitions_len != offsets_len {
                return Err(ArgError(format!("Number of offsets must match number of partitions: {} != {}", offsets_len, partitions_len)));
            }

            let mut partitions_and_offsets: Vec<(&i32, &i64)> = partitions_unsorted.iter().zip(offsets_unsorted.iter()).collect();

            partitions_and_offsets.sort();

            partitions_sorted = partitions_and_offsets.iter().map(|&(&p, _)| p).collect();
            offsets_sorted = Some(partitions_and_offsets.iter().map(|&(_, &o)| o).collect());
        } else {
            partitions_sorted = partitions_unsorted;
            offsets_sorted = None;

            partitions_sorted.sort();
        }

        Ok(Self {
            base,
            topic,
            partitions: partitions_sorted,
            all_partitions,
            offsets: offsets_sorted,
            consumer_group,
            follow,
            no_headers,
            no_timestamp,
            no_key,
            no_payload,
            tail,
            count,
            json_batch,
            key_regex,
            header_regexes,
            timestamp_before,
            timestamp_after,
            latest,
        })
    }
}

fn parse_header_regex(s: &str) -> Result<(Regex, Regex)> {
    s.split_once('=')
        .map(|(k, v)| (Regex::new(k).unwrap(), Regex::new(v).unwrap()))
        .ok_or(ArgError(format!("Invalid header regex '{}'", s)))
}

fn parse_timestamp(s: &str) -> Result<i64> {
    let ts = DateTime::parse_from_rfc3339(s)
        .map_err(|e| ArgError(format!("Invalid ISO-8601 timestamp '{}': {}", s, e)))?;

    Ok(ts.timestamp_nanos())
}

fn parse_partition(str: &String) -> Result<i32> {
    str.parse::<u32>()
        .map(|v| v as i32)
        .map_err(|e| ArgError(format!("Invalid partition '{}': {}", str, e)))
}

fn parse_offset(str: &String) -> Result<i64> {
    str.parse::<i64>()
        .map_err(|e| ArgError(format!("Invalid offset '{}': {}", str, e)))
}

fn parse_replication(str: &String) -> Result<i32> {
    str.parse::<i32>()
        .map_err(|e| ArgError(format!("Invalid replication factor '{}': {}", str, e)))
}

fn parse_number(str: &String) -> Result<i64> {
    str.parse::<i64>()
        .map_err(|e| ArgError(format!("Invalid number '{}': {}", str, e)))
}

fn parse_timeout(str: &String) -> Result<u64> {
    str.parse::<u64>()
        .map_err(|e| ArgError(format!("Invalid timeout '{}': {}", str, e)))
}

fn parse_count(str: &String) -> Result<usize> {
    str.parse::<usize>()
        .map_err(|e| ArgError(format!("Invalid count '{}': {}", str, e)))
}

pub struct ProduceConfig {
    pub base: BaseConfig,
    pub topic: String,
    pub partition: Option<i32>,
    pub key: Option<String>,
    pub key_file: Option<String>,
    pub payload_file: Option<String>,
    pub headers: Vec<(String, String)>,
    pub json_batch: bool,
    pub batch_size: Option<usize>,
}

impl ProduceConfig {
    pub fn new(matches: &ArgMatches) -> Result<Self> {
        let base = BaseConfig::new(matches)?;
        let topic = matches.value_of(ARG_TOPIC).map(|s| s.to_string()).unwrap();
        let partition = matches.value_of(ARG_PARTITION).map(|s| parse_partition(&s.to_string())).swap()?;
        let key = matches.value_of(ARG_KEY).map(|s| s.to_string());
        let key_file = matches.value_of(ARG_KEY_FILE).map(|s| s.to_string());
        let payload_file = matches.value_of(ARG_PAYLOAD_FILE).map(|s| s.to_string());
        let headers_string = matches.values_of(ARG_HEADERS);
        let result_headers = headers_string
            .map(|values| values.map(|hs| split_name_value_pair("header", hs))
                .collect::<Vec<Result<(String, String)>>>().swap());

        let headers = match result_headers {
            None => Vec::new(),
            Some(h) => h?
        };

        let json_batch = matches.is_present(ARG_JSON_BATCH);
        let batch_size = matches.value_of(ARG_BATCH_SIZE).map(|s| parse_count(&s.to_string())).swap()?;

        Ok(Self {
            base,
            topic,
            partition,
            key,
            key_file,
            payload_file,
            headers,
            json_batch,
            batch_size,
        })
    }
}

fn split_name_value_pair(description: &str, nv_pair: &str) -> Result<(String, String)> {
    let mut split = nv_pair.splitn(2, "=");
    let name = split.next();
    let value = split.next();

    match (name, value) {
        (Some(n), Some(v)) => Ok((n.to_string(), v.to_string())),
        _ => Err(ArgError(format!("Invalid {}: {}", description, nv_pair)))
    }
}

#[test]
fn test_split_name_value_pair() {
    assert_eq!(Ok(("a".to_string(), "b".to_string())), split_name_value_pair("", "a=b"));
    assert_eq!(Ok(("a".to_string(), "b=c".to_string())), split_name_value_pair("", "a=b=c"));
    assert!(split_name_value_pair("", "a=").is_err());
    assert!(split_name_value_pair("", "a").is_err());
    assert!(split_name_value_pair("", "").is_err());
}