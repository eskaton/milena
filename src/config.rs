use std::path::Path;
use std::time::Duration;

use clap::ArgMatches;
use regex::Regex;

use crate::args::{ARG_BATCH_SIZE, ARG_BOOTSTRAP_SERVER, ARG_CONSUMER_GROUP, ARG_COUNT, ARG_EARLIEST, ARG_EXTRA_PROPERTIES, ARG_EXTRA_PROPERTIES_FILE, ARG_FOLLOW, ARG_GET, ARG_HEADERS, ARG_JSON_BATCH, ARG_KEY, ARG_KEY_FILE, ARG_KEY_REGEX, ARG_LAGS, ARG_NO_HEADERS, ARG_OFFSETS, ARG_PARTITION, ARG_PARTITIONS, ARG_PAYLOAD_FILE, ARG_REPLICATION, ARG_SET, ARG_TAIL, ARG_TIMEOUT, ARG_TOPIC, ARG_WITH_OFFSETS, OP_ALTER, OP_CREATE, OP_DELETE, OP_DESCRIBE, OP_LIST};
use crate::DEFAULT_GROUP_ID;

pub struct BaseConfig {
    pub servers: Vec<String>,
    pub properties: Option<Vec<(String, String)>>,
    pub timeout: Duration,
}

impl BaseConfig {
    pub fn new(matches: &ArgMatches) -> Self {
        let servers = matches.values_of(ARG_BOOTSTRAP_SERVER)
            .map(|v| v.map(|s| s.to_string()).collect::<Vec<String>>()).unwrap();
        let properties = BaseConfig::get_properties(matches);
        let timeout = Duration::from_millis(matches.value_of(ARG_TIMEOUT).map(|s| parse_timeout(&s.to_string())).unwrap());

        Self { servers, properties, timeout }
    }

    fn get_properties(matches: &ArgMatches) -> Option<Vec<(String, String)>> {
        if matches.is_present(ARG_EXTRA_PROPERTIES) {
            let values = matches.values_of(ARG_EXTRA_PROPERTIES).unwrap();
            let nv_pairs: Vec<(String, String)> = values
                .map(|s| split_name_value_pair("property", s))
                .collect();

            return Option::from(nv_pairs);
        } else if matches.is_present(ARG_EXTRA_PROPERTIES_FILE) {
            let path = matches.value_of(ARG_EXTRA_PROPERTIES_FILE).unwrap();

            assert!(Path::new(path).exists(), "File {} doesn't exist", path);

            return Option::from(dotproperties::parse_from_file(path).unwrap());
        }

        return None;
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
    pub fn new(matches: &ArgMatches) -> Self {
        let base = BaseConfig::new(matches);
        let consumer_group = matches.value_of(ARG_CONSUMER_GROUP).map(|s| s.to_string());

        Self { base, consumer_group }
    }
}

pub struct ConfigConfig {
    pub base: BaseConfig,
    pub topic: Option<String>,
    pub mode: ConfigMode,
    pub pattern: Option<String>,
    pub values: Option<Vec<(String, String)>>,
}

impl ConfigConfig {
    pub fn new(matches: &ArgMatches) -> Self {
        let base = BaseConfig::new(matches);
        let topic = matches.value_of(ARG_TOPIC).map(|s| s.to_string());
        let mode = match matches.is_present(ARG_SET) {
            true => ConfigMode::SET,
            _ => ConfigMode::GET
        };
        let pattern = matches.value_of(ARG_GET).map(|s| s.to_string());
        let values = matches.values_of(ARG_SET)
            .map(|v| v.map(|s| split_name_value_pair("configuration", s))
                .collect::<Vec<_>>());

        Self { base, topic, mode, pattern, values }
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
    pub lags: bool,
}

impl OffsetsConfig {
    pub fn new(matches: &ArgMatches) -> Self {
        let (matches, mode) = match matches.subcommand() {
            (OP_LIST, Some(matches)) => (matches, OffsetMode::LIST),
            (OP_ALTER, Some(matches)) => (matches, OffsetMode::ALTER),
            (cmd, _) => panic!("Invalid subcommand {}", cmd)
        };

        let base = BaseConfig::new(matches);
        let topic = matches.value_of(ARG_TOPIC).map(|s| s.to_string());
        let consumer_group = matches.value_of(ARG_CONSUMER_GROUP).map(|s| s.to_string());
        let partitions = match matches.is_present(ARG_PARTITIONS) {
            true => Some(matches.values_of(ARG_PARTITIONS)
                .map(|v| v.map(|s| parse_partition(&s.to_string()))
                    .collect::<Vec<i32>>()).unwrap()),
            _ => None
        };
        let offsets = match matches.is_present(ARG_OFFSETS) {
            true => Some(matches.values_of(ARG_OFFSETS)
                .map(|v| v.map(|s| parse_offset(&s.to_string()))
                    .collect::<Vec<i64>>()).unwrap()),
            _ => None
        };
        let earliest = matches.is_present(ARG_EARLIEST);
        let lags = matches.is_present(ARG_LAGS);

        Self {
            base,
            mode,
            topic,
            consumer_group,
            partitions,
            offsets,
            earliest,
            lags,
        }
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
    pub fn new(matches: &ArgMatches) -> Self {
        let (matches, mode) = match matches.subcommand() {
            (OP_LIST, Some(matches)) => (matches, TopicMode::LIST),
            (OP_DESCRIBE, Some(matches)) => (matches, TopicMode::DESCRIBE),
            (OP_ALTER, Some(matches)) => (matches, TopicMode::ALTER),
            (OP_CREATE, Some(matches)) => (matches, TopicMode::CREATE),
            (OP_DELETE, Some(matches)) => (matches, TopicMode::DELETE),
            (cmd, _) => panic!("Invalid subcommand {}", cmd)
        };

        let base = BaseConfig::new(matches);
        let topic = matches.value_of(ARG_TOPIC).map(|s| s.to_string());
        let with_offsets = matches.is_present(ARG_WITH_OFFSETS);
        let partitions = matches.value_of(ARG_PARTITIONS).map(|s| parse_partition(&s.to_string()));
        let replication = matches.value_of(ARG_REPLICATION).map(|s| parse_replication(&s.to_string()));

        Self {
            base,
            mode,
            topic,
            with_offsets,
            partitions,
            replication,
        }
    }
}

pub struct ConsumeConfig {
    pub base: BaseConfig,
    pub topic: String,
    pub partitions: Vec<i32>,
    pub offsets: Option<Vec<i64>>,
    pub consumer_group: String,
    pub follow: bool,
    pub no_headers: bool,
    pub tail: Option<i64>,
    pub count: Option<usize>,
    pub json_batch: bool,
    pub key_regex: Option<Regex>,
}

impl ConsumeConfig {
    pub fn new(matches: &ArgMatches) -> Self {
        let base = BaseConfig::new(matches);
        let topic = matches.value_of(ARG_TOPIC).map(|s| s.to_string()).unwrap();
        let partitions_unsorted = matches.values_of(ARG_PARTITIONS)
            .map(|v| v.map(|s| parse_partition(&s.to_string()))
                .collect::<Vec<i32>>()).unwrap();
        let maybe_offsets_unsorted = matches.values_of(ARG_OFFSETS)
            .map(|v| v.map(|s| parse_offset(&s.to_string()))
                .collect::<Vec<i64>>());
        let consumer_group = matches.value_of(ARG_CONSUMER_GROUP)
            .map(|s| s.to_string()).unwrap_or(DEFAULT_GROUP_ID.to_string());
        let follow = matches.is_present(ARG_FOLLOW);
        let no_headers = matches.is_present(ARG_NO_HEADERS);
        let tail = match matches.is_present(ARG_TAIL) {
            true => Some(matches.value_of(ARG_TAIL).map(|s| parse_number(&s.to_string())).unwrap()),
            false => None
        };
        let count = match matches.is_present(ARG_COUNT) {
            true => Some(matches.value_of(ARG_COUNT).map(|s| parse_count(&s.to_string())).unwrap()),
            false => None
        };
        let json_batch = matches.is_present(ARG_JSON_BATCH);
        let key_regex = matches.value_of(ARG_KEY_REGEX).map(|s| Regex::new(s).unwrap());

        assert!(count.unwrap_or(1) > 0, "count must be > 0");

        let mut partitions_sorted: Vec<i32>;
        let offsets_sorted: Option<Vec<i64>>;

        if maybe_offsets_unsorted.is_some() {
            let partitions_len = partitions_unsorted.len();
            let offsets_unsorted = maybe_offsets_unsorted.unwrap();
            let offsets_len = offsets_unsorted.len();

            assert_eq!(partitions_len, offsets_len, "Number of offsets must match number of partitions: {} != {}", offsets_len, partitions_len);

            let mut partitions_and_offsets: Vec<(&i32, &i64)> = partitions_unsorted.iter().zip(offsets_unsorted.iter()).collect();

            partitions_and_offsets.sort();

            partitions_sorted = partitions_and_offsets.iter().map(|t| *(*t).0).collect();
            offsets_sorted = Some(partitions_and_offsets.iter().map(|t| *(*t).1).collect());
        } else {
            partitions_sorted = partitions_unsorted;
            offsets_sorted = None;

            partitions_sorted.sort();
        }

        Self {
            base,
            topic,
            partitions: partitions_sorted,
            offsets: offsets_sorted,
            consumer_group,
            follow,
            no_headers,
            tail,
            count,
            json_batch,
            key_regex,
        }
    }
}

fn parse_partition(str: &String) -> i32 {
    return str.parse::<u32>()
        .expect(format!("Invalid partition: '{}'", str).as_str()) as i32;
}

fn parse_offset(str: &String) -> i64 {
    return str.parse::<i64>()
        .expect(format!("Invalid offset: '{}'", str).as_str());
}

fn parse_replication(str: &String) -> i32 {
    return str.parse::<i32>()
        .expect(format!("Invalid replication factor: '{}'", str).as_str());
}

fn parse_number(str: &String) -> i64 {
    return str.parse::<i64>()
        .expect(format!("Invalid number: '{}'", str).as_str());
}

fn parse_timeout(str: &String) -> u64 {
    return str.parse::<u64>()
        .expect(format!("Invalid timeout: '{}'", str).as_str());
}

fn parse_count(str: &String) -> usize {
    return str.parse::<usize>()
        .expect(format!("Invalid count: '{}'", str).as_str());
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
    pub fn new(matches: &ArgMatches) -> Self {
        let base = BaseConfig::new(matches);
        let topic = matches.value_of(ARG_TOPIC).map(|s| s.to_string()).unwrap();
        let partition = matches.value_of(ARG_PARTITION).map(|s| parse_partition(&s.to_string()));
        let key = matches.value_of(ARG_KEY).map(|s| s.to_string());
        let key_file = matches.value_of(ARG_KEY_FILE).map(|s| s.to_string());
        let payload_file = matches.value_of(ARG_PAYLOAD_FILE).map(|s| s.to_string());
        let headers_string = matches.values_of(ARG_HEADERS);
        let mut headers = Vec::new();

        headers_string.map(|values|
            values.for_each(|hs|
                headers.push(split_name_value_pair("header", hs))));

        let json_batch = matches.is_present(ARG_JSON_BATCH);
        let batch_size = matches.value_of(ARG_BATCH_SIZE).map(|s| parse_count(&s.to_string()));

        Self {
            base,
            topic,
            partition,
            key,
            key_file,
            payload_file,
            headers,
            json_batch,
            batch_size,
        }
    }
}

fn split_name_value_pair(description: &str, nv_pair: &str) -> (String, String) {
    let mut split = nv_pair.splitn(2, "=");
    let name = split.next().unwrap();
    let value = split.next().expect(format!("Invalid {}: {}", description, nv_pair).as_str());

    (name.to_string(), value.to_string())
}
