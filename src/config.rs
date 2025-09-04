use crate::args::{
    ARG_ALL_PARTITIONS, ARG_BATCH_SIZE, ARG_BOOTSTRAP_SERVER, ARG_CONSUMER_GROUP, ARG_COUNT,
    ARG_EARLIEST, ARG_EXTRA_PROPERTIES, ARG_EXTRA_PROPERTIES_FILE, ARG_FOLLOW, ARG_GET,
    ARG_HEADERS, ARG_HEADER_REGEX, ARG_INCLUDE_DEFAULTS, ARG_JSON_BATCH, ARG_KEY, ARG_KEY_FILE,
    ARG_KEY_REGEX, ARG_LAGS, ARG_LATEST, ARG_NO_HEADERS, ARG_NO_KEY, ARG_NO_PAYLOAD,
    ARG_NO_TIMESTAMP, ARG_OFFSETS, ARG_OUTPUT_FILE, ARG_PARTITION, ARG_PARTITIONS,
    ARG_PAYLOAD_FILE, ARG_PAYLOAD_REGEX, ARG_REPLICATION, ARG_RESOLVE, ARG_SET, ARG_TAIL,
    ARG_TIMEOUT, ARG_TIMESTAMP_AFTER, ARG_TIMESTAMP_BEFORE, ARG_TOPIC, ARG_WITH_ASSIGNMENTS, ARG_WITH_OFFSETS,
    OP_ALTER, OP_CREATE, OP_DELETE, OP_DESCRIBE, OP_LIST,
};
use crate::error::MilenaError::GenericError;
use crate::error::Result;
use crate::resolve::add_override;
use crate::utils::Swap;
use crate::MilenaError::ArgError;
use crate::DEFAULT_GROUP_ID;
use chrono::DateTime;
use clap::ArgMatches;
use lazy_static::lazy_static;
use regex::Regex;
use std::collections::HashMap;
use std::panic;
use std::path::Path;
use std::string::ToString;
use std::time::Duration;

#[derive(Copy, Clone, PartialEq)]
pub enum ConfigType {
    Producer,
    Consumer,
    Both,
}

lazy_static! {
    pub(crate) static ref CONFIG_PROPERTIES: HashMap<&'static str, ConfigType> = [
        ("acks", ConfigType::Producer),
        ("allow.auto.create.topics", ConfigType::Both),
        ("api.version.fallback.ms", ConfigType::Both),
        ("api.version.request", ConfigType::Both),
        ("api.version.request.timeout.ms", ConfigType::Both),
        ("auto.commit.enable", ConfigType::Consumer),
        ("auto.commit.interval.ms", ConfigType::Consumer),
        ("auto.commit.interval.ms", ConfigType::Consumer),
        ("auto.offset.reset", ConfigType::Consumer),
        ("background_event_cb", ConfigType::Both),
        ("batch.num.messages", ConfigType::Producer),
        ("batch.size", ConfigType::Producer),
        ("bootstrap.servers", ConfigType::Both),
        ("broker.address.family", ConfigType::Both),
        ("broker.address.ttl", ConfigType::Both),
        ("broker.version.fallback", ConfigType::Both),
        ("builtin.features", ConfigType::Both),
        ("check.crcs", ConfigType::Consumer),
        ("client.dns.lookup", ConfigType::Both),
        ("client.id", ConfigType::Both),
        ("client.rack", ConfigType::Both),
        ("closesocket_cb", ConfigType::Both),
        ("compression.codec", ConfigType::Producer),
        ("compression.codec", ConfigType::Producer),
        ("compression.level", ConfigType::Producer),
        ("compression.type", ConfigType::Producer),
        ("compression.type", ConfigType::Producer),
        ("connect_cb", ConfigType::Both),
        ("connections.max.idle.ms", ConfigType::Both),
        ("consume.callback.max.messages", ConfigType::Consumer),
        ("consume_cb", ConfigType::Consumer),
        ("coordinator.query.interval.ms", ConfigType::Consumer),
        ("debug", ConfigType::Both),
        ("default_topic_conf", ConfigType::Both),
        ("delivery.report.only.error", ConfigType::Producer),
        ("delivery.timeout.ms", ConfigType::Producer),
        ("dr_cb", ConfigType::Producer),
        ("dr_msg_cb", ConfigType::Producer),
        ("enable.auto.commit", ConfigType::Consumer),
        ("enable.auto.commit", ConfigType::Consumer),
        ("enable.auto.offset.store", ConfigType::Consumer),
        ("enabled_events", ConfigType::Both),
        ("enable.gapless.guarantee", ConfigType::Producer),
        ("enable.idempotence", ConfigType::Producer),
        ("enable.metrics.push", ConfigType::Both),
        ("enable.partition.eof", ConfigType::Consumer),
        ("enable.random.seed", ConfigType::Both),
        ("enable.sasl.oauthbearer.unsecure.jwt", ConfigType::Both),
        ("enable.ssl.certificate.verification", ConfigType::Both),
        ("error_cb", ConfigType::Both),
        ("fetch.error.backoff.ms", ConfigType::Consumer),
        ("fetch.max.bytes", ConfigType::Consumer),
        ("fetch.message.max.bytes", ConfigType::Consumer),
        ("fetch.min.bytes", ConfigType::Consumer),
        ("fetch.queue.backoff.ms", ConfigType::Consumer),
        ("fetch.wait.max.ms", ConfigType::Consumer),
        ("group.id", ConfigType::Consumer),
        ("group.instance.id", ConfigType::Consumer),
        ("group.protocol", ConfigType::Consumer),
        ("group.protocol.type", ConfigType::Consumer),
        ("group.remote.assignor", ConfigType::Consumer),
        ("heartbeat.interval.ms", ConfigType::Consumer),
        ("interceptors", ConfigType::Both),
        ("internal.termination.signal", ConfigType::Both),
        ("isolation.level", ConfigType::Consumer),
        ("linger.ms", ConfigType::Producer),
        ("log_cb", ConfigType::Both),
        ("log.connection.close", ConfigType::Both),
        ("log_level", ConfigType::Both),
        ("log.queue", ConfigType::Both),
        ("log.thread.name", ConfigType::Both),
        ("max.in.flight", ConfigType::Both),
        ("max.in.flight.requests.per.connection", ConfigType::Both),
        ("max.partition.fetch.bytes", ConfigType::Consumer),
        ("max.poll.interval.ms", ConfigType::Consumer),
        ("message.copy.max.bytes", ConfigType::Both),
        ("message.max.bytes", ConfigType::Both),
        ("message.send.max.retries", ConfigType::Producer),
        ("message.timeout.ms", ConfigType::Producer),
        ("metadata.broker.list", ConfigType::Both),
        ("metadata.max.age.ms", ConfigType::Both),
        ("msg_order_cmp", ConfigType::Producer),
        ("oauthbearer_token_refresh_cb", ConfigType::Both),
        ("offset_commit_cb", ConfigType::Consumer),
        ("offset.store.method", ConfigType::Consumer),
        ("offset.store.method", ConfigType::Consumer),
        ("offset.store.path", ConfigType::Consumer),
        ("offset.store.sync.interval.ms", ConfigType::Consumer),
        ("opaque", ConfigType::Both),
        ("opaque", ConfigType::Both),
        ("open_cb", ConfigType::Both),
        ("partition.assignment.strategy", ConfigType::Consumer),
        ("partitioner_cb", ConfigType::Producer),
        ("partitioner", ConfigType::Producer),
        ("plugin.library.paths", ConfigType::Both),
        ("produce.offset.report", ConfigType::Producer),
        (
            "queue.buffering.backpressure.threshold",
            ConfigType::Producer
        ),
        ("queue.buffering.max.kbytes", ConfigType::Producer),
        ("queue.buffering.max.messages", ConfigType::Producer),
        ("queue.buffering.max.ms", ConfigType::Producer),
        ("queued.max.messages.kbytes", ConfigType::Consumer),
        ("queued.min.messages", ConfigType::Consumer),
        ("queuing.strategy", ConfigType::Producer),
        ("rebalance_cb", ConfigType::Consumer),
        ("receive.message.max.bytes", ConfigType::Both),
        ("reconnect.backoff.jitter.ms", ConfigType::Both),
        ("reconnect.backoff.max.ms", ConfigType::Both),
        ("reconnect.backoff.ms", ConfigType::Both),
        ("request.required.acks", ConfigType::Producer),
        ("request.timeout.ms", ConfigType::Producer),
        ("resolve_cb", ConfigType::Both),
        ("retries", ConfigType::Producer),
        ("retry.backoff.max.ms", ConfigType::Both),
        ("retry.backoff.ms", ConfigType::Both),
        ("sasl.kerberos.keytab", ConfigType::Both),
        ("sasl.kerberos.kinit.cmd", ConfigType::Both),
        ("sasl.kerberos.min.time.before.relogin", ConfigType::Both),
        ("sasl.kerberos.principal", ConfigType::Both),
        ("sasl.kerberos.service.name", ConfigType::Both),
        ("sasl.mechanism", ConfigType::Both),
        ("sasl.mechanisms", ConfigType::Both),
        ("sasl.oauthbearer.client.id", ConfigType::Both),
        ("sasl.oauthbearer.client.secret", ConfigType::Both),
        ("sasl.oauthbearer.config", ConfigType::Both),
        ("sasl.oauthbearer.extensions", ConfigType::Both),
        ("sasl.oauthbearer.method", ConfigType::Both),
        ("sasl.oauthbearer.scope", ConfigType::Both),
        ("sasl.oauthbearer.token.endpoint.url", ConfigType::Both),
        ("sasl.password", ConfigType::Both),
        ("sasl.username", ConfigType::Both),
        ("security.protocol", ConfigType::Both),
        ("session.timeout.ms", ConfigType::Consumer),
        ("socket.blocking.max.ms", ConfigType::Both),
        ("socket_cb", ConfigType::Both),
        ("socket.connection.setup.timeout.ms", ConfigType::Both),
        ("socket.keepalive.enable", ConfigType::Both),
        ("socket.max.fails", ConfigType::Both),
        ("socket.nagle.disable", ConfigType::Both),
        ("socket.receive.buffer.bytes", ConfigType::Both),
        ("socket.send.buffer.bytes", ConfigType::Both),
        ("socket.timeout.ms", ConfigType::Both),
        ("ssl.ca.certificate.stores", ConfigType::Both),
        ("ssl_ca", ConfigType::Both),
        ("ssl.ca.location", ConfigType::Both),
        ("ssl.ca.pem", ConfigType::Both),
        ("ssl_certificate", ConfigType::Both),
        ("ssl.certificate.location", ConfigType::Both),
        ("ssl.certificate.pem", ConfigType::Both),
        ("ssl.certificate.verify_cb", ConfigType::Both),
        ("ssl.cipher.suites", ConfigType::Both),
        ("ssl.crl.location", ConfigType::Both),
        ("ssl.curves.list", ConfigType::Both),
        ("ssl.endpoint.identification.algorithm", ConfigType::Both),
        ("ssl_engine_callback_data", ConfigType::Both),
        ("ssl.engine.id", ConfigType::Both),
        ("ssl.engine.location", ConfigType::Both),
        ("ssl_key", ConfigType::Both),
        ("ssl.key.location", ConfigType::Both),
        ("ssl.key.password", ConfigType::Both),
        ("ssl.key.pem", ConfigType::Both),
        ("ssl.keystore.location", ConfigType::Both),
        ("ssl.keystore.password", ConfigType::Both),
        ("ssl.providers", ConfigType::Both),
        ("ssl.sigalgs.list", ConfigType::Both),
        ("statistics.interval.ms", ConfigType::Both),
        ("stats_cb", ConfigType::Both),
        ("sticky.partitioning.linger.ms", ConfigType::Producer),
        ("throttle_cb", ConfigType::Both),
        ("topic.blacklist", ConfigType::Both),
        ("topic.metadata.propagation.max.ms", ConfigType::Both),
        ("topic.metadata.refresh.fast.cnt", ConfigType::Both),
        ("topic.metadata.refresh.fast.interval.ms", ConfigType::Both),
        ("topic.metadata.refresh.interval.ms", ConfigType::Both),
        ("topic.metadata.refresh.sparse", ConfigType::Both),
        ("transactional.id", ConfigType::Producer),
        ("transaction.timeout.ms", ConfigType::Producer)
    ]
    .iter()
    .copied()
    .collect();
}

pub struct BaseConfig {
    pub servers: Vec<String>,
    pub properties: Option<Vec<(String, String)>>,
    pub timeout: Duration,
}

impl BaseConfig {
    pub fn new(matches: &ArgMatches) -> Result<Self> {
        let servers = matches
            .values_of(ARG_BOOTSTRAP_SERVER)
            .map(|v| v.map(|s| s.to_string()).collect::<Vec<String>>())
            .unwrap();
        let properties = BaseConfig::get_properties(matches)?;
        let timeout = matches
            .value_of(ARG_TIMEOUT)
            .map(|s| parse_timeout(&s.to_string()))
            .swap()?
            .map(Duration::from_millis)
            .unwrap();

        matches.values_of(ARG_RESOLVE).into_iter().for_each(|v| {
            v.into_iter().for_each(|s| {
                let mut parts = s.splitn(2, ':');

                add_override(parts.next().unwrap(), parts.next().unwrap())
            })
        });

        Ok(Self {
            servers,
            properties,
            timeout,
        })
    }

    fn get_properties(matches: &ArgMatches) -> Result<Option<Vec<(String, String)>>> {
        if matches.is_present(ARG_EXTRA_PROPERTIES) {
            let values = matches.values_of(ARG_EXTRA_PROPERTIES).unwrap();
            let nv_pairs = values
                .map(|s| split_name_value_pair("property", s))
                .collect::<Vec<Result<(String, String)>>>()
                .swap()?;

            return Ok(Option::from(nv_pairs));
        } else if matches.is_present(ARG_EXTRA_PROPERTIES_FILE) {
            let path = matches.value_of(ARG_EXTRA_PROPERTIES_FILE).unwrap();

            if !Path::new(path).exists() {
                return Err(GenericError(format!("File {} doesn't exist", path)));
            }

            return Ok(Option::from(dotproperties::parse_from_file(path).unwrap()));
        }

        Ok(None)
    }
}

#[derive(PartialEq)]
pub enum GroupMode {
    List,
    Delete,
}

pub struct GroupConfig {
    pub base: BaseConfig,
    pub mode: GroupMode,
    pub consumer_group: Option<String>,
    pub with_assignments: bool,
}

impl GroupConfig {
    pub fn new(matches: &ArgMatches) -> Result<Self> {
        let (matches, mode) = match matches.subcommand() {
            Some((OP_LIST, matches)) => (matches, GroupMode::List),
            Some((OP_DELETE, matches)) => (matches, GroupMode::Delete),
            _ => panic!("Invalid subcommand"),
        };

        let base = BaseConfig::new(matches)?;
        let consumer_group = matches.value_of(ARG_CONSUMER_GROUP).map(|s| s.to_string());
        let with_assignments = matches.try_contains_id(ARG_WITH_ASSIGNMENTS).unwrap_or(false);

        Ok(Self {
            base,
            mode,
            consumer_group,
            with_assignments,
        })
    }
}

#[derive(PartialEq)]
pub enum ConfigMode {
    Get,
    Set,
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
            true => ConfigMode::Set,
            _ => ConfigMode::Get,
        };
        let pattern = matches.value_of(ARG_GET).map(|s| s.to_string());
        let values = match matches
            .values_of(ARG_SET)
            .map(|v| {
                v.map(|s| split_name_value_pair("configuration", s))
                    .collect::<Vec<_>>()
            })
            .map(|s| s.swap())
        {
            Some(v) => {
                let x = v?;
                Some(x)
            }
            None => None,
        };
        let include_defaults = matches.contains_id(ARG_INCLUDE_DEFAULTS);

        Ok(Self {
            base,
            topic,
            mode,
            pattern,
            values,
            include_defaults,
        })
    }
}

#[derive(PartialEq)]
pub enum OffsetMode {
    List,
    Alter,
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
            Some((OP_LIST, matches)) => (matches, OffsetMode::List),
            Some((OP_ALTER, matches)) => (matches, OffsetMode::Alter),
            _ => panic!("Invalid subcommand"),
        };

        let base = BaseConfig::new(matches)?;
        let topic = matches.value_of(ARG_TOPIC).map(|s| s.to_string());
        let consumer_group = matches.value_of(ARG_CONSUMER_GROUP).map(|s| s.to_string());
        let partitions = matches
            .try_get_many::<String>(ARG_PARTITIONS)
            .ok()
            .unwrap_or(None)
            .map(|v| v.map(parse_partition).collect::<Result<Vec<i32>>>())
            .swap()?;
        let offsets = matches
            .try_get_many::<String>(ARG_OFFSETS)
            .ok()
            .unwrap_or(None)
            .map(|v| v.map(parse_offset).collect::<Result<Vec<i64>>>())
            .swap()?;
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
    List,
    Describe,
    Create,
    Delete,
    Alter,
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
            Some((OP_LIST, matches)) => (matches, TopicMode::List),
            Some((OP_DESCRIBE, matches)) => (matches, TopicMode::Describe),
            Some((OP_ALTER, matches)) => (matches, TopicMode::Alter),
            Some((OP_CREATE, matches)) => (matches, TopicMode::Create),
            Some((OP_DELETE, matches)) => (matches, TopicMode::Delete),
            _ => panic!("Invalid subcommand"),
        };

        let base = BaseConfig::new(matches)?;
        let topic = matches.value_of(ARG_TOPIC).map(|s| s.to_string());
        let with_offsets = matches.try_contains_id(ARG_WITH_OFFSETS).unwrap_or(false);
        let partitions = matches
            .try_get_one(ARG_PARTITIONS)
            .unwrap_or(None)
            .map(|s: &String| parse_partition(s))
            .swap()?;
        let replication = matches
            .try_get_one(ARG_REPLICATION)
            .unwrap_or(None)
            .map(|s: &String| parse_replication(s))
            .swap()?;

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
    pub payload_regex: Option<Regex>,
    pub header_regexes: Option<Vec<(Regex, Regex)>>,
    pub timestamp_before: Option<i64>,
    pub timestamp_after: Option<i64>,
    pub latest: bool,
    pub output_file: Option<String>,
}

impl ConsumeConfig {
    pub fn new(matches: &ArgMatches) -> Result<Self> {
        let base = BaseConfig::new(matches)?;
        let topic = matches.value_of(ARG_TOPIC).map(|s| s.to_string()).unwrap();
        let partitions_unsorted = matches
            .values_of(ARG_PARTITIONS)
            .map(|v| {
                v.map(|s| parse_partition(&s.to_string()))
                    .collect::<Result<Vec<i32>>>()
            })
            .swap()?
            .unwrap();
        let all_partitions = matches.is_present(ARG_ALL_PARTITIONS);
        let maybe_offsets_unsorted = matches
            .values_of(ARG_OFFSETS)
            .map(|v| {
                v.map(|s| parse_offset(&s.to_string()))
                    .collect::<Result<Vec<i64>>>()
            })
            .swap()?;
        let consumer_group = matches
            .value_of(ARG_CONSUMER_GROUP)
            .map(|s| s.to_string())
            .unwrap_or(DEFAULT_GROUP_ID.to_string());
        let follow = matches.is_present(ARG_FOLLOW);
        let no_headers = matches.is_present(ARG_NO_HEADERS);
        let no_timestamp = matches.is_present(ARG_NO_TIMESTAMP);
        let no_key = matches.is_present(ARG_NO_KEY);
        let no_payload = matches.is_present(ARG_NO_PAYLOAD);
        let tail = matches
            .value_of(ARG_TAIL)
            .map(|s| parse_number(&s.to_string()))
            .swap()?;
        let count = matches
            .value_of(ARG_COUNT)
            .map(|s| parse_count(&s.to_string()))
            .swap()?;
        let json_batch = matches.is_present(ARG_JSON_BATCH);
        let key_regex = matches
            .value_of(ARG_KEY_REGEX)
            .map(|s| Regex::new(s).unwrap());
        let payload_regex = matches
            .value_of(ARG_PAYLOAD_REGEX)
            .map(|s| Regex::new(s).unwrap());
        let header_regexes = matches
            .values_of(ARG_HEADER_REGEX)
            .map(|values| values.map(parse_header_regex).collect())
            .swap()?;
        let timestamp_before = matches
            .value_of(ARG_TIMESTAMP_BEFORE)
            .map(parse_timestamp)
            .swap()?;
        let timestamp_after = matches
            .value_of(ARG_TIMESTAMP_AFTER)
            .map(parse_timestamp)
            .swap()?;
        let latest = matches.is_present(ARG_LATEST);
        let output_file = matches.value_of(ARG_OUTPUT_FILE).map(|s| s.to_string());

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
                return Err(ArgError(format!(
                    "Number of offsets must match number of partitions: {} != {}",
                    offsets_len, partitions_len
                )));
            }

            let mut partitions_and_offsets: Vec<(&i32, &i64)> = partitions_unsorted
                .iter()
                .zip(offsets_unsorted.iter())
                .collect();

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
            payload_regex,
            header_regexes,
            timestamp_before,
            timestamp_after,
            latest,
            output_file,
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

    Ok(ts.timestamp_nanos_opt().unwrap())
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
        let partition = matches
            .value_of(ARG_PARTITION)
            .map(|s| parse_partition(&s.to_string()))
            .swap()?;
        let key = matches.value_of(ARG_KEY).map(|s| s.to_string());
        let key_file = matches.value_of(ARG_KEY_FILE).map(|s| s.to_string());
        let payload_file = matches.value_of(ARG_PAYLOAD_FILE).map(|s| s.to_string());
        let headers_string = matches.values_of(ARG_HEADERS);
        let result_headers = headers_string.map(|values| {
            values
                .map(|hs| split_name_value_pair("header", hs))
                .collect::<Vec<Result<(String, String)>>>()
                .swap()
        });

        let headers = match result_headers {
            None => Vec::new(),
            Some(h) => h?,
        };

        let json_batch = matches.is_present(ARG_JSON_BATCH);
        let batch_size = matches
            .value_of(ARG_BATCH_SIZE)
            .map(|s| parse_count(&s.to_string()))
            .swap()?;

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
        _ => Err(ArgError(format!("Invalid {}: {}", description, nv_pair))),
    }
}

#[test]
fn test_split_name_value_pair() {
    assert_eq!(
        Ok(("a".to_string(), "b".to_string())),
        split_name_value_pair("", "a=b")
    );
    assert_eq!(
        Ok(("a".to_string(), "b=c".to_string())),
        split_name_value_pair("", "a=b=c")
    );
    assert!(split_name_value_pair("", "a=").is_err());
    assert!(split_name_value_pair("", "a").is_err());
    assert!(split_name_value_pair("", "").is_err());
}
