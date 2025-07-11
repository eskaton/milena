#[macro_use(crate_version, value_parser)]
extern crate clap;

use std::borrow::Cow;
use std::cmp::{max, PartialEq};
use std::collections::{HashMap, HashSet};
use std::fs::{read, read_to_string};
use std::time::Duration;
use std::{env, io};

use chrono::{DateTime, Utc};
use clap::{ArgMatches, Command};
use clap_complete::{generate, Shell};
use colorize::AnsiColor;
use futures::executor;
use num_integer::div_mod_floor;
use rdkafka::admin::TopicReplication::Fixed;
use rdkafka::admin::{
    AdminClient, AdminOptions, AlterConfig, ConfigResourceResult, NewPartitions, NewTopic,
    OwnedResourceSpecifier, ResourceSpecifier, TopicResult,
};
use rdkafka::client::{Client, DefaultClientContext};
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::error::KafkaResult;
use rdkafka::message::Timestamp::CreateTime;
use rdkafka::message::{
    BorrowedHeaders, BorrowedMessage, DeliveryResult, Headers, Message, OwnedHeaders,
};
use rdkafka::metadata::MetadataTopic;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer, ProducerContext};
use rdkafka::topic_partition_list::Offset::Offset;
use rdkafka::types::RDKafkaType;
use rdkafka::util::Timeout;
use rdkafka::{ClientConfig, ClientContext, TopicPartitionList};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::args::create_cmd;
use crate::args::{
    ARG_COMPLETIONS, CMD_BROKERS, CMD_CONFIG, CMD_CONSUME, CMD_GROUPS, CMD_OFFSETS, CMD_PRODUCE,
    CMD_TOPICS,
};
use crate::config::{
    BaseConfig, ConfigConfig, ConfigMode, ConfigType, ConsumeConfig, GroupConfig, GroupMode,
    OffsetMode, OffsetsConfig, ProduceConfig, TopicConfig, TopicMode, CONFIG_PROPERTIES,
};
use crate::error::{MilenaError, Result};
use crate::MilenaError::{ArgError, GenericError, KafkaError};

mod args;
mod config;
mod error;
mod utils;

const DEFAULT_GROUP_ID: &str = "milena";

enum OffsetPosition {
    Earliest,
    Latest,
}

#[derive(Debug, Serialize)]
struct Broker {
    id: i32,
    host: String,
    port: i32,
}

impl Broker {
    fn new(id: i32, host: String, port: i32) -> Self {
        Self { id, host, port }
    }
}

#[derive(Debug, Serialize)]
struct ConfigValue {
    name: String,
    value: String,
}

impl ConfigValue {
    fn new(name: String, value: String) -> Self {
        Self { name, value }
    }
}

#[derive(Debug, Serialize)]
struct TopicConfigs {
    name: String,
    configs: Vec<ConfigValue>,
}

impl TopicConfigs {
    fn new(name: String, configs: Vec<ConfigValue>) -> Self {
        Self { name, configs }
    }
}

#[derive(Debug, Serialize)]
struct Group {
    name: String,
    protocol_type: String,
    protocol: String,
    state: String,
    members: Vec<Member>,
}

impl Group {
    fn new(name: String, protocol_type: String, protocol: String, state: String) -> Self {
        let members = Vec::<Member>::new();

        Self {
            name,
            protocol_type,
            protocol,
            state,
            members,
        }
    }

    fn add_member(&mut self, member: Member) {
        self.members.push(member);
    }
}

#[derive(Debug, Serialize)]
struct Member {
    id: String,
    client_host: String,
}

impl Member {
    fn new(id: String, client_host: String) -> Self {
        Self { id, client_host }
    }
}

#[derive(Debug, Serialize)]
struct GroupOffsets {
    group: String,
    topics: Vec<TopicOffsets>,
}

impl GroupOffsets {
    fn new(group: String) -> Self {
        let topics = Vec::<TopicOffsets>::new();

        Self { group, topics }
    }

    fn add_topic_offsets(&mut self, topic: TopicOffsets) {
        self.topics.push(topic);
    }
}

#[derive(Debug, Serialize)]
struct TopicOffsets {
    name: String,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    offsets: HashMap<i32, i64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    lags: HashMap<i32, i64>,
}

impl TopicOffsets {
    fn new(name: String) -> Self {
        let offsets = HashMap::<i32, i64>::new();
        let lags = HashMap::<i32, i64>::new();

        Self {
            name,
            offsets,
            lags,
        }
    }

    fn add_offset(&mut self, partition: i32, offset: i64) {
        self.offsets.insert(partition, offset);
    }

    fn add_lag(&mut self, partition: i32, lag: i64) {
        self.lags.insert(partition, lag);
    }
}

#[derive(Debug, Serialize)]
struct Topic {
    name: String,
    partitions: Vec<Partition>,
}

impl Topic {
    fn new(name: String) -> Self {
        let partitions = Vec::<Partition>::new();

        Self { name, partitions }
    }

    fn add_partition(&mut self, partition: Partition) {
        self.partitions.push(partition);
    }
}

#[derive(Debug, Serialize)]
struct Partition {
    id: i32,
    leader: i32,
    replicas: Vec<i32>,
    #[serde(rename = "ISR")]
    isr: Vec<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    offsets: Option<Offsets>,
}

impl Partition {
    fn new(id: i32, leader: i32, replicas_arr: &[i32], isr_arr: &[i32]) -> Self {
        let replicas = replicas_arr.to_vec();
        let isr = isr_arr.to_vec();

        Self {
            id,
            leader,
            replicas,
            isr,
            offsets: None,
        }
    }

    fn add_offsets(&mut self, offsets: Offsets) {
        self.offsets = Option::from(offsets);
    }
}

#[derive(Debug, Serialize)]
struct Offsets {
    low: i64,
    high: i64,
}

impl Offsets {
    fn new(low: i64, high: i64) -> Self {
        Self { low, high }
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum TimestampType {
    CreateTime,
    LogAppendTime,
}

#[derive(Debug, Serialize, Deserialize)]
struct Timestamp {
    #[serde(rename = "type")]
    timestamp_type: TimestampType,
    time: DateTime<Utc>,
}

impl Timestamp {
    fn new(timestamp: rdkafka::Timestamp) -> Self {
        assert!(timestamp.to_millis().is_some());

        let timestamp_type = match timestamp {
            CreateTime(_) => TimestampType::CreateTime,
            _ => TimestampType::LogAppendTime,
        };

        let millis = timestamp.to_millis().unwrap();
        let (secs, msecs) = div_mod_floor(millis, 1000);
        let time = DateTime::from_timestamp(secs, msecs as u32 * 1_000_000).unwrap();

        Self {
            timestamp_type,
            time,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Header {
    key: String,
    value: String,
}

impl Header {
    fn new(key: String, value: String) -> Header {
        Self { key, value }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ConsumedMessage<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    timestamp: Option<Timestamp>,
    #[serde(skip_serializing_if = "Option::is_none")]
    key: Option<Cow<'a, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    payload: Option<Cow<'a, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    headers: Option<Vec<Header>>,
}

impl<'a> ConsumedMessage<'a> {
    fn new(
        timestamp: Option<Timestamp>,
        key: Option<Cow<'a, str>>,
        payload: Option<Cow<'a, str>>,
        headers: Option<Vec<Header>>,
    ) -> ConsumedMessage<'a> {
        Self {
            key,
            timestamp,
            payload,
            headers,
        }
    }
}

struct KeyContext {}

impl ClientContext for KeyContext {}

impl ProducerContext for KeyContext {
    type DeliveryOpaque = Box<Option<Vec<u8>>>;

    fn delivery(&self, delivery_result: &DeliveryResult, delivery_opaque: Self::DeliveryOpaque) {
        match delivery_result {
            Ok(_) => match delivery_opaque.as_ref() {
                None => println!("Message successfully delivered"),
                Some(key) => match String::from_utf8(key.to_vec()) {
                    Ok(str_key) => println!(
                        "Message with key '{}' successfully delivered",
                        escape_newlines(&str_key)
                    ),
                    Err(_) => println!("Message with key '{:?}' successfully delivered", key),
                },
            },
            Err(e) => eprintln!("{}", format!("Failed to deliver message: {:?}", e).red()),
        };
    }
}

fn escape_newlines(string: &str) -> String {
    string
        .replace("\r\n", "\\r\\n")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
}

fn create_consumer(config: &BaseConfig, consumer_group: Option<String>) -> Result<BaseConsumer> {
    let mut client_config = create_client_config(config, ConfigType::Consumer);

    consumer_group.map(|g| client_config.set("group.id", g.as_str()));

    let client = client_config
        .create::<BaseConsumer>()
        .map_err(MilenaError::from)?;

    Ok(client)
}

fn create_producer(config: &BaseConfig) -> Result<BaseProducer<KeyContext>> {
    let client_config = create_client_config(config, ConfigType::Consumer);
    let client = client_config
        .create_with_context(KeyContext {})
        .map_err(MilenaError::from)?;

    Ok(client)
}

fn create_admin_client(config: &BaseConfig) -> Result<AdminClient<DefaultClientContext>> {
    let client_config = create_client_config(config, ConfigType::Both);
    let client = client_config.create().map_err(MilenaError::from)?;

    Ok(client)
}

fn create_client(config: &BaseConfig) -> Result<Client> {
    let client_config = create_client_config(config, ConfigType::Both);
    let native_config = client_config
        .create_native_config()
        .map_err(MilenaError::from)?;
    let kafka_type = RDKafkaType::RD_KAFKA_PRODUCER;
    let client = Client::new(
        &client_config,
        native_config,
        kafka_type,
        DefaultClientContext,
    )
    .map_err(MilenaError::from)?;

    Ok(client)
}

fn create_client_config(config: &BaseConfig, config_type: ConfigType) -> ClientConfig {
    let servers: String = config.servers.join(",");
    let mut client_config: ClientConfig = ClientConfig::new();

    client_config.set("bootstrap.servers".to_string(), &servers);

    if config_type == ConfigType::Consumer {
        client_config.set("enable.auto.offset.store".to_string(), "false");
    }

    if let Some(properties) = &config.properties {
        properties.iter().for_each(|p| {
            let maybe_config_type = CONFIG_PROPERTIES.get(&*p.0);

            if maybe_config_type.is_none()
                || *maybe_config_type.unwrap() == config_type
                || *maybe_config_type.unwrap() == ConfigType::Both
            {
                client_config.set(&*p.0, &*p.1);
            }
        })
    };

    client_config
}

fn cmd_config(matches: &ArgMatches) -> Result<()> {
    let config = ConfigConfig::new(matches)?;
    let client = create_admin_client(&config.base)?;

    match &config.mode {
        ConfigMode::Get => config_get(&config, &client),
        ConfigMode::Set => config_set(&config, &client),
    }
}

fn config_get(config: &ConfigConfig, client: &AdminClient<DefaultClientContext>) -> Result<()> {
    let options = AdminOptions::new();
    let configs = executor::block_on(get_configs(config, client, &options))?;
    let topics_configs = configs
        .iter()
        .map(|result| {
            let resource = result.as_ref().map_err(MilenaError::from)?;
            let topic = match &resource.specifier {
                OwnedResourceSpecifier::Topic(name) => Ok(name.to_string()),
                _ => Err(GenericError(
                    "Received configuration for unexpected resource".to_string(),
                )),
            }?;
            let pattern = config.pattern.as_ref();
            let configs = resource
                .entries
                .iter()
                .filter(|entry| match &pattern {
                    None => true,
                    Some(p) => entry.name.to_string().contains(*p),
                })
                .filter(|entry| {
                    !matches!((entry.is_default, config.include_defaults), (true, false))
                })
                .map(|entry| {
                    let name = entry.name.to_string();
                    let value = entry
                        .value
                        .as_ref()
                        .map(|s| s.to_string())
                        .unwrap_or("".to_string());

                    ConfigValue::new(name, value)
                })
                .collect::<Vec<ConfigValue>>();

            Ok(TopicConfigs::new(topic, configs))
        })
        .collect::<Result<Vec<TopicConfigs>>>()?;

    println!("{}", json!(topics_configs));

    Ok(())
}

async fn get_configs(
    config: &ConfigConfig,
    admin_client: &AdminClient<DefaultClientContext>,
    options: &AdminOptions,
) -> Result<Vec<ConfigResourceResult>> {
    let client = create_client(&config.base)?;
    let metadata = client
        .fetch_metadata(None, config.base.timeout)
        .map_err(MilenaError::from)?;
    let topics: Vec<String> = match &config.topic {
        None => metadata
            .topics()
            .iter()
            .map(|m| m.name().to_string())
            .collect(),
        Some(topic) => metadata
            .topics()
            .iter()
            .filter(|t| t.name().eq(topic.as_str()))
            .map(|m| m.name().to_string())
            .collect(),
    };
    let requested_topics = topics
        .iter()
        .map(|n| ResourceSpecifier::Topic(n))
        .collect::<Vec<ResourceSpecifier>>();
    let result = admin_client
        .describe_configs(&requested_topics, options)
        .await
        .map_err(MilenaError::from)?;

    Ok(result)
}

fn config_set(config: &ConfigConfig, client: &AdminClient<DefaultClientContext>) -> Result<()> {
    let options = AdminOptions::new();
    let topic = config.topic.as_ref().unwrap();
    let mut alter_config = AlterConfig::new(ResourceSpecifier::Topic(topic.as_str()));

    config.values.as_ref().unwrap().iter().for_each(|pair| {
        alter_config.entries.insert(&pair.0, &pair.1);
    });

    let result = executor::block_on(client.alter_configs(&[alter_config], &options));

    match result {
        Ok(_) => println!("Configuration of topic {} altered", topic),
        Err(e) => return Err(MilenaError::from(e)),
    };

    Ok(())
}

fn cmd_brokers(matches: &ArgMatches) -> Result<()> {
    let config = BaseConfig::new(matches)?;
    let client: Client = create_client(&config)?;
    let metadata = client
        .fetch_metadata(None, config.timeout)
        .map_err(MilenaError::from)?;
    let brokers: Vec<Broker> = metadata
        .brokers()
        .iter()
        .map(|broker| Broker::new(broker.id(), broker.host().to_string(), broker.port()))
        .collect();

    println!("{}", json!(brokers));

    Ok(())
}

fn cmd_topics(matches: &ArgMatches) -> Result<()> {
    let config = TopicConfig::new(matches)?;

    match config.mode {
        TopicMode::Alter => alter_topic(&config),
        TopicMode::Create => create_topic(&config),
        TopicMode::Delete => delete_topic(&config),
        _ => show_topics(&config),
    }?;

    Ok(())
}

fn create_topic(config: &TopicConfig) -> Result<()> {
    let client = create_admin_client(&config.base)?;
    let topic = config.topic.as_ref().unwrap();
    let partitions = config.partitions.unwrap_or(1);
    let replication = config.replication.unwrap_or(1);
    let new_topic = NewTopic::new(topic, partitions, Fixed(replication));
    let options = AdminOptions::new();
    let result = executor::block_on(client.create_topics(&[new_topic], &options));

    evaluate_topic_result(topic, result, "created", "create")?;

    Ok(())
}

fn delete_topic(config: &TopicConfig) -> Result<()> {
    let client = create_admin_client(&config.base)?;
    let topic = config.topic.as_ref().unwrap();
    let options = AdminOptions::new();
    let result = executor::block_on(client.delete_topics(&[topic], &options));

    evaluate_topic_result(topic, result, "deleted", "delete")?;

    Ok(())
}

fn alter_topic(config: &TopicConfig) -> Result<()> {
    let client = create_admin_client(&config.base)?;
    let topic = config.topic.as_ref().unwrap();
    let options = AdminOptions::new();

    if config.partitions.is_some() {
        let partitions = config.partitions.unwrap();
        let new_partitions = NewPartitions::new(topic.as_str(), partitions as usize);
        let result = executor::block_on(client.create_partitions(&[new_partitions], &options));

        evaluate_topic_result(topic, result, "altered", "alter")?
    } else {
        println!("Topic {} unchanged. Please provide new settings", topic);
    }

    Ok(())
}

fn evaluate_topic_result(
    topic: &String,
    result: KafkaResult<Vec<TopicResult>>,
    done: &str,
    operation: &str,
) -> Result<()> {
    match result {
        Ok(results) => results
            .iter()
            .try_for_each(|topic_result| match topic_result {
                Ok(_) => Ok(println!("Topic '{}' {}", topic, done)),
                Err((_, e)) => Err(KafkaError(format!(
                    "Failed to {} topic '{}': {}",
                    operation, topic, e
                ))),
            }),
        Err(e) => Err(KafkaError(format!(
            "Failed to {} topic '{}': {}",
            operation, topic, e
        ))),
    }
}

fn show_topics(config: &TopicConfig) -> Result<()> {
    let consumer: BaseConsumer = create_consumer(&config.base, None)?;
    let metadata = consumer
        .fetch_metadata(None, config.base.timeout)
        .map_err(MilenaError::from)?;
    let mut rec_topics = Vec::<Topic>::new();
    let topics: Vec<&MetadataTopic> = match &config.topic {
        None => metadata.topics().iter().collect(),
        Some(topic) => metadata
            .topics()
            .iter()
            .filter(|t| t.name().eq(topic))
            .collect(),
    };

    if config.topic.is_some() && topics.is_empty() {
        return Err(GenericError(format!(
            "Topic {} does not exist",
            &config.topic.as_ref().unwrap()
        )));
    }

    for topic in topics {
        let mut rec_topic = Topic::new(topic.name().to_string());

        if config.mode == TopicMode::Describe {
            for partition in topic.partitions() {
                let mut rec_partition = Partition::new(
                    partition.id(),
                    partition.leader(),
                    partition.replicas(),
                    partition.isr(),
                );

                if config.with_offsets {
                    let (low, high) = consumer
                        .fetch_watermarks(topic.name(), partition.id(), Duration::from_secs(1))
                        .unwrap_or((-1, -1));

                    let rec_offsets = Offsets::new(low, high);

                    rec_partition.add_offsets(rec_offsets);
                }

                rec_topic.add_partition(rec_partition);
            }
        }

        rec_topics.push(rec_topic);
    }

    match config.mode {
        TopicMode::List => println!(
            "{}",
            json!(rec_topics
                .iter()
                .map(|t| t.name.as_str())
                .collect::<Vec<&str>>())
        ),
        TopicMode::Describe => println!("{}", json!(rec_topics)),
        _ => {}
    }

    Ok(())
}

fn cmd_groups(matches: &ArgMatches) -> Result<()> {
    let config = GroupConfig::new(matches)?;

    match config.mode {
        GroupMode::Delete => delete_group(&config),
        _ => show_groups(&config),
    }?;

    Ok(())
}

fn delete_group(config: &GroupConfig) -> Result<()> {
    let client = create_admin_client(&config.base)?;
    let consumer_group = config.consumer_group.as_ref().unwrap();
    let options = AdminOptions::new();
    let result = executor::block_on(client.delete_groups(&[consumer_group], &options));

    match result {
        Ok(results) => results
            .iter()
            .try_for_each(|group_result| match group_result {
                Ok(_) => Ok(println!("Group '{}' deleted", consumer_group)),
                Err((_, e)) => Err(KafkaError(format!(
                    "Failed to delete group '{}': {}",
                    consumer_group, e
                ))),
            }),
        Err(e) => Err(KafkaError(format!(
            "Failed to delete group '{}': {}",
            consumer_group, e
        ))),
    }
}

fn show_groups(config: &GroupConfig) -> Result<()> {
    let client: BaseConsumer = create_consumer(&config.base, None)?;

    client
        .fetch_metadata(None, config.base.timeout)
        .map_err(MilenaError::from)?;

    let group_list = client
        .fetch_group_list(config.consumer_group.as_deref(), config.base.timeout)
        .map_err(MilenaError::from)?;
    let mut rec_groups = Vec::<Group>::new();

    group_list.groups().iter().for_each(|group| {
        let mut rec_group = Group::new(
            group.name().to_string(),
            group.protocol_type().to_string(),
            group.protocol().to_string(),
            group.state().to_string(),
        );

        group.members().iter().for_each(|member| {
            rec_group.add_member(Member::new(
                member.id().to_string(),
                member.client_host().to_string(),
            ))
        });

        rec_groups.push(rec_group)
    });

    println!("{}", json!(rec_groups));

    Ok(())
}

fn cmd_offsets(matches: &ArgMatches) -> Result<()> {
    let config = OffsetsConfig::new(matches)?;

    match config.mode {
        OffsetMode::Alter => alter_offsets(&config),
        _ => show_offsets(&config),
    }?;

    Ok(())
}

fn show_offsets(config: &OffsetsConfig) -> Result<()> {
    let client: BaseConsumer = create_consumer(&config.base, None)?;
    let metadata = client
        .fetch_metadata(config.topic.as_deref(), config.base.timeout)
        .map_err(MilenaError::from)?;
    let group_list = client
        .fetch_group_list(config.consumer_group.as_deref(), config.base.timeout)
        .map_err(MilenaError::from)?;
    let groups: Vec<String> = group_list
        .groups()
        .iter()
        .map(|group| group.name().to_string())
        .collect();
    let topics: Vec<&MetadataTopic> = metadata.topics().iter().collect();
    let mut rec_group_offsets = Vec::<GroupOffsets>::new();

    for group in groups {
        let consumer: BaseConsumer = create_consumer(&config.base, Some(group.to_string()))?;
        let mut topic_partitions: TopicPartitionList = TopicPartitionList::new();
        let mut group_offsets = GroupOffsets::new(group.to_string());

        topics.iter().for_each(|topic| {
            topic.partitions().iter().for_each(|partition| {
                topic_partitions.add_partition(topic.name(), partition.id());
            });
        });

        let offsets = consumer
            .committed_offsets(topic_partitions, config.base.timeout)
            .map_err(MilenaError::from)?;
        let available_topics: HashSet<_> = offsets
            .elements()
            .iter()
            .map(|elem| elem.topic().to_string())
            .collect();

        for topic_name in available_topics {
            let mut topic_offsets = TopicOffsets::new(topic_name.to_string());

            offsets
                .elements_for_topic(topic_name.as_str())
                .iter()
                .for_each(|elem| {
                    if let Offset(offset) = elem.offset() {
                        if config.lags {
                            let (_, high) = consumer
                                .fetch_watermarks(
                                    topic_name.as_str(),
                                    elem.partition(),
                                    Duration::from_secs(1),
                                )
                                .unwrap_or((-1, -1));

                            if high != -1 {
                                topic_offsets.add_lag(elem.partition(), high - offset)
                            }
                        } else {
                            topic_offsets.add_offset(elem.partition(), offset)
                        }
                    }
                });
            if !topic_offsets.offsets.is_empty() || !topic_offsets.lags.is_empty() {
                group_offsets.add_topic_offsets(topic_offsets)
            }
        }

        if !group_offsets.topics.is_empty() {
            rec_group_offsets.push(group_offsets)
        }
    }

    println!("{}", json!(rec_group_offsets));

    Ok(())
}

fn alter_offsets(config: &OffsetsConfig) -> Result<()> {
    let client: BaseConsumer = create_consumer(
        &config.base,
        Some(config.consumer_group.as_ref().unwrap().to_string()),
    )?;
    let mut topic_partitions: TopicPartitionList = TopicPartitionList::new();
    let topic = config.topic.as_ref().unwrap();
    let earliest = config.earliest;
    let latest = config.latest;
    let metadata = client
        .fetch_metadata(Some(topic.as_str()), config.base.timeout)
        .map_err(MilenaError::from)?;
    let metadata_partitions: HashSet<i32> = metadata
        .topics()
        .iter()
        .flat_map(|t| t.partitions().iter().map(|p| p.id()))
        .collect();

    if config.partitions.as_ref().is_some() {
        let partitions = config.partitions.as_ref().unwrap();

        if earliest {
            for partition in partitions {
                add_offset(
                    &client,
                    &mut topic_partitions,
                    topic,
                    *partition,
                    OffsetPosition::Earliest,
                )?;
            }
        } else if latest {
            for partition in partitions {
                add_offset(
                    &client,
                    &mut topic_partitions,
                    topic,
                    *partition,
                    OffsetPosition::Latest,
                )?;
            }
        } else {
            let offsets = config.offsets.as_ref().unwrap();

            if partitions.len() != offsets.len() {
                return Err(GenericError(format!(
                    "Number of provided partitions ({}) doesn't match number of offsets ({})",
                    partitions.len(),
                    offsets.len()
                )));
            }

            let partition_set = HashSet::from_iter(partitions.iter().cloned());
            let difference: HashSet<_> = partition_set.difference(&metadata_partitions).collect();

            if !difference.is_empty() {
                return Err(GenericError(format!(
                    "Invalid partitions: {:?}",
                    difference
                )));
            }

            partitions
                .iter()
                .zip(offsets.iter())
                .try_for_each(|(p, o)| {
                    topic_partitions.add_partition_offset(topic.as_str(), *p, Offset(*o))
                })?;
        }
    } else if earliest {
        for partition in metadata_partitions {
            add_offset(
                &client,
                &mut topic_partitions,
                topic,
                partition,
                OffsetPosition::Earliest,
            )?;
        }
    } else if latest {
        for partition in metadata_partitions {
            add_offset(
                &client,
                &mut topic_partitions,
                topic,
                partition,
                OffsetPosition::Latest,
            )?;
        }
    } else {
        let offsets = config.offsets.as_ref().unwrap();

        if metadata_partitions.len() != offsets.len() {
            return Err(GenericError(format!(
                "Number of offsets ({}) doesn't match number of partitions ({})",
                offsets.len(),
                metadata_partitions.len()
            )));
        }

        (0i32..offsets.len() as i32)
            .zip(offsets.iter())
            .try_for_each(|(p, o)| {
                topic_partitions.add_partition_offset(topic.as_str(), p, Offset(*o))
            })?
    }

    client
        .assign(&topic_partitions)
        .map_err(MilenaError::from)?;
    client
        .store_offsets(&topic_partitions)
        .map_err(MilenaError::from)?;
    client
        .commit(&topic_partitions, CommitMode::Sync)
        .map_err(MilenaError::from)?;

    Ok(())
}

fn add_offset(
    client: &BaseConsumer,
    topic_partitions: &mut TopicPartitionList,
    topic: &str,
    partition: i32,
    position: OffsetPosition,
) -> KafkaResult<()> {
    let (low, high) = get_watermarks(client, topic, partition).unwrap();

    topic_partitions.add_partition_offset(
        topic,
        partition,
        Offset(match position {
            OffsetPosition::Earliest => low,
            OffsetPosition::Latest => high,
        }),
    )
}

fn cmd_consume(matches: &ArgMatches) -> Result<()> {
    let config = ConsumeConfig::new(matches)?;
    let consumer_group = Option::from(config.consumer_group.clone());
    let consumer: BaseConsumer = create_consumer(&config.base, consumer_group)?;
    let mut count = config.count.unwrap_or(0);
    let json_batch = config.json_batch;
    let follow = config.follow;
    let mut current_count = 0;
    let topic_partition = get_topic_partitions(&consumer, &config)?;
    let mut messages = Vec::<ConsumedMessage>::new();

    if let Err(e) = consumer.assign(&topic_partition) {
        return Err(MilenaError::from(e));
    }

    let timeout = match follow {
        true => None,
        false => Some(Duration::new(1, 0)),
    };

    if config.tail.is_some() {
        count = config.tail.unwrap() as usize;
    }

    loop {
        if count > 0 && count == current_count && !follow {
            break;
        }

        match consumer.poll(timeout) {
            Some(result) => {
                let opt_message = handle_fetch_result(&config, &result)?;

                if opt_message.is_none() {
                    continue;
                }

                let message = opt_message.unwrap();

                if json_batch {
                    let key = message.key.map(|s| Cow::from(Cow::into_owned(s)));
                    let payload = message.payload.map(|s| Cow::from(Cow::into_owned(s)));

                    messages.push(ConsumedMessage::new(
                        message.timestamp,
                        key,
                        payload,
                        message.headers,
                    ));
                } else {
                    println!("{}", json!(message));
                }
            }
            None => break,
        }

        current_count += 1;
    }

    if json_batch {
        println!("{}", json!(messages));
    }

    Ok(())
}

fn get_topic_partitions(
    consumer: &BaseConsumer,
    config: &ConsumeConfig,
) -> Result<TopicPartitionList> {
    let offsets = &config.offsets;
    let latest = config.latest;
    let tail = &config.tail;
    let topic = &config.topic;
    let mut partitions = config.partitions.clone();
    let all_partitions = config.all_partitions;
    let mut topic_partition: TopicPartitionList = TopicPartitionList::new();

    if offsets.is_some() {
        for (&partition, &offset) in config
            .partitions
            .iter()
            .zip(offsets.as_ref().unwrap().iter())
        {
            let (low, high) = get_watermarks(consumer, topic, partition)?;

            if offset < low || offset > high {
                return Err(GenericError(format!(
                    "Invalid offset {} for partition {}. Must be in range [{}, {}]",
                    offset, partition, low, high
                )));
            }

            topic_partition.add_partition_offset(topic, partition, Offset(offset))?;
        }
    } else if latest {
        if all_partitions {
            partitions = get_all_partitions(consumer, config, topic)?;
        }

        for partition in partitions {
            let (low, high) = get_watermarks(consumer, topic, partition)?;

            if high != -1 {
                let offset = Offset(max(low, high));

                topic_partition.add_partition_offset(topic, partition, offset)?;
            }
        }
    } else if tail.is_some() {
        for partition in partitions {
            let (low, high) = get_watermarks(consumer, topic, partition)?;

            if high != -1 {
                let offset = Offset(max(low, high - tail.unwrap()));

                topic_partition.add_partition_offset(topic, partition, offset)?;
            }
        }
    } else {
        if all_partitions {
            partitions = get_all_partitions(consumer, config, topic)?;
        }

        for partition in partitions {
            let (low, _high) = get_watermarks(consumer, topic, partition)?;

            topic_partition.add_partition_offset(topic, partition, Offset(low))?;
        }
    }

    Ok(topic_partition)
}

fn get_all_partitions(consumer: &BaseConsumer, config: &ConsumeConfig, topic: &String) -> Result<Vec<i32>> {
    let metadata = consumer
        .fetch_metadata(Some(topic), config.base.timeout)
        .map_err(MilenaError::from)?;
    let topics = metadata
        .topics()
        .iter()
        .filter(|t| t.name().eq(topic))
        .collect::<Vec<&MetadataTopic>>();
    let topics_meta = topics.first().unwrap();

    Ok(topics_meta
        .partitions()
        .iter()
        .map(|mdp| mdp.id())
        .collect::<Vec<i32>>())
}

fn get_watermarks(consumer: &BaseConsumer, topic: &str, partition: i32) -> Result<(i64, i64)> {
    let result = consumer.fetch_watermarks(topic, partition, Duration::from_secs(1));

    match result {
        Ok(result) => Ok(result),
        Err(e) => Err(KafkaError(format!("{}", e))),
    }
}

fn handle_fetch_result<'a>(
    config: &ConsumeConfig,
    result: &'a KafkaResult<BorrowedMessage>,
) -> Result<Option<ConsumedMessage<'a>>> {
    result.as_ref().map_err(MilenaError::from)?;

    let message = result.as_ref()?;
    let headers = match config.no_headers {
        true => None,
        false => message.headers().map(get_headers),
    };
    let timestamp = match config.no_timestamp {
        true => None,
        false => message
            .timestamp()
            .to_millis()
            .map(|_| Timestamp::new(message.timestamp())),
    };

    if let Some(config_timestamp) = config.timestamp_before {
        if timestamp.is_none() {
            return Ok(None);
        } else if config_timestamp
            <= timestamp
                .as_ref()
                .unwrap()
                .time
                .timestamp_nanos_opt()
                .unwrap()
        {
            return Ok(None);
        }
    }

    if config.timestamp_after.is_some() {
        if timestamp.is_none() {
            return Ok(None);
        } else if config.timestamp_after.unwrap()
            >= timestamp
                .as_ref()
                .unwrap()
                .time
                .timestamp_nanos_opt()
                .unwrap()
        {
            return Ok(None);
        }
    }

    let key = match config.no_key {
        true => None,
        false => message.key().map(|k| String::from_utf8_lossy(k)),
    };

    if config.key_regex.is_some() {
        if key.is_none() {
            return Ok(None);
        } else if !config
            .key_regex
            .as_ref()
            .unwrap()
            .is_match(key.as_ref().unwrap().as_ref())
        {
            return Ok(None);
        }
    }

    if config.header_regexes.is_some() {
        if headers.is_none() {
            return Ok(None);
        } else {
            let mut matched = 0usize;

            config
                .header_regexes
                .as_ref()
                .unwrap()
                .iter()
                .for_each(|(key_regex, value_regex)| {
                    if headers
                        .as_ref()
                        .unwrap()
                        .iter()
                        .filter(|h| {
                            key_regex.is_match(h.key.as_str())
                                && value_regex.is_match(h.value.as_str())
                        })
                        .count()
                        > 0usize
                    {
                        matched += 1;
                    }
                });

            if matched != config.header_regexes.as_ref().unwrap().len() {
                return Ok(None);
            }
        }
    }

    let payload = match config.no_payload {
        true => None,
        false => message.payload().map(|p| String::from_utf8_lossy(p)),
    };

    Ok(Some(ConsumedMessage::new(timestamp, key, payload, headers)))
}

fn get_headers(borrowed_headers: &BorrowedHeaders) -> Vec<Header> {
    let mut headers = Vec::<Header>::new();
    let count = borrowed_headers.count();

    for idx in 0..count {
        if let Some(h) = get_header(borrowed_headers, idx) {
            headers.push(h)
        }
    }

    headers
}

fn get_header(borrowed_headers: &BorrowedHeaders, idx: usize) -> Option<Header> {
    let kafka_header = borrowed_headers.get(idx);

    Some(Header::new(
        kafka_header.key.to_string(),
        String::from_utf8_lossy(kafka_header.value.unwrap_or(&[])).to_string(),
    ))
}

fn cmd_produce(matches: &ArgMatches) -> Result<()> {
    let config = ProduceConfig::new(matches)?;
    let producer = create_producer(&config.base)?;
    let payload = &config
        .payload_file
        .as_ref()
        .map(|f| read_to_string(f).unwrap());
    let json_batch = config.json_batch;
    let batch_size = config.batch_size.unwrap_or(128);

    if json_batch && payload.is_some() {
        let messages: Vec<ConsumedMessage> =
            serde_json::from_str(payload.as_ref().unwrap().as_str()).unwrap();
        let mut count: usize = 0;

        for message in messages.iter() {
            let msg_payload = message.payload.as_ref().map(|s| s.to_string());
            let msg_headers = message.headers.as_ref();
            let msg_key = message.key.as_ref().map(|s| s.as_bytes().to_owned());
            let mut headers = OwnedHeaders::new();

            if msg_headers.is_some() {
                for header in msg_headers.unwrap() {
                    headers = headers.insert(rdkafka::message::Header {
                        key: header.key.as_str(),
                        value: Some(header.value.as_str()),
                    });
                }
            }

            send_message(&config, &producer, &msg_payload, &msg_key, &headers)?;

            count += 1;

            if count % batch_size == 0 {
                producer.flush(Timeout::After(Duration::new(30, 0)))?;
            }
        }
    } else {
        let mut headers = OwnedHeaders::new();

        for header in config.headers.iter() {
            headers = headers.insert(rdkafka::message::Header {
                key: header.0.as_str(),
                value: Some(header.1.as_str()),
            });
        }

        let key = get_key(&config)?;
        send_message(&config, &producer, payload, &key, &headers)?;
    }

    producer.flush(Timeout::After(Duration::new(30, 0)))?;

    Ok(())
}

fn send_message(
    config: &ProduceConfig,
    producer: &BaseProducer<KeyContext>,
    payload: &Option<String>,
    key: &Option<Vec<u8>>,
    headers: &OwnedHeaders,
) -> Result<()> {
    let delivery_opaque = Box::from(key.clone());
    let mut record = BaseRecord::<Vec<u8>, String, Box<Option<Vec<u8>>>>::with_opaque_to(
        &config.topic,
        delivery_opaque,
    )
    .headers(headers.clone());

    if config.partition.is_some() {
        record = record.partition(config.partition.unwrap());
    }

    if key.is_some() {
        record = record.key(key.as_ref().unwrap());
    }

    if payload.is_some() {
        record = record.payload(payload.as_ref().unwrap());
    };

    producer
        .send(record)
        .map_err(|(e, _)| MilenaError::from(e))?;

    Ok(())
}

fn get_key(config: &ProduceConfig) -> Result<Option<Vec<u8>>> {
    if config.key_file.is_some() {
        let content = config
            .key_file
            .as_ref()
            .map(read)
            .unwrap()
            .map_err(|e| GenericError(format!("Failed to read key file: {}", e)))?;

        Ok(Some(content))
    } else if config.key.is_some() {
        Ok(Some(config.key.as_ref().unwrap().to_string().into_bytes()))
    } else {
        Ok(None)
    }
}

fn cmd_completions(generator: &Shell, cmd: &mut Command) {
    generate(
        *generator,
        cmd,
        cmd.get_name().to_string(),
        &mut io::stdout(),
    );
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let matches = create_cmd().get_matches_from(&args);
    let generator = matches.get_one::<Shell>(ARG_COMPLETIONS);

    if let Some(gen) = generator {
        cmd_completions(gen, &mut create_cmd())
    }

    let result = match matches.subcommand() {
        Some((CMD_BROKERS, matches)) => cmd_brokers(matches),
        Some((CMD_TOPICS, matches)) => cmd_topics(matches),
        Some((CMD_GROUPS, matches)) => cmd_groups(matches),
        Some((CMD_OFFSETS, matches)) => cmd_offsets(matches),
        Some((CMD_CONFIG, matches)) => cmd_config(matches),
        Some((CMD_CONSUME, matches)) => cmd_consume(matches),
        Some((CMD_PRODUCE, matches)) => cmd_produce(matches),
        _ => Ok(()),
    };

    if let Err(GenericError(error)) = result {
        eprintln!("{}", error.as_str().to_string().red());
    } else if let Err(KafkaError(error)) = result {
        eprintln!("{}", error.as_str().to_string().red());
    } else if let Err(ArgError(error)) = result {
        eprintln!("{}", error.as_str().to_string().red());
    }
}
