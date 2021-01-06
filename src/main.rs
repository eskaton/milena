#[macro_use]
extern crate clap;

use std::borrow::{Borrow, Cow};
use std::cmp::max;
use std::env;
use std::fs::{read, read_to_string};
use std::time::Duration;

use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use clap::ArgMatches;
use futures::executor;
use num_integer::div_mod_floor;
use rdkafka::{ClientConfig, ClientContext, TopicPartitionList};
use rdkafka::admin::{AdminClient, AdminOptions, AlterConfig, ConfigResource, OwnedResourceSpecifier, ResourceSpecifier};
use rdkafka::client::{Client, DefaultClientContext};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::{KafkaResult, RDKafkaError};
use rdkafka::message::{BorrowedHeaders, BorrowedMessage, DeliveryResult, Headers, Message, OwnedHeaders};
use rdkafka::message::Timestamp::CreateTime;
use rdkafka::metadata::MetadataTopic;
use rdkafka::producer::{BaseProducer, BaseRecord, ProducerContext};
use rdkafka::topic_partition_list::Offset::Offset;
use rdkafka::types::RDKafkaType;
use serde::Serialize;
use serde_json::json;

use crate::args::{CMD_BROKERS, CMD_CONFIG, CMD_CONSUME, CMD_GROUPS, CMD_PRODUCE, CMD_TOPICS};
use crate::args::parse_args;
use crate::config::{BaseConfig, ConfigConfig, ConfigMode, ConsumeConfig, ProduceConfig, TopicConfig, TopicMode};

mod args;
mod config;

const DEFAULT_GROUP_ID: &'static str = "kafka-tool";

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
        let replicas = replicas_arr.iter().map(|r| *r).collect();
        let isr = isr_arr.iter().map(|r| *r).collect();

        Self { id, leader, replicas, isr, offsets: None }
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

#[derive(Debug, Serialize)]
enum TimestampType {
    CreateTime,
    LogAppendTime,
}

#[derive(Debug, Serialize)]
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
            _ => TimestampType::LogAppendTime
        };

        let millis = timestamp.to_millis().unwrap();
        let (secs, msecs) = div_mod_floor(millis, 1000);
        let naive_date_time = NaiveDateTime::from_timestamp(secs, msecs as u32 * 1_000_000);
        let time = Utc.from_utc_datetime(&naive_date_time);

        Self { timestamp_type, time }
    }
}

#[derive(Debug, Serialize)]
struct Header<'a> {
    key: &'a str,
    value: Cow<'a, str>,
}

impl<'a> Header<'a> {
    fn new(key: &'a str, value: Cow<'a, str>) -> Header<'a> {
        Self { key, value }
    }
}

#[derive(Debug, Serialize)]
struct ConsumedMessage<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    timestamp: Option<Timestamp>,
    #[serde(skip_serializing_if = "Option::is_none")]
    key: Option<Cow<'a, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    payload: Option<Cow<'a, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    headers: Option<Vec<Header<'a>>>,
}

impl<'a> ConsumedMessage<'a> {
    fn new(timestamp: Option<Timestamp>, key: Option<Cow<'a, str>>, payload: Option<Cow<'a, str>>, headers: Option<Vec<Header<'a>>>) -> ConsumedMessage<'a> {
        Self { key, timestamp, payload, headers }
    }
}

struct KeyContext {}

impl ClientContext for KeyContext {}

impl ProducerContext for KeyContext {
    type DeliveryOpaque = Box<Option<Vec<u8>>>;

    fn delivery(&self, delivery_result: &DeliveryResult, delivery_opaque: Self::DeliveryOpaque) {
        match delivery_result {
            Ok(_) => {
                match delivery_opaque.borrow() {
                    None => println!("Message successfully delivered"),
                    Some(key) => {
                        match String::from_utf8(key.to_vec()) {
                            Ok(str_key) => println!("Message with key '{}' successfully delivered",
                                                    escape_newlines(&str_key)),
                            Err(_) => println!("Message with key '{:?}' successfully delivered", key)
                        }
                    }
                }
            }
            Err(e) => println!("Failed to deliver message: {:?}", e)
        };
    }
}

fn escape_newlines(string: &String) -> String {
    string.replace("\r\n", "\\r\\n")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
}

fn create_consumer(config: &BaseConfig, consumer_group: Option<String>) -> BaseConsumer {
    let mut client_config = create_client_config(config);

    consumer_group.map(|g| client_config.set(&"group.id", g.as_str()));

    client_config.create().expect("Failed to create consumer")
}

fn create_producer(config: &BaseConfig) -> BaseProducer<KeyContext> {
    let client_config = create_client_config(config);

    client_config.create_with_context(KeyContext {})
        .expect("Failed to create producer")
}

fn create_admin_client(config: &BaseConfig) -> AdminClient<DefaultClientContext> {
    let client_config = create_client_config(config);

    client_config.create().expect("Failed to create admin client")
}

fn create_client(config: &BaseConfig) -> Client {
    let client_config = create_client_config(config);
    let native_config = client_config.create_native_config()
        .expect("Failed to create native config");
    let kafka_type = RDKafkaType::RD_KAFKA_PRODUCER;

    Client::new(&client_config, native_config, kafka_type, DefaultClientContext)
        .expect("Failed to create client")
}

fn create_client_config(config: &BaseConfig) -> ClientConfig {
    let servers: String = config.servers.join(",");
    let mut client_config: ClientConfig = ClientConfig::new();

    client_config.set(&"bootstrap.servers".to_string(), &servers);

    match &config.properties {
        Some(properties) => properties.iter().for_each(|p| {
            client_config.set(&*p.0, &*p.1);
        }),
        None => ()
    };

    client_config
}

fn cmd_config(matches: &ArgMatches) {
    let config = ConfigConfig::new(matches);
    let client = create_admin_client(&config.base);

    match &config.mode {
        ConfigMode::GET => config_get(&config, &client),
        ConfigMode::SET => config_set(&config, &client)
    }
}

fn config_get(config: &ConfigConfig, client: &AdminClient<DefaultClientContext>) {
    let options = AdminOptions::new();
    let configs = executor::block_on(get_configs(&config, client, &options));
    let topics_configs = configs.iter().map(|result| {
        let resource = result.as_ref().expect("Failed to get result");
        let topic = match &resource.specifier {
            OwnedResourceSpecifier::Topic(name) => name.to_string(),
            _ => panic!("Received configuration for unexpected resource")
        };
        let pattern = config.pattern.as_ref();
        let configs = resource.entries.iter()
            .filter(|entry| {
                match &pattern {
                    None => true,
                    Some(p) => entry.name.to_string().contains(*p)
                }
            })
            .map(|entry| {
                let name = entry.name.to_string();
                let value = entry.value.as_ref().map(|s| s.to_string()).unwrap_or("".to_string());

                ConfigValue::new(name, value)
            }).collect::<Vec<ConfigValue>>();

        TopicConfigs::new(topic, configs)
    }).collect::<Vec<TopicConfigs>>();

    println!("{}", json!(topics_configs).to_string())
}

async fn get_configs(config: &ConfigConfig, admin_client: &AdminClient<DefaultClientContext>, options: &AdminOptions) -> Vec<Result<ConfigResource, RDKafkaError>> {
    let client = create_client(&config.base);
    let metadata = client
        .fetch_metadata(None, Duration::from_millis(3000))
        .expect("Failed to fetch metadata");
    let topics: Vec<String> = match &config.topic {
        None => metadata.topics().iter().map(|m| m.name().to_string()).collect(),
        Some(topic) => metadata.topics().iter().filter(|t| t.name().eq(topic.as_str())).map(|m| m.name().to_string()).collect()
    };
    let requested_topics = topics.iter().map(|n| ResourceSpecifier::Topic(n)).collect::<Vec<ResourceSpecifier>>();

    admin_client.describe_configs(&requested_topics, &options)
        .await
        .expect("Failed to get topic configuration")
}

fn config_set(config: &ConfigConfig, client: &AdminClient<DefaultClientContext>) {
    let options = AdminOptions::new();
    let topic = config.topic.as_ref().unwrap();
    let mut alter_config = AlterConfig::new(ResourceSpecifier::Topic(topic.as_str()));

    config.values.as_ref().unwrap().iter().for_each(|pair| {
        alter_config.entries.insert(&pair.0, &pair.1);
    });

    let result = executor::block_on(client.alter_configs(&[alter_config], &options));

    match result {
        Ok(_) => println!("Configuration of topic {} altered", topic),
        Err(e) => println!("Failed to alter topic configuration: {}", e)
    }
}

fn cmd_brokers(matches: &ArgMatches) {
    let config = TopicConfig::new(matches);
    let client: Client = create_client(&config.base);
    let metadata = client
        .fetch_metadata(None, Duration::from_millis(3000))
        .expect("Failed to fetch metadata");
    let brokers: Vec<Broker> = metadata.brokers().iter()
        .map(|broker| Broker::new(broker.id(), broker.host().to_string(), broker.port()))
        .collect();

    println!("{}", json!(brokers).to_string())
}

fn cmd_topics(matches: &ArgMatches) {
    let config = TopicConfig::new(matches);
    let consumer: BaseConsumer = create_consumer(&config.base, None);
    let metadata = consumer
        .fetch_metadata(None, Duration::from_millis(3000))
        .expect("Failed to fetch metadata");
    let mut rec_topics = Vec::<Topic>::new();
    let topics: Vec<&MetadataTopic> = match config.topic {
        None => metadata.topics().iter().collect(),
        Some(topic) => metadata.topics().iter().filter(|t| t.name().eq(&topic)).collect()
    };

    for topic in topics {
        let mut rec_topic = Topic::new(topic.name().to_string());

        if config.mode == TopicMode::DESCRIBE {
            for partition in topic.partitions() {
                let mut rec_partition = Partition::new(partition.id(), partition.leader(), partition.replicas(), partition.isr());

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
        TopicMode::LIST => println!("{}", json!(rec_topics.iter().map(|t| t.name.as_str()).collect::<Vec<&str>>()).to_string()),
        TopicMode::DESCRIBE => println!("{}", json!(rec_topics).to_string())
    }
}

fn cmd_groups(matches: &ArgMatches) {
    let config = BaseConfig::new(matches);
    let client: BaseConsumer = create_consumer(&config, None);
    let metadata = client
        .fetch_group_list(None, Duration::from_millis(3000))
        .expect("Failed to fetch group metadata");

    metadata.groups().iter().for_each(|group| {
        println!("Group: {} protocol_type={} protocol={} state={}", group.name(), group.protocol_type(), group.protocol(), group.state());

        group.members().iter().for_each(|member|
            println!(" Member: {} host: {}", member.id(), member.client_host()))
    });
}

fn cmd_consume(matches: &ArgMatches) {
    let config = ConsumeConfig::new(matches);
    let consumer_group = Option::from(config.consumer_group.clone());
    let consumer: BaseConsumer = create_consumer(&config.base, consumer_group);
    let count = config.count.unwrap_or(0);
    let mut current_count = 0;
    let topic_partition = get_topic_partitions(&consumer, &config);

    match consumer.assign(&topic_partition) {
        Err(e) => panic!("Failed to assign partitions: {}", e),
        Ok(_) => ()
    }

    let timeout = match config.follow {
        true => None,
        false => Some(Duration::new(1, 0))
    };

    loop {
        if count > 0 && count == current_count {
            return;
        }

        match consumer.poll(timeout) {
            Some(result) => handle_fetch_result(&config, result),
            None => return
        }

        current_count = current_count + 1;
    }
}

fn get_topic_partitions(consumer: &BaseConsumer, config: &ConsumeConfig) -> TopicPartitionList {
    let offsets = &config.offsets;
    let tail = &config.tail;
    let topic = &config.topic;
    let partitions = &config.partitions;
    let mut topic_partition: TopicPartitionList = TopicPartitionList::new();

    if offsets.is_some() {
        config.partitions.iter().zip(offsets.as_ref().unwrap().iter()).for_each(|po| {
            let partition = *po.0;
            let offset = *po.1;
            let (low, high) = get_watermarks(consumer, topic, partition);

            if offset < low || offset > high {
                panic!("Invalid offset {} for partition {}. Must be in range [{}, {}]",
                       offset, partition, low, high)
            }

            topic_partition.add_partition_offset(&topic, partition, Offset(offset));
        });
    } else if tail.is_some() {
        partitions.iter().for_each(|p| {
            let (low, high) = get_watermarks(consumer, topic, *p);

            if high != -1 {
                let offset = Offset(max(low, high - tail.unwrap()));

                topic_partition.add_partition_offset(&topic, *p, offset);
            }
        });
    } else {
        config.partitions.iter().for_each(|p| {
            let (low, _high) = get_watermarks(consumer, topic, *p);

            topic_partition.add_partition_offset(&topic, *p, Offset(low));
        });
    }

    topic_partition
}

fn get_watermarks(consumer: &BaseConsumer, topic: &String, partition: i32) -> (i64, i64) {
    let (low, high) = consumer
        .fetch_watermarks(topic, partition, Duration::from_secs(1))
        .unwrap_or((-1, -1));
    (low, high)
}

fn handle_fetch_result(config: &ConsumeConfig, result: KafkaResult<BorrowedMessage>) {
    if result.is_err() {
        panic!("Poll failed with error: {}", result.err().unwrap())
    }

    let message = result.unwrap();
    let headers = match config.no_headers {
        true => None,
        false => message.headers().map(|h| get_headers(h))
    };
    let timestamp = match message.timestamp().to_millis() {
        Some(_) => Some(Timestamp::new(message.timestamp())),
        None => None
    };
    let key = message.key().map(|k| String::from_utf8_lossy(k));
    let payload = message.payload().map(|p| String::from_utf8_lossy(p));
    let print_message = ConsumedMessage::new(timestamp, key, payload, headers);

    println!("{}", json!(print_message).to_string());
}

fn get_headers(borrowed_headers: &BorrowedHeaders) -> Vec<Header> {
    let mut headers = Vec::<Header>::new();
    let count = borrowed_headers.count();

    for idx in 0..count {
        get_header(borrowed_headers, idx).map(|h| headers.push(h));
    }

    return headers;
}

fn get_header(borrowed_headers: &BorrowedHeaders, idx: usize) -> Option<Header> {
    borrowed_headers.get(idx).map(|h| Header::new(h.0, String::from_utf8_lossy(h.1)))
}

fn cmd_produce(matches: &ArgMatches) {
    let config = ProduceConfig::new(matches);
    let producer = create_producer(&config.base);
    let payload = &config.payload_file.as_ref().map(|f| read_to_string(f).unwrap());
    let mut headers = OwnedHeaders::new();

    for header in config.headers.iter() {
        headers = headers.add(&(*header).0.to_string(), &(*header).1.to_string());
    }

    let key = get_key(&config);
    let delivery_opaque = Box::from(key.clone());
    let mut record = BaseRecord::<Vec<u8>, String, Box<Option<Vec<u8>>>>::with_opaque_to(&config.topic, delivery_opaque).headers(headers);

    if key.is_some() {
        record = record.key(&key.as_ref().unwrap());
    }

    if payload.is_some() {
        record = record.payload(payload.as_ref().unwrap());
    };

    match producer.send(record) {
        Err(e) => panic!("{:?}", e),
        _ => ()
    }

    producer.flush(Duration::new(30, 0));
}

fn get_key(config: &ProduceConfig) -> Option<Vec<u8>> {
    if config.key_file.is_some() {
        let content = config.key_file.as_ref().map(|f| read(f)).unwrap();

        if content.is_err() {
            panic!("Failed to read key file");
        }

        Some(content.unwrap())
    } else if config.key.is_some() {
        Some(config.key.as_ref().unwrap().to_string().into_bytes())
    } else {
        None
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let matches = parse_args(&args);

    match matches.subcommand() {
        (CMD_BROKERS, Some(matches)) => cmd_brokers(&matches),
        (CMD_TOPICS, Some(matches)) => cmd_topics(&matches),
        (CMD_GROUPS, Some(matches)) => cmd_groups(&matches),
        (CMD_CONFIG, Some(matches)) => cmd_config(&matches),
        (CMD_CONSUME, Some(matches)) => cmd_consume(&matches),
        (CMD_PRODUCE, Some(matches)) => cmd_produce(&matches),
        _ => {}
    };
}
