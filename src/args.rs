use clap::{AppSettings, Arg, ArgGroup, Command};
use clap_complete::Shell;

pub const CMD_BROKERS: &str = "brokers";
pub const CMD_TOPICS: &str = "topics";
pub const CMD_GROUPS: &str = "groups";
pub const CMD_CONFIG: &str = "config";
pub const CMD_CONSUME: &str = "consume";
pub const CMD_PRODUCE: &str = "produce";
pub const CMD_OFFSETS: &str = "offsets";

pub const OP_LIST: &str = "list";
pub const OP_DESCRIBE: &str = "describe";
pub const OP_CREATE: &str = "create";
pub const OP_DELETE: &str = "delete";
pub const OP_ALTER: &str = "alter";

pub const ARG_COMPLETIONS: &str = "completions";
pub const ARG_BOOTSTRAP_SERVER: &str = "bootstrap-server";
pub const ARG_EXTRA_PROPERTIES: &str = "extra-properties";
pub const ARG_EXTRA_PROPERTIES_FILE: &str = "extra-properties-file";
pub const ARG_WITH_OFFSETS: &str = "with-offsets";
pub const ARG_TOPIC: &str = "topic";
pub const ARG_OFFSETS: &str = "offsets";
pub const ARG_EARLIEST: &str = "earliest";
pub const ARG_LATEST: &str = "latest";
pub const ARG_LAGS: &str = "lags";
pub const ARG_PARTITIONS: &str = "partitions";
pub const ARG_ALL_PARTITIONS: &str = "all-partitions";
pub const ARG_PARTITION: &str = "partition";
pub const ARG_REPLICATION: &str = "replication";
pub const ARG_CONSUMER_GROUP: &str = "consumer-group";
pub const ARG_FOLLOW: &str = "follow";
pub const ARG_TAIL: &str = "tail";
pub const ARG_COUNT: &str = "count";
pub const ARG_PAYLOAD_FILE: &str = "payload-file";
pub const ARG_KEY: &str = "key";
pub const ARG_KEY_FILE: &str = "key-file";
pub const ARG_HEADERS: &str = "headers";
pub const ARG_NO_HEADERS: &str = "no-headers";
pub const ARG_NO_TIMESTAMP: &str = "no-timestamp";
pub const ARG_NO_KEY: &str = "no-key";
pub const ARG_NO_PAYLOAD: &str = "no-payload";
pub const ARG_KEY_REGEX: &str = "key-regex";
pub const ARG_HEADER_REGEX: &str = "header-regex";
pub const ARG_TIMESTAMP_BEFORE: &str = "timestamp-before";
pub const ARG_TIMESTAMP_AFTER: &str = "timestamp-after";
pub const ARG_GET: &str = "get";
pub const ARG_SET: &str = "set";
pub const ARG_JSON_BATCH: &str = "json-batch";
pub const ARG_BATCH_SIZE: &str = "batch-size";
pub const ARG_TIMEOUT: &str = "timeout";
pub const ARG_INCLUDE_DEFAULTS: &str = "include-defaults";

fn add_global_args(app: Command) -> Command {
    let arg_servers: Arg = Arg::with_name(ARG_BOOTSTRAP_SERVER)
        .help("The bootstrap servers")
        .short('b')
        .long(ARG_BOOTSTRAP_SERVER)
        .value_name("SERVERS")
        .use_delimiter(true)
        .required(true);

    let arg_extra_properties = Arg::with_name(ARG_EXTRA_PROPERTIES)
        .help("Additional properties to pass to librdkafka")
        .short('x')
        .long(ARG_EXTRA_PROPERTIES)
        .value_name("PROPERTIES")
        .use_delimiter(true)
        .conflicts_with(ARG_EXTRA_PROPERTIES_FILE);

    let arg_extra_properties_file = Arg::with_name(ARG_EXTRA_PROPERTIES_FILE)
        .help("A file containing additional properties to pass to librdkafka")
        .long(ARG_EXTRA_PROPERTIES_FILE)
        .short('X')
        .value_name("PROPERTIES_FILE")
        .conflicts_with(ARG_EXTRA_PROPERTIES);

    let arg_timeout = Arg::with_name(ARG_TIMEOUT)
        .help("Timeout in milliseconds")
        .short('T')
        .long(ARG_TIMEOUT)
        .value_name("TIMEOUT")
        .default_value("3000");

    app.arg(&arg_servers)
        .arg(&arg_extra_properties)
        .arg(&arg_extra_properties_file)
        .arg(&arg_timeout)
}

pub fn create_cmd() -> Command<'static> {
    let arg_completions = Arg::new(ARG_COMPLETIONS)
        .help("Generate shell completions")
        .short('c')
        .long(ARG_COMPLETIONS)
        .value_parser(value_parser!(Shell))
        .value_name("SHELL");

    let arg_with_offsets = Arg::with_name(ARG_WITH_OFFSETS)
        .help("Include offsets")
        .long(ARG_WITH_OFFSETS);

    let arg_topic = Arg::with_name(ARG_TOPIC)
        .help("A topic name")
        .short('t')
        .long(ARG_TOPIC)
        .value_name("TOPIC");

    let arg_topic_partitions = Arg::with_name(ARG_PARTITIONS)
        .help("Number of partitions")
        .long(ARG_PARTITIONS)
        .short('p')
        .value_name("PARTITIONS");

    let arg_replication = Arg::with_name(ARG_REPLICATION)
        .help("Replication factor")
        .long(ARG_REPLICATION)
        .short('r')
        .value_name("REPLICATION");

    let arg_partitions = Arg::with_name(ARG_PARTITIONS)
        .help("A comma separated list of partition numbers")
        .short('p')
        .long(ARG_PARTITIONS)
        .use_delimiter(true)
        .value_name("PARTITIONS");

    let arg_partition = Arg::with_name(ARG_PARTITION)
        .help("A partition number")
        .short('p')
        .long(ARG_PARTITION)
        .value_name("PARTITION");

    let arg_all_partition = Arg::with_name(ARG_ALL_PARTITIONS)
        .help("Consume from all partitions")
        .long(ARG_ALL_PARTITIONS)
        .conflicts_with_all(&[ARG_PARTITIONS, ARG_TAIL]);

    let arg_offsets = Arg::with_name(ARG_OFFSETS)
        .help("A comma separated list of offsets")
        .short('o')
        .long(ARG_OFFSETS)
        .use_delimiter(true)
        .value_name("OFFSETS");

    let arg_group = Arg::with_name(ARG_CONSUMER_GROUP)
        .help("A consumer group")
        .short('g')
        .long(ARG_CONSUMER_GROUP)
        .value_name("GROUP_ID");

    let arg_follow = Arg::with_name(ARG_FOLLOW)
        .help("Follow the topic")
        .short('f')
        .long(ARG_FOLLOW)
        .conflicts_with(ARG_COUNT);

    let arg_tail = Arg::with_name(ARG_TAIL)
        .help("Read from the end")
        .long(ARG_TAIL)
        .value_name("COUNT")
        .conflicts_with_all(&[ARG_COUNT, ARG_OFFSETS]);

    let arg_count = Arg::with_name(ARG_COUNT)
        .help("Number of messages to read")
        .long(ARG_COUNT)
        .value_name("COUNT");

    let arg_payload_file = Arg::with_name(ARG_PAYLOAD_FILE)
        .help("A file containing the payload")
        .long(ARG_PAYLOAD_FILE)
        .value_name("PAYLOAD_FILE");

    let arg_key = Arg::with_name(ARG_KEY)
        .help("A key")
        .long(ARG_KEY)
        .value_name("KEY")
        .conflicts_with(ARG_KEY_FILE);

    let arg_key_file = Arg::with_name(ARG_KEY_FILE)
        .help("A file containing the key")
        .long(ARG_KEY_FILE)
        .value_name("KEY_FILE")
        .conflicts_with(ARG_KEY);

    let arg_headers = Arg::with_name(ARG_HEADERS)
        .help("Headers as comma separated key=value pairs")
        .long(ARG_HEADERS)
        .use_delimiter(true)
        .value_name("HEADERS");

    let arg_no_headers = Arg::with_name(ARG_NO_HEADERS)
        .help("Exclude headers")
        .long(ARG_NO_HEADERS);

    let arg_no_timestamp = Arg::with_name(ARG_NO_TIMESTAMP)
        .help("Exclude timestamp")
        .long(ARG_NO_TIMESTAMP);

    let arg_no_key = Arg::with_name(ARG_NO_KEY)
        .help("Exclude key")
        .long(ARG_NO_KEY);

    let arg_no_payload = Arg::with_name(ARG_NO_PAYLOAD)
        .help("Exclude payload")
        .long(ARG_NO_PAYLOAD);

    let arg_timestamp_before = Arg::with_name(ARG_TIMESTAMP_BEFORE)
        .help("filter messages with a timestamp before TIMESTAMP")
        .long(ARG_TIMESTAMP_BEFORE)
        .value_name("TIMESTAMP");

    let arg_timestamp_after = Arg::with_name(ARG_TIMESTAMP_AFTER)
        .help("filter messages with a timestamp after TIMESTAMP")
        .long(ARG_TIMESTAMP_AFTER)
        .value_name("TIMESTAMP");

    let arg_key_regex = Arg::with_name(ARG_KEY_REGEX)
        .help("Regular expression to filter messages by key")
        .long(ARG_KEY_REGEX)
        .value_name("REGEX");

    let arg_header_regex = Arg::with_name(ARG_HEADER_REGEX)
        .help("Comma separated list of regular expression to filter messages by headers (Format: <key-regex>=<value-regex>)")
        .value_delimiter(',')
        .long(ARG_HEADER_REGEX)
        .value_name("REGEX");

    let arg_get = Arg::with_name(ARG_GET)
        .help("Get configuration values")
        .long(ARG_GET)
        .min_values(0)
        .value_name("NAME");

    let arg_set = Arg::with_name(ARG_SET)
        .help("Set configuration values. Takes a list of comma separated name=value pairs")
        .long(ARG_SET)
        .value_delimiter(',')
        .value_name("CONFIG")
        .conflicts_with(ARG_GET)
        .requires_all(&[ARG_TOPIC]);

    let arg_include_defaults = Arg::with_name(ARG_INCLUDE_DEFAULTS)
        .help("Also show default values")
        .long(ARG_INCLUDE_DEFAULTS)
        .requires(ARG_GET);

    let arg_json_batch_consumer = Arg::with_name(ARG_JSON_BATCH)
        .long(ARG_JSON_BATCH)
        .conflicts_with(ARG_FOLLOW)
        .help("Consume all messages and serialize them as a batch of JSON messages");

    let arg_json_batch_producer = Arg::with_name(ARG_JSON_BATCH)
        .long(ARG_JSON_BATCH)
        .requires_all(&[ARG_PAYLOAD_FILE])
        .conflicts_with_all(&[ARG_HEADERS, ARG_KEY, ARG_KEY_FILE])
        .help("Treat the content of the payload file as a batch of serialized JSON messages including headers and keys");

    let arg_batch_size = Arg::with_name(ARG_BATCH_SIZE)
        .help("The count of messages that is sent to the broker in one batch")
        .long(ARG_BATCH_SIZE)
        .value_name("COUNT")
        .requires_all(&[ARG_JSON_BATCH]);

    let arg_offsets_offsets = Arg::with_name(ARG_OFFSETS)
        .help("A comma separated list of offsets")
        .short('o')
        .long(ARG_OFFSETS)
        .use_delimiter(true)
        .value_name("OFFSETS");

    let arg_earliest = Arg::with_name(ARG_EARLIEST)
        .help("Set offsets to earliest")
        .short('E')
        .long(ARG_EARLIEST)
        .conflicts_with(ARG_OFFSETS);

    let arg_latest = Arg::with_name(ARG_LATEST)
        .help("Set offsets to latest")
        .short('L')
        .long(ARG_LATEST)
        .conflicts_with(ARG_OFFSETS);

    let arg_lags = Arg::with_name(ARG_LAGS)
        .help("List lags instead of offsets")
        .short('l')
        .long(ARG_LAGS);

    Command::new("milena")
        .version(crate_version!())
        .propagate_version(false)
        .arg(arg_completions)
        .subcommand(add_global_args(
            Command::new(CMD_BROKERS).about("Display information about the brokers"),
        ))
        .subcommand(
            Command::new(CMD_TOPICS)
                .about("Manage topics")
                .setting(AppSettings::SubcommandRequiredElseHelp)
                .subcommand(add_global_args(
                    Command::new(OP_LIST).about("List topics").arg(&arg_topic),
                ))
                .subcommand(add_global_args(
                    Command::new(OP_DESCRIBE)
                        .about("Describe topics")
                        .arg(&arg_topic)
                        .arg(&arg_with_offsets),
                ))
                .subcommand(add_global_args(
                    Command::new(OP_CREATE)
                        .about("Create a topic")
                        .arg(arg_topic.clone().required(true))
                        .arg(&arg_topic_partitions)
                        .arg(&arg_replication),
                ))
                .subcommand(add_global_args(
                    Command::new(OP_DELETE)
                        .about("Delete a topic")
                        .arg(arg_topic.clone().required(true)),
                ))
                .subcommand(add_global_args(
                    Command::new(OP_ALTER)
                        .about("Alter a topic")
                        .arg(arg_topic.clone().required(true))
                        .arg(arg_topic_partitions),
                )),
        )
        .subcommand(
            Command::new(CMD_GROUPS)
                .about("Display and delete consumer groups")
                .setting(AppSettings::SubcommandRequiredElseHelp)
                .subcommand(add_global_args(
                    Command::new(OP_LIST).about("List groups").arg(&arg_group),
                ))
                .subcommand(add_global_args(
                    Command::new(OP_DELETE)
                        .about("Delete a group")
                        .arg(&arg_group),
                )),
        )
        .subcommand(
            Command::new(CMD_OFFSETS)
                .about("Display and alter offsets")
                .setting(AppSettings::SubcommandRequiredElseHelp)
                .subcommand(add_global_args(
                    Command::new(OP_LIST)
                        .about("List offsets")
                        .arg(&arg_topic)
                        .arg(&arg_group)
                        .arg(&arg_lags),
                ))
                .subcommand(add_global_args(
                    Command::new(OP_ALTER)
                        .about("Alter offsets")
                        .arg(arg_topic.clone().required(true))
                        .arg(arg_group.clone().required(true))
                        .arg(&arg_partitions)
                        .arg(&arg_offsets_offsets)
                        .arg(&arg_earliest)
                        .arg(&arg_latest)
                        .group(
                            ArgGroup::with_name("offset")
                                .arg(ARG_OFFSETS)
                                .arg(ARG_EARLIEST)
                                .arg(ARG_LATEST)
                                .required(true),
                        ),
                )),
        )
        .subcommand(add_global_args(
            Command::new(CMD_CONFIG)
                .about("Display and alter topic configuration")
                .arg(&arg_topic)
                .arg(&arg_set)
                .arg(&arg_get)
                .arg(&arg_include_defaults),
        ))
        .subcommand(add_global_args(
            Command::new(CMD_CONSUME)
                .about("Consume from a topic")
                .arg(arg_topic.clone().required(true))
                .arg(arg_partitions.clone().default_value("0"))
                .arg(&arg_all_partition)
                .arg(&arg_offsets)
                .arg(&arg_group)
                .arg(&arg_follow)
                .arg(&arg_tail)
                .arg(&arg_count)
                .arg(&arg_no_headers)
                .arg(&arg_no_timestamp)
                .arg(&arg_no_key)
                .arg(&arg_no_payload)
                .arg(&arg_timestamp_before)
                .arg(&arg_timestamp_after)
                .arg(&arg_key_regex)
                .arg(&arg_header_regex)
                .arg(&arg_json_batch_consumer)
                .arg(&arg_latest),
        ))
        .subcommand(
            add_global_args(
                Command::new(CMD_PRODUCE)
                    .about("Produce to a topic")
                    .arg(arg_topic.clone().required(true))
                    .arg(&arg_partition)
                    .arg(&arg_headers)
                    .arg(&arg_key)
                    .arg(&arg_key_file)
                    .arg(&arg_payload_file)
                    .arg(&arg_json_batch_producer),
            )
            .arg(&arg_batch_size),
        )
        .setting(AppSettings::ArgRequiredElseHelp)
}
