use clap::{App, AppSettings, Arg, ArgMatches, SubCommand};

pub const CMD_BROKERS: &'static str = "brokers";
pub const CMD_TOPICS: &'static str = "topics";
pub const CMD_GROUPS: &'static str = "groups";
pub const CMD_CONFIG: &'static str = "config";
pub const CMD_CONSUME: &'static str = "consume";
pub const CMD_PRODUCE: &'static str = "produce";
pub const CMD_OFFSETS: &'static str = "offsets";

pub const ARG_BOOTSTRAP_SERVER: &'static str = "bootstrap-server";
pub const ARG_EXTRA_PROPERTIES: &'static str = "extra-properties";
pub const ARG_EXTRA_PROPERTIES_FILE: &'static str = "extra-properties-file";
pub const ARG_WITH_OFFSETS: &'static str = "with-offsets";
pub const ARG_TOPIC: &'static str = "topic";
pub const ARG_OFFSETS: &'static str = "offsets";
pub const ARG_PARTITIONS: &'static str = "partitions";
pub const ARG_REPLICATION: &'static str = "replication";
pub const ARG_CONSUMER_GROUP: &'static str = "consumer-group";
pub const ARG_FOLLOW: &'static str = "follow";
pub const ARG_TAIL: &'static str = "tail";
pub const ARG_COUNT: &'static str = "count";
pub const ARG_LIST: &'static str = "list";
pub const ARG_DESCRIBE: &'static str = "describe";
pub const ARG_CREATE: &'static str = "create";
pub const ARG_DELETE: &'static str = "delete";
pub const ARG_ALTER: &'static str = "alter";
pub const ARG_PAYLOAD_FILE: &'static str = "payload-file";
pub const ARG_KEY: &'static str = "key";
pub const ARG_KEY_FILE: &'static str = "key-file";
pub const ARG_HEADERS: &'static str = "headers";
pub const ARG_NO_HEADERS: &'static str = "no-headers";
pub const ARG_GET: &'static str = "get";
pub const ARG_SET: &'static str = "set";
pub const ARG_JSON_BATCH: &'static str = "json-batch";

pub fn parse_args(args: &Vec<String>) -> ArgMatches {
    let arg_servers = Arg::with_name(ARG_BOOTSTRAP_SERVER)
        .help("The bootstrap servers")
        .short("b")
        .long(ARG_BOOTSTRAP_SERVER)
        .value_name("SERVERS")
        .use_delimiter(true)
        .required(true);

    let arg_extra_properties = Arg::with_name(ARG_EXTRA_PROPERTIES)
        .help("Additional properties to pass to librdkafka")
        .short("x")
        .long(ARG_EXTRA_PROPERTIES)
        .value_name("PROPERTIES")
        .use_delimiter(true)
        .conflicts_with(ARG_EXTRA_PROPERTIES_FILE);

    let arg_extra_properties_file = Arg::with_name(ARG_EXTRA_PROPERTIES_FILE)
        .help("A file containing additional properties to pass to librdkafka")
        .long(ARG_EXTRA_PROPERTIES_FILE)
        .short("X")
        .value_name("PROPERTIES_FILE")
        .conflicts_with(ARG_EXTRA_PROPERTIES);

    let arg_with_offsets = Arg::with_name(ARG_WITH_OFFSETS)
        .help("Include offsets")
        .long(ARG_WITH_OFFSETS)
        .conflicts_with_all(&[ARG_LIST, ARG_ALTER, ARG_CREATE, ARG_DELETE]);

    let arg_topic = Arg::with_name(ARG_TOPIC)
        .help("A topic name")
        .short("t")
        .long(ARG_TOPIC)
        .value_name("TOPIC");

    let arg_topic_partitions = Arg::with_name(ARG_PARTITIONS)
        .help("Number of partitions when altering a topic")
        .long(ARG_PARTITIONS)
        .short("p")
        .value_name("PARTITIONS");

    let arg_replication = Arg::with_name(ARG_REPLICATION)
        .help("Replication factor")
        .long(ARG_REPLICATION)
        .short("r")
        .value_name("REPLICATION");

    let arg_partitions = Arg::with_name(ARG_PARTITIONS)
        .help("A comma separated list of partition numbers (Default: 0)")
        .short("p")
        .long(ARG_PARTITIONS)
        .use_delimiter(true)
        .value_name("PARTITIONS");

    let arg_offsets = Arg::with_name(ARG_OFFSETS)
        .help("A comma separated list of offsets")
        .short("o")
        .long(ARG_OFFSETS)
        .use_delimiter(true)
        .value_name("OFFSETS");

    let arg_group = Arg::with_name(ARG_CONSUMER_GROUP)
        .help("A consumer group")
        .short("g")
        .long(ARG_CONSUMER_GROUP)
        .value_name("GROUP_ID");

    let arg_follow = Arg::with_name(ARG_FOLLOW)
        .help("Follow the topic")
        .short("f")
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

    let arg_get = Arg::with_name(ARG_GET)
        .help("Get configuration values")
        .long(ARG_GET)
        .min_values(0)
        .value_name("NAME");

    let arg_set = Arg::with_name(ARG_SET)
        .help("Set configuration values. Takes a list of comma separated name=value pairs")
        .long(ARG_SET)
        .value_delimiter(",")
        .value_name("CONFIG")
        .conflicts_with(ARG_GET)
        .requires_all(&[ARG_TOPIC]);

    let arg_json_batch = Arg::with_name(ARG_JSON_BATCH)
        .long(ARG_JSON_BATCH);

    let arg_offsets_partitions = Arg::with_name(ARG_PARTITIONS)
        .help("A comma separated list of partition numbers")
        .short("p")
        .long(ARG_PARTITIONS)
        .use_delimiter(true)
        .value_name("PARTITIONS");

    let arg_offsets_offsets = Arg::with_name(ARG_OFFSETS)
        .help("A comma separated list of offsets")
        .short("o")
        .long(ARG_OFFSETS)
        .use_delimiter(true)
        .value_name("OFFSETS")
        .required(true);

    return App::new("Milena")
        .version(crate_version!())
        .setting(AppSettings::GlobalVersion)
        .setting(AppSettings::VersionlessSubcommands)
        .subcommand(SubCommand::with_name(CMD_BROKERS)
            .about("Display information the brokers")
            .arg(&arg_servers)
            .arg(&arg_extra_properties)
            .arg(&arg_extra_properties_file))
        .subcommand(SubCommand::with_name(CMD_TOPICS)
            .about("Manage topics")
            .setting(AppSettings::SubcommandRequiredElseHelp)
            .subcommand(SubCommand::with_name(ARG_LIST)
                .about("List topics")
                .arg(&arg_servers)
                .arg(&arg_extra_properties)
                .arg(&arg_extra_properties_file)
                .arg(&arg_topic))
            .subcommand(SubCommand::with_name(ARG_DESCRIBE)
                .about("Describe topics")
                .arg(&arg_servers)
                .arg(&arg_extra_properties)
                .arg(&arg_extra_properties_file)
                .arg(&arg_topic)
                .arg(&arg_with_offsets))
            .subcommand(SubCommand::with_name(ARG_CREATE)
                .about("Create a topic")
                .arg(&arg_servers)
                .arg(&arg_extra_properties)
                .arg(&arg_extra_properties_file)
                .arg(&arg_topic.clone().required(true))
                .arg(&arg_topic_partitions)
                .arg(&arg_replication))
            .subcommand(SubCommand::with_name(ARG_DELETE)
                .about("Delete a topic")
                .arg(&arg_servers)
                .arg(&arg_extra_properties)
                .arg(&arg_extra_properties_file)
                .arg(&arg_topic.clone().required(true)))
            .subcommand(SubCommand::with_name(ARG_ALTER)
                .about("Alter a topic")
                .arg(&arg_servers)
                .arg(&arg_extra_properties)
                .arg(&arg_extra_properties_file)
                .arg(&arg_topic.clone().required(true))
                .arg(&arg_topic_partitions)))
        .subcommand(SubCommand::with_name(CMD_GROUPS)
            .about("Display information about groups")
            .arg(&arg_servers)
            .arg(&arg_extra_properties)
            .arg(&arg_extra_properties_file))
        .subcommand(SubCommand::with_name(CMD_OFFSETS)
            .about("Display and alter offsets")
            .setting(AppSettings::SubcommandRequiredElseHelp)
            .subcommand(SubCommand::with_name(ARG_LIST)
                .about("List offsets")
                .arg(&arg_servers)
                .arg(&arg_extra_properties)
                .arg(&arg_extra_properties_file)
                .arg(&arg_topic)
                .arg(&arg_group))
            .subcommand(SubCommand::with_name(ARG_ALTER)
                .about("Alter offsets")
                .arg(&arg_servers)
                .arg(&arg_extra_properties)
                .arg(&arg_extra_properties_file)
                .arg(&arg_topic.clone().required(true))
                .arg(&arg_group.clone().required(true))
                .arg(&arg_offsets_partitions)
                .arg(&arg_offsets_offsets)))
        .subcommand(SubCommand::with_name(CMD_CONFIG)
            .about("Display and alter topic configuration")
            .arg(&arg_servers)
            .arg(&arg_extra_properties)
            .arg(&arg_extra_properties_file)
            .arg(&arg_topic)
            .arg(&arg_set)
            .arg(&arg_get))
        .subcommand(SubCommand::with_name(CMD_CONSUME)
            .about("Consume from a topic")
            .arg(&arg_servers)
            .arg(&arg_extra_properties)
            .arg(&arg_extra_properties_file)
            .arg(&arg_topic.clone().required(true))
            .arg(&arg_partitions)
            .arg(&arg_offsets)
            .arg(&arg_group)
            .arg(&arg_follow)
            .arg(&arg_tail)
            .arg(&arg_count)
            .arg(&arg_no_headers)
            .arg(&arg_json_batch.clone()
                .conflicts_with(ARG_FOLLOW)
                .help("Consume all messages and serialize them as a batch of JSON messages")))
        .subcommand(SubCommand::with_name(CMD_PRODUCE)
            .about("Produce to a topic")
            .arg(&arg_servers)
            .arg(&arg_extra_properties)
            .arg(&arg_extra_properties_file)
            .arg(&arg_topic.clone().required(true))
            .arg(&arg_headers)
            .arg(&arg_key)
            .arg(&arg_key_file)
            .arg(&arg_payload_file)
            .arg(&arg_json_batch.clone()
                .help("Treat the content of the payload file as a batch of serialized JSON messages")))
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .get_matches_from(args);
}
