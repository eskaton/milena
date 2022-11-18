use rdkafka::error::RDKafkaError;
use crate::GenericError;
use crate::MilenaError::{ArgError, KafkaError};

#[derive(Debug, PartialEq, Eq)]
pub enum MilenaError {
    ArgError(String),
    KafkaError(String),
    GenericError(String),
}

impl Into<String> for MilenaError {
    fn into(self) -> String {
        match self {
            ArgError(s) => s,
            KafkaError(s) => s,
            GenericError(s) => s,
        }
    }
}

impl From<Vec<MilenaError>> for MilenaError {
    fn from(errors: Vec<MilenaError>) -> Self {
        let msg = errors.iter().map(|error| {
            match error {
                ArgError(msg) => msg.clone(),
                KafkaError(msg) => msg.clone(),
                GenericError(msg) => msg.clone(),
            }
        }).collect::<Vec<String>>().join("\n");

        GenericError(msg)
    }
}

impl From<rdkafka::error::KafkaError> for MilenaError  {
    fn from(error: rdkafka::error::KafkaError) -> Self {
        KafkaError(format!("{}", error))
    }
}

impl From<&RDKafkaError> for MilenaError  {
    fn from(error: &RDKafkaError) -> Self {
        KafkaError(format!("{}", error))
    }
}

impl From<&rdkafka::error::KafkaError> for MilenaError  {
    fn from(error: &rdkafka::error::KafkaError) -> Self {
        KafkaError(format!("{}", error))
    }
}

pub type Result<T> = std::result::Result<T, MilenaError>;