use crate::GenericError;
use crate::MilenaError::{ArgError, KafkaError};
use std::io::Error;

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

impl From<rdkafka::error::KafkaError> for MilenaError {
    fn from(error: rdkafka::error::KafkaError) -> Self {
        KafkaError(format!("{}", error))
    }
}

impl From<&rdkafka::error::RDKafkaError> for MilenaError {
    fn from(error: &rdkafka::error::RDKafkaError) -> Self {
        KafkaError(format!("{}", error))
    }
}

impl From<&rdkafka::error::KafkaError> for MilenaError {
    fn from(error: &rdkafka::error::KafkaError) -> Self {
        KafkaError(format!("{}", error))
    }
}

impl From<&rdkafka::error::RDKafkaErrorCode> for MilenaError {
    fn from(error: &rdkafka::error::RDKafkaErrorCode) -> Self {
        KafkaError(format!("{}", error))
    }
}

impl From<Error> for MilenaError {
    fn from(error: Error) -> Self {
        GenericError(format!("{}", error))
    }
}


pub type Result<T> = std::result::Result<T, MilenaError>;