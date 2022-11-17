use crate::GenericError;
use crate::MilenaError::ArgError;

#[derive(Debug, PartialEq, Eq)]
pub enum MilenaError {
    ArgError(String),
    GenericError(String),
}

impl Into<String> for MilenaError {
    fn into(self) -> String {
        match self {
            ArgError(s) => s,
            GenericError(s) => s
        }
    }
}

impl From<Vec<MilenaError>> for MilenaError {
    fn from(errors: Vec<MilenaError>) -> Self {
        let msg = errors.iter().map(|error| {
            match error {
                ArgError(msg) => msg.clone(),
                GenericError(msg) => msg.clone()
            }
        }).collect::<Vec<String>>().join("\n");

        GenericError(msg)
    }
}

pub type Result<T> = std::result::Result<T, MilenaError>;