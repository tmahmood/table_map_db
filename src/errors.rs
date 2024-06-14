use thiserror::Error;

#[derive(Error, Debug, Clone)]
pub enum DataToolErrors {


    #[error("Error received: {0}")]
    GenericError(String),

    #[error("CSV Error: {0}")]
    CsvError(String),
}

impl From<csv::Error> for DataToolErrors {
    fn from(value: csv::Error) -> Self {
        Self::CsvError(value.to_string())
    }
}

impl From<std::io::Error> for DataToolErrors {
    fn from(value: std::io::Error) -> Self {
        Self::GenericError(value.to_string())
    }
}

