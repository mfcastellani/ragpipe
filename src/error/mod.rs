use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[non_exhaustive]
#[derive(Error, Debug)]
pub enum Error {
    #[error("pipeline error: {context}")]
    Pipeline { context: &'static str },

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("task join error: {0}")]
    Join(#[from] tokio::task::JoinError),

    // Leaf error used when we only have a string message.
    #[error("{0}")]
    Message(String),

    #[error("stage `{stage}` error: {source}")]
    Stage {
        stage: &'static str,
        #[source]
        source: Box<Error>,
    },

    #[error("stage `{stage}` exhausted retries after {attempts} attempts: {source}")]
    RetryExhausted {
        stage: &'static str,
        attempts: u32,
        #[source]
        source: Box<Error>,
    },
}

impl Error {
    pub fn pipeline(context: &'static str) -> Self {
        Self::Pipeline { context }
    }

    /// Attach a simple message as the cause of a stage error.
    pub fn stage(stage: &'static str, message: impl Into<String>) -> Self {
        Self::Stage {
            stage,
            source: Box::new(Self::Message(message.into())),
        }
    }

    /// Attach an existing `Error` as the cause of a stage error.
    pub fn stage_source(stage: &'static str, source: Error) -> Self {
        match source {
            // Already stage-contextualized; avoid double wrapping.
            Error::Stage { .. } | Error::RetryExhausted { .. } => source,
            other => Self::Stage {
                stage,
                source: Box::new(other),
            },
        }
    }

    pub fn retry_exhausted(stage: &'static str, attempts: u32, source: Error) -> Self {
        Self::RetryExhausted {
            stage,
            attempts,
            source: Box::new(source),
        }
    }
}
