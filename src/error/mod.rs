use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("pipeline error: {context}")]
    Pipeline { context: &'static str },

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("task join error: {0}")]
    Join(#[from] tokio::task::JoinError),
}

impl Error {
    pub fn pipeline(context: &'static str) -> Self {
        Self::Pipeline { context }
    }
}
