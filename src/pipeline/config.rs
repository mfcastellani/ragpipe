use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone, Default)]
pub(crate) struct StageConfig {
    pub buffers: Arc<HashMap<&'static str, usize>>,
    pub concurrencies: Arc<HashMap<&'static str, usize>>,
}

impl StageConfig {
    pub fn buffer_for(&self, stage: &'static str, global: usize) -> usize {
        self.buffers.get(stage).copied().unwrap_or(global)
    }

    pub fn concurrency_for(&self, stage: &'static str) -> usize {
        self.concurrencies.get(stage).copied().unwrap_or(1)
    }
}

tokio::task_local! {
    pub(crate) static STAGE_CONFIG: StageConfig;
}
