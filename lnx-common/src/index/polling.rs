use std::time::Duration;
use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
pub enum PollingMode {
    Continuous { period: Duration },
    Dynamic { period: Duration },
}
