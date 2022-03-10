use std::time::Duration;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
pub enum PollingMode {
    Continuous { period: Duration },
    Dynamic { period: Duration },
}
