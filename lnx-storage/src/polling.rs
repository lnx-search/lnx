use std::time::Duration;

#[derive(Debug, Clone)]
pub enum PollingMode {
    Continuous { period: Duration },
    Dynamic { period: Duration },
}
