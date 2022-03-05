use std::time::Duration;

#[derive(Debug, Copy, Clone)]
pub enum PollingMode {
    Continuous { period: Duration },
    Dynamic { period: Duration },
}
