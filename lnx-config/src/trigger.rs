#[doc(hidden)]
/// A trigger can be invoked when a config is changed.
pub struct Trigger {
    callback: Box<dyn Fn() + Send + Sync>,
}

impl Trigger {
    pub fn new<T>(trigger: T) -> Self
    where
        T: Fn() + Send + Sync + 'static,
    {
        Self {
            callback: Box::new(trigger),
        }
    }

    /// Trigger the callback.
    pub fn trigger(&self) {
        (self.callback)();
    }
}
