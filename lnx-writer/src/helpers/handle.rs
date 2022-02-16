use tokio::task::JoinHandle;

pub struct CancellingJoinHandle<T>(pub Option<JoinHandle<T>>);

impl<T> Drop for CancellingJoinHandle<T> {
    fn drop(&mut self) {
        if let Some(handle) = self.0.take() {
            handle.abort();
        }
    }
}

impl<T> From<JoinHandle<T>> for CancellingJoinHandle<T> {
    fn from(h: JoinHandle<T>) -> Self {
        Self(Some(h))
    }
}
