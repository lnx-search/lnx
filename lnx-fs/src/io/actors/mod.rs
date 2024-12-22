use std::fmt::Debug;
use std::io;
use std::io::ErrorKind;

use async_trait::async_trait;
use tracing::error;

mod tablet_reader;
mod tablet_writer;

pub use self::tablet_reader::{Positions, TabletReader, TabletReaderOptions};
pub use self::tablet_writer::{
    TabletWriter,
    TabletWriterOptions,
    WriterEventHook,
    WriterResponse,
};

#[cfg_attr(test, mockall::automock)]
#[async_trait(?Send)]
/// A factory that creates actor tasks from within the context
/// of a glommio runtime.
pub trait ActorFactory: Send {
    /// Spawns an actor instance with the pre-configured state in factory.
    async fn spawn_actor(self) -> io::Result<()>;
}

#[derive(Debug)]
/// Metadata relating to the blob being written.
pub struct Metadata {
    /// The file path for the blob.
    pub path: String,
    /// The transaction ID attached to the write event if applicable.
    pub transaction_id: Option<ulid::Ulid>,
}

fn writer_closed<E>(_err: E) -> io::Error {
    io::Error::new(ErrorKind::Interrupted, "Writer closed or aborted")
}

fn writer_controller_bug_log<E>(_err: E) -> io::Error {
    error!(
        "LIKELY BUG DETECTED: Controller checked to create writers but writer \
            channel still closed, system cannot progress"
    );
    io::Error::new(
        ErrorKind::Other,
        "Writers failed to start or aborted, this is a bug",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    // Mostly just used to make code-cov happy.
    #[test]
    fn test_error_conversions() {
        let err = writer_closed(());
        assert_eq!(err.kind(), ErrorKind::Interrupted);
        let err = writer_controller_bug_log(());
        assert_eq!(err.kind(), ErrorKind::Other);
    }
}
