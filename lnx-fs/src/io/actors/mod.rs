use std::fmt::Debug;
use std::io;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use tracing::error;
use crate::metastore::TabletId;

mod tablet_reader;
mod tablet_writer;
mod footer;
mod basic_writer;

pub use self::tablet_reader::{TabletReader, TabletReaderOptions};
pub use self::tablet_writer::{TabletWriter, TabletWriterOptions};

#[async_trait(?Send)]
/// A factory that creates actor tasks from within the context
/// of a glommio runtime.
pub trait ActorFactory: Send {
    /// Spawns an actor instance with the pre-configured state in factory.
    async fn spawn_actor(self) -> io::Result<()>;
}

pub(super) fn get_tablet_file_path(base: &Path, tablet_id: TabletId) -> PathBuf {
    base.join(tablet_id.to_string()).with_extension("tablet")
}

pub(super) fn get_tablet_metadata_file_path(base: &Path, tablet_id: TabletId) -> PathBuf {
    base.join(tablet_id.to_string()).with_extension("tablet.meta")
}

#[derive(Debug)]
/// Metadata relating to the blob being written.
pub struct Metadata {
    /// The file path for the blob.
    pub path: String,
    /// When the file was created.
    pub created_at: u64,
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
    io::Error::new(ErrorKind::Other, "Writers failed to start or aborted, this is a bug")
}

fn writer_failed_to_start<E>(_err: E) -> io::Error {
    io::Error::new(ErrorKind::Other, "Writer failed to start or aborted")
}