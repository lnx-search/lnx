mod actors;
mod body;
mod event;
mod flush_waker;
pub(crate) mod metadata;
mod runtime;
mod utils;

use std::path::{Path, PathBuf};

pub use self::actors::{
    Metadata,
    TabletReader,
    TabletReaderOptions,
    TabletWriter,
    TabletWriterOptions,
};
pub(crate) use self::actors::{WriterEventHook, WriterResponse};
pub use self::body::{Body, BodySender};
pub use self::event::{EventData, FileEvent};
pub use self::flush_waker::{BulkFlushWaker, FlushWaker};
pub use self::runtime::{create_io_runtime, RuntimeDispatcher, RuntimeOptions};
use crate::metastore::TabletId;

pub fn get_tablet_file_path(base: &Path, tablet_id: TabletId) -> PathBuf {
    base.join(tablet_id.to_string()).with_extension("tablet")
}
