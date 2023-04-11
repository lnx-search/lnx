use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io;
use std::io::ErrorKind;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytecheck::CheckBytes;
use exponential_backoff::Backoff;
use hashbrown::HashMap;
use parking_lot::RwLock;
use puppet::ActorMailbox;
use rkyv::{Archive, Deserialize, Serialize};

use crate::fragments::writer::{Flush, GetCurrentState, RemoveOnDrop, Seal, WriteFile};
use crate::listeners::{FragmentListener, ListenerManager};
use crate::{EnvCtx, SharedSlice};

mod block;
mod reader;
mod writer;

pub use self::block::{BlockId, BlockInfo};
pub use self::reader::FragmentReader;
pub use self::writer::{
    FragmentStream,
    FragmentWriter,
    StreamError,
    WriteDocBlock,
    WriterState,
};
use crate::metastore::Metastore;

#[repr(C)]
#[derive(Serialize, Deserialize, Archive, Debug, Clone)]
#[archive_attr(derive(CheckBytes, Debug))]
pub struct FragmentInfo {
    /// The unique ID of the fragment.
    ///
    /// This is unique across all nodes and can only be created
    /// by the node associated with the ID (it is a HLCTimestamp)
    pub fragment_id: u64,
    /// The fragment ID that should contain the current fragments data.
    ///
    /// This is a somewhat confusing mechanic but it is used to allow
    /// other nodes to perform shard-nothing architecture in the event
    /// the node originally responsible for indexing the data contained
    /// within the fragment goes down.
    ///
    /// This ID allows the node that produced this fragment to hand control
    /// back to the parent node to manage without voiding the behavour
    /// of the unique ID (IDs are linked to node IDs so one node cannot create
    /// another node's IDs)
    ///
    /// If an orphan ID exists, the effective 'owner' of the fragment should be
    /// taken from this ID rather than the original `fragment_id`.
    pub orphaned_id: Option<u64>,
    /// The number of blocks in the fragment.
    pub num_blocks: u32,
    /// The number of bytes in total that make up the fragment.
    pub num_bytes_total: u64,
    /// The number of documents in the fragment.
    pub num_docs: u32,
    /// A list of fragments that this fragment is made from.
    ///
    /// This only goes back in time upto 1 generation of fragments,
    /// i.e. older fragments which may have previously been part
    /// of the parent fragments will not be included.
    pub child_of_fragments: Vec<u64>,
}

#[derive(Clone)]
/// The segment writer manager.
///
/// This keeps track of active writers and caches active files
/// while closing in-active files are a period of in activity.
pub struct IndexFragmentsWriters {
    /// The storage configuration environment.
    env: EnvCtx,
    /// The cache of live writers mapping fragment ID to writer.
    active_writers: Arc<RwLock<HashMap<u64, ActorMailbox<FragmentWriter>>>>,
    /// The storage metastore.
    ///
    /// This tracks any important metadata for regular use without
    /// requiring repairs or recovery.
    metastore: Metastore,
    /// Event listeners and notifications.
    listeners: ListenerManager,
}

impl IndexFragmentsWriters {
    /// Create a new fragment writer with a given metastore.
    pub fn from_existing_state(
        env: EnvCtx,
        metastore: Metastore,
        writers: HashMap<u64, ActorMailbox<FragmentWriter>>,
        listeners: ListenerManager,
    ) -> Self {
        let slf = Self {
            env,
            active_writers: Arc::new(RwLock::new(writers)),
            metastore,
            listeners,
        };

        slf.listeners.register_fragment_listener(slf.clone());

        slf
    }

    #[instrument(name = "open-fragment-writer", skip_all)]
    /// This will use the live writer if it already exists otherwise, a writer
    /// will be opened.
    async fn get_writer(
        &self,
        fragment_id: u64,
    ) -> io::Result<ActorMailbox<FragmentWriter>> {
        if let Some(writer) = self.active_writers.read().get(&fragment_id).cloned() {
            trace!("Using cached writer");
            return Ok(writer);
        }

        let path =
            crate::resolvers::get_fragment_location(&self.env.root_path, fragment_id);
        info!(path = %path.display(), "Opening new fragment writer");

        let mut options = OpenOptions::new();
        options.write(true);
        options.create(true);

        self.sync_directory().await?;

        let file = tokio::task::spawn_blocking(move || options.open(path))
            .await
            .expect("Join thread")?;

        let writer = FragmentWriter::new(
            self.env.clone(),
            fragment_id,
            file,
            self.metastore.clone(),
        );

        info!("Creating new blank fragment");
        self.metastore
            .create_new_fragment(fragment_id)
            .map_err(|e| io::Error::new(ErrorKind::Other, e))?;

        self.active_writers
            .write()
            .insert(fragment_id, writer.clone());
        Ok(writer)
    }

    /// Gets the current state of the writer.
    pub async fn get_current_writer_state(
        &self,
        fragment_id: u64,
    ) -> Option<WriterState> {
        let writer = self.active_writers.read().get(&fragment_id).cloned()?;

        Some(writer.send(GetCurrentState).await)
    }

    #[instrument(name = "open-fragment-writer", skip_all)]
    pub async fn write_file(
        &self,
        fragment_id: u64,
        file: String,
        bytes: SharedSlice,
    ) -> io::Result<()> {
        let writer = self.get_writer(fragment_id).await?;
        writer
            .send(WriteFile {
                file: file.clone(),
                bytes,
            })
            .await?;
        self.listeners.trigger_fragment_file_add(fragment_id, file);
        Ok(())
    }

    #[instrument(name = "fragment-write-block", skip(self, block_data))]
    /// Writes a set of block documents to the given fragment.
    pub async fn write_block(
        &self,
        fragment_id: u64,
        block_data: WriteDocBlock,
    ) -> io::Result<()> {
        let writer = self.get_writer(fragment_id).await?;
        let doc = block_data.block.clone();
        let num_bytes = doc.data().len();
        let start = Instant::now();

        // TODO: Add cache to skip blocks which already exist.
        //       This can help cut out duplicates early on.
        writer.send(block_data).await?;

        writer.send(Flush).await?;

        // TODO: Optimise with smallvec
        self.listeners
            .trigger_fragment_block_flush(fragment_id, vec![doc]);

        debug!(elapsed = ?start.elapsed(), num_bytes = num_bytes, "Wrote bytes to fragment");

        Ok(())
    }

    #[instrument(name = "fragment-write-blocks", skip(self, blocks))]
    /// Writes a set of block documents to the given fragment.
    pub async fn write_many_blocks(
        &self,
        fragment_id: u64,
        blocks: &[WriteDocBlock],
    ) -> io::Result<()> {
        let writer = self.get_writer(fragment_id).await?;
        let mut num_bytes = 0;
        let mut docs = Vec::with_capacity(blocks.len());
        let start = Instant::now();

        // TODO: Add cache to skip blocks which already exist.
        //       This can help cut out duplicates early on.
        for block_data in blocks {
            docs.push(block_data.block.clone());
            num_bytes += block_data.block.data().len();
            writer.send(block_data.clone()).await?;
        }

        writer.send(Flush).await?;

        // TODO: Optimise with smallvec
        self.listeners
            .trigger_fragment_block_flush(fragment_id, docs);

        debug!(elapsed = ?start.elapsed(), num_bytes = num_bytes, "Wrote bytes to fragment");

        Ok(())
    }

    #[instrument(name = "fragment-write-stream", skip(self, stream))]
    /// Writes the incoming fragment stream to given fragment.
    pub async fn write_stream(
        &self,
        fragment_id: u64,
        stream: FragmentStream,
    ) -> Result<(), StreamError> {
        let writer = self.get_writer(fragment_id).await?;

        let start = Instant::now();
        writer.send(stream).await?;
        info!(elapsed = ?start.elapsed(), "Fragment stream download complete");

        Ok(())
    }

    #[instrument(name = "fragment-seal", skip(self))]
    /// Seal written fragment
    pub async fn seal(&self, fragment_id: u64, info: FragmentInfo) -> io::Result<()> {
        let writer = self.get_writer(fragment_id).await?;

        let start = Instant::now();
        writer.send(Seal(info)).await?;

        info!(elapsed = ?start.elapsed(), "Fragment seal complete");

        self.listeners.trigger_fragment_seal(fragment_id);

        Ok(())
    }

    /// Sync the fragments directory to ensure fragments are correctly persisted.
    ///
    /// This is a no-op on windows.
    async fn sync_directory(&self) -> io::Result<()> {
        #[cfg(unix)]
        lnx_executor::spawn_task(move || {
            std::fs::OpenOptions::new()
                .read(true)
                .open(&crate::resolvers::fragments_folder())?
                .sync_data()?;
            Ok::<_, io::Error>(())
        })
        .await
        .expect("Join task")?;

        Ok(())
    }
}

impl FragmentListener for IndexFragmentsWriters {
    fn on_delete(&self, fragment_id: u64) {
        if let Some(writer) = self.active_writers.write().remove(&fragment_id) {
            lnx_executor::spawn_task(async move {
                writer.send(RemoveOnDrop).await;
            });
        }
    }
}

#[derive(Clone)]
pub struct IndexFragmentsReaders {
    /// The storage configuration environment.
    env: EnvCtx,
    sealed_fragments: Arc<RwLock<BTreeMap<u64, FragmentReader>>>,
    /// Event listeners and notifications.
    listeners: ListenerManager,
}

impl IndexFragmentsReaders {
    /// Create a new fragment writer with a given metastore.
    pub fn from_existing_state(
        env: EnvCtx,
        readers: BTreeMap<u64, FragmentReader>,
        listeners: ListenerManager,
    ) -> Self {
        let slf = Self {
            env,
            sealed_fragments: Arc::new(RwLock::new(readers)),
            listeners,
        };

        slf.listeners.register_fragment_listener(slf.clone());

        slf
    }

    /// Get a given fragment reader if it exists
    pub fn get_reader(&self, fragment_id: u64) -> Option<FragmentReader> {
        self.sealed_fragments.read().get(&fragment_id).cloned()
    }

    /// Open a new reader.
    ///
    /// This will retry opening the file if it can up to 3 times.
    pub async fn open_new_reader(&self, fragment_id: u64) -> io::Result<()> {
        let backoff = Backoff::new(3, Duration::from_secs(1), Duration::from_secs(5));

        let mut last_error = None;
        for wait_for in backoff.iter() {
            let slf = self.clone();
            let result = lnx_executor::spawn_task(async move {
                slf.try_add_new_reader_blocking(fragment_id)
            })
            .await
            .expect("Join task");

            match result {
                Ok(()) => return Ok(()),
                Err(e) => {
                    warn!(error = ?e, wait_for = ?wait_for, "Failed to open reader, retrying in {wait_for:?}");
                    last_error = Some(e);
                },
            }
        }

        Err(last_error.unwrap())
    }

    fn try_add_new_reader_blocking(&self, fragment_id: u64) -> io::Result<()> {
        info!(fragment_id = fragment_id, "Opening fragment");
        let path =
            crate::resolvers::get_fragment_location(&self.env.root_path, fragment_id);
        let reader = FragmentReader::open_mmap_blocking(path)?;
        self.sealed_fragments.write().insert(fragment_id, reader);

        self.listeners.trigger_fragment_read_ready(fragment_id);

        Ok(())
    }
}

impl FragmentListener for IndexFragmentsReaders {
    fn on_delete(&self, fragment_id: u64) {
        if let Some(fragment) = self.sealed_fragments.write().remove(&fragment_id) {
            fragment.set_remove_on_drop();
        }

        debug!(
            fragment_id = fragment_id,
            "Fragment is no longer searchable"
        );
    }
}
