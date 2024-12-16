use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::fs::File;
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use std::{cmp, io};

use serde_derive::{Serialize, Deserialize};
use bon::Builder;
use tracing::{error, info, instrument, warn};

use crate::io::{WriterEventHook, FileEvent, WriterResponse};
use crate::metastore::TabletId;

#[derive(Debug, Clone, Builder)]
/// Checkpoint actor configuration.
pub struct CheckpointOptions {
    #[builder(into)]
    /// The base path to store metadata snapshot files.
    base_path: PathBuf,
}

/// Creates a new checkpointing actor.
/// 
/// This actor listening for new writer events via the [WriterEventHook]
/// callbacks and takes atomic snapshots of the tablet state and persists them to disk.
/// 
/// The actor itself runs in a separate thread and commits a new change upto 1 second
/// after the change was applied.
pub async fn spawn_checkpoint_actor(
    options: CheckpointOptions,
) -> io::Result<MetadataCheckpointEventHook> {
    let base_path = options.base_path.clone();
    let (parent_directory, temp_dir) = tokio::task::spawn_blocking(move || {
        if let Err(e) = cleanup_old_temp_directories(&base_path) {
            warn!(
                    error = ?e,
                    "Failed to cleanup old temporary directories, this may need manual cleanup of `temp*` files"
                );
        }

        let tmp = tempfile::TempDir::with_prefix_in("temp", &base_path)?;
        let parent_directory = File::open(base_path.parent().unwrap())?;

        Ok::<_, io::Error>((parent_directory, tmp))
    }).await.expect("Join background thread")?;

    let (tx, rx) = flume::unbounded();

    let slf = MetadataCheckpointActor {
        live_state: BTreeMap::new(),
        to_remove: Vec::new(),
        incoming: rx,
        options,
        temp_dir,
        parent_directory,
    };

    std::thread::Builder::new()
        .name("lnx-fs-checkpointer".to_string())
        .spawn(move || slf.run())
        .expect("Spawn background thread");

    Ok(MetadataCheckpointEventHook { sender: tx })
}

/// The event callbacks for submitting new checkpoint events to the actor.
pub struct MetadataCheckpointEventHook {
    sender: flume::Sender<CheckpointEvent>,
}

impl Debug for MetadataCheckpointEventHook {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MetadataCheckpointEventHook")
    }
}

impl WriterEventHook for MetadataCheckpointEventHook {
    fn on_writer_start(&self, _tablet_id: TabletId) {}

    fn on_writer_response(&self, response: WriterResponse) {
        let result = self.sender.send(CheckpointEvent::FileEvent(response));
        if result.is_err() {
            warn!("Failed to send checkpoint event info due because channel closed");
        }
    }

    fn on_writer_close(&self, tablet_id: TabletId) {
        let result = self.sender.send(CheckpointEvent::WriterClose(tablet_id));
        if result.is_err() {
            warn!("Failed to send checkpoint event info due because channel closed");
        }
    }
}

/// Read a tablet checkpoint.
/// 
/// If the tablet checkpoint does not exist or cannot be read, a default [TabletCheckpoint] is returned.
pub async fn read_tablet_checkpoint(
    options: &CheckpointOptions, 
    tablet_id: TabletId,
) -> TabletCheckpoint {
    let path = get_checkpoint_export_path(&options.base_path, tablet_id);
    
    tokio::task::spawn_blocking(move || {
        let file = match File::open(path) {
            Err(e) => {
                warn!(
                    error = ?e, 
                    tablet_id = %tablet_id, 
                    "Failed to read tablet checkpoint, system will automatically recover-metadata from tablet",
                );
                return TabletCheckpoint::default();
            },
            Ok(file) => file,
        };
        
        match rmp_serde::from_read::<_, TabletCheckpoint>(file) {
            Err(e) => {
                warn!(
                    error = ?e, 
                    tablet_id = %tablet_id, 
                    "Failed to read tablet checkpoint, system will automatically recover-metadata from tablet",
                );
                TabletCheckpoint::default()
            },
            Ok(checkpoint) => checkpoint,
        }
    }).await.expect("Spawn background thread")  
}

enum CheckpointEvent {
    WriterClose(TabletId),
    FileEvent(WriterResponse),
}

struct MetadataCheckpointActor {
    live_state: BTreeMap<TabletId, TabletCheckpoint>,
    to_remove: Vec<TabletId>,
    incoming: flume::Receiver<CheckpointEvent>,
    options: CheckpointOptions,
    temp_dir: tempfile::TempDir,
    parent_directory: File,
}

impl MetadataCheckpointActor {
    #[instrument("checkpointer", skip(self))]
    fn run(mut self) {
        info!("Checkpointing actor is running");

        loop {
            let Ok(event) = self.incoming.recv() else {
                break;
            };
            self.handle_event(event);

            let commit_deadline = Instant::now() + Duration::from_secs(1);
            while let Ok(event) = self.incoming.recv_deadline(commit_deadline) {
                self.handle_event(event);
            }

            self.snapshot_checkpoints();
            self.cleanup_dead_writers();
        }

        self.snapshot_checkpoints();
        info!("Checkpointing actor has shutdown");
    }

    fn handle_event(&mut self, event: CheckpointEvent) {
        match event {
            CheckpointEvent::WriterClose(tablet_id) => {
                self.to_remove.push(tablet_id);
            },
            CheckpointEvent::FileEvent(response) => {
                let state = self.live_state.entry(response.tablet_id).or_default();
                state.observed_writer_position =
                    cmp::max(state.observed_writer_position, response.writer_position);
                state.events.push(response.event);
            },
        }
    }

    fn cleanup_dead_writers(&mut self) {
        for tablet_id in self.to_remove.drain(..) {
            self.live_state.remove(&tablet_id);
        }
    }

    fn snapshot_checkpoints(&mut self) {
        for (tablet_id, checkpoint) in self.live_state.iter() {
            if let Err(e) = self.snapshot_tablet_checkpoint(*tablet_id, checkpoint) {
                error!(error = ?e, tablet_id = %tablet_id, "Failed to export tablet checkpoints");
            }
        }

        if let Err(e) = self.parent_directory.sync_data() {
            error!(error = ?e, "Failed to sync parent directory of checkpoint files");
        }
    }

    fn snapshot_tablet_checkpoint(
        &self,
        tablet_id: TabletId,
        checkpoint: &TabletCheckpoint,
    ) -> io::Result<()> {
        let mut writer = tempfile::NamedTempFile::new_in(self.temp_dir.path())?;

        let export_path = get_checkpoint_export_path(
            &self.options.base_path,
            tablet_id,
        );

        let serialized = rmp_serde::to_vec_named(checkpoint)
            .map_err(|e| io::Error::new(ErrorKind::Other, e.to_string()))?;

        writer.write_all(&serialized)?;
        writer.as_file_mut().sync_all()?;

        writer.persist(export_path)?;

        Ok(())
    }
}

#[derive(Default, Debug, Serialize, Deserialize)]
// WARNING: Changing this layout requires updating the bucket documentation.
pub struct TabletCheckpoint {
    /// The current file events that occurred for the tablet.
    pub events: Vec<FileEvent>,
    /// The current writer position from the last event we observed.
    pub observed_writer_position: u64,
}

fn cleanup_old_temp_directories(dir_path: &Path) -> io::Result<()> {
    for entry in dir_path.read_dir()? {
        let entry = entry?;
        let file_name = entry.file_name();
        let name = file_name.to_string_lossy();

        if name.starts_with("temp") {
            info!("Old temporary directory detected, cleaning up old files...");
            std::fs::remove_dir(entry.path())?;
        }
    }
    Ok(())
}

fn get_checkpoint_export_path(
    base: &Path,
    tablet_id: TabletId,
) -> PathBuf {
    base.join(tablet_id.to_string())
        .with_extension("ckpt")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cleanup_old_temp_directories() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("tempexample");
        std::fs::create_dir(&path).unwrap();
        cleanup_old_temp_directories(temp_dir.path())
            .expect("Cleanup old temp directories");
        assert!(!path.exists(), "dir should be removed");
    }

    #[tokio::test]
    async fn test_actor_serialize_and_persist_events() {
        let (_tx, rx) = flume::unbounded();
        let temp_dir = tempfile::tempdir().unwrap();
        let inner_temp_dir = tempfile::tempdir_in(temp_dir.path()).unwrap();

        let parent_directory = File::open(temp_dir.path()).unwrap();

        let options = CheckpointOptions::builder()
            .base_path(temp_dir.path())
            .build();

        let mut actor = MetadataCheckpointActor {
            live_state: BTreeMap::new(),
            to_remove: Vec::new(),
            incoming: rx,
            options: options.clone(),
            temp_dir: inner_temp_dir,
            parent_directory,
        };

        let tablet_id = TabletId::new();
        actor.handle_event(CheckpointEvent::FileEvent(WriterResponse {
            tablet_id,
            writer_position: 128,
            event: FileEvent::create(None, "example.txt".to_string(), 0..128),
        }));

        actor.snapshot_checkpoints();

        let checkpoint = read_tablet_checkpoint(&options, tablet_id).await;
        assert_eq!(checkpoint.events.len(), 1);
        assert_eq!(checkpoint.observed_writer_position, 128);
    }

    #[test]
    fn test_actor_clean_up_dead_actor() {
        let (_tx, rx) = flume::unbounded();
        let temp_dir = tempfile::tempdir().unwrap();
        let inner_temp_dir = tempfile::tempdir_in(temp_dir.path()).unwrap();

        let parent_directory = File::open(temp_dir.path()).unwrap();

        let options = CheckpointOptions::builder()
            .base_path(temp_dir.path())
            .build();

        let mut actor = MetadataCheckpointActor {
            live_state: BTreeMap::new(),
            to_remove: Vec::new(),
            incoming: rx,
            options,
            temp_dir: inner_temp_dir,
            parent_directory,
        };

        let tablet_id = TabletId::new();
        actor.handle_event(CheckpointEvent::FileEvent(WriterResponse {
            tablet_id,
            writer_position: 128,
            event: FileEvent::create(None, "example.txt".to_string(), 0..128),
        }));
        actor.handle_event(CheckpointEvent::WriterClose(tablet_id));
        assert_eq!(actor.live_state.len(), 1);

        actor.cleanup_dead_writers();
        assert_eq!(actor.live_state.len(), 0);
    }

    #[tokio::test]
    async fn test_full_actor_flow() {
        let _ = tracing_subscriber::fmt::try_init();
        
        let temp_dir = tempfile::tempdir().unwrap();
        let options = CheckpointOptions::builder()
            .base_path(temp_dir.path())
            .build();

        let tx = spawn_checkpoint_actor(options.clone())
            .await
            .expect("Create checkpoint actor");
        dbg!(&tx);

        let tablet_id = TabletId::new();
        tx.on_writer_start(tablet_id);
        tx.on_writer_response(WriterResponse {
            tablet_id,
            writer_position: 128,
            event: FileEvent::create(None, "example.txt".to_string(), 0..128),
        });
        tx.on_writer_response(WriterResponse {
            tablet_id,
            writer_position: 300,
            event: FileEvent::create(None, "example.txt".to_string(), 128..256),
        });
        tx.on_writer_close(tablet_id);

        drop(tx);
        tokio::time::sleep(Duration::from_secs(1)).await;

        let checkpoint = read_tablet_checkpoint(&options, tablet_id).await;
        assert_eq!(checkpoint.events.len(), 2);
        assert_eq!(checkpoint.observed_writer_position, 300);
    }
    
    #[tokio::test]
    async fn test_read_checkpoint_missing_file() {
        let dir = tempfile::tempdir().unwrap();

        let options = CheckpointOptions::builder()
            .base_path(dir.path())
            .build();
        
        let ckpt = read_tablet_checkpoint(&options, TabletId::new()).await;
        assert_eq!(ckpt.observed_writer_position, 0);
        assert!(ckpt.events.is_empty());
    }
    
    #[tokio::test]
    async fn test_read_checkpoint_invalid_data() {
        let dir = tempfile::tempdir().unwrap();
        
        let options = CheckpointOptions::builder()
            .base_path(dir.path())
            .build();
        let tablet_id = TabletId::new();
        
        let path = get_checkpoint_export_path(&options.base_path, tablet_id);
        std::fs::write(path, b"hello, world").unwrap();
        
        let ckpt = read_tablet_checkpoint(&options, tablet_id).await;
        assert_eq!(ckpt.observed_writer_position, 0);
        assert!(ckpt.events.is_empty());
    }
}
