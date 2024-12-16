use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::io;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use tracing::{info, instrument, warn};

use crate::io::FileEvent;
use crate::metastore::{Metastore, MetastoreError, TabletId};

/// Reads the metastore state from disk.
///
/// This scans all the existing tablets and associated checkpoints,
/// if a tablet checkpoint cannot be read or does not exist, the tablet itself
/// is scanned to ensure all metadata events are read.
pub async fn load_metastore_state_from_disk(
    metastore: Metastore,
    tablet_storage_path: &Path,
    checkpoint_storage_path: &Path,
) -> Result<(), MetastoreError> {
    let tablet_storage_path = tablet_storage_path.to_path_buf();
    let checkpoint_storage_path = checkpoint_storage_path.to_path_buf();

    let cb = move || {
        read_metastore_from_disk(metastore, tablet_storage_path, checkpoint_storage_path)
    };

    tokio::task::spawn_blocking(cb)
        .await
        .expect("Spawn background thread")
        .map_err(MetastoreError::RecoverError)
}

#[instrument(skip(metastore))]
fn read_metastore_from_disk(
    metastore: Metastore,
    tablet_storage_path: PathBuf,
    checkpoint_storage_path: PathBuf,
) -> io::Result<()> {
    let mut seen_commit = BTreeSet::new();
    let mut pending_transaction_events = BTreeMap::new();
    let mut mutate = metastore.begin_mutate();

    let tablet_ids = parse_tablet_ids(&tablet_storage_path)?;
    info!(num_tablets = tablet_ids.len(), "Found tablets");

    let mut num_events_seen = 0;
    for tablet_id in tablet_ids {
        let events = load_tablet_events(
            &tablet_storage_path,
            &checkpoint_storage_path,
            tablet_id,
        )?;

        for event in events {
            num_events_seen += 1;

            let transaction_id = match event.transaction_id {
                // Operation was not part of a bulk transaction.
                None => {
                    mutate.add_event(tablet_id, event);
                    continue;
                },
                // Operation was part of a larger transaction and needs
                // a commit marker to exist to be valid.
                Some(tx_id) => tx_id,
            };

            if event.is_commit() {
                seen_commit.insert(transaction_id);

                let pending: Vec<FileEvent> = pending_transaction_events
                    .remove(&transaction_id)
                    .unwrap_or_default();
                for event in pending {
                    mutate.add_event(tablet_id, event);
                }
            } else if seen_commit.contains(&transaction_id) {
                mutate.add_event(tablet_id, event);
            } else {
                pending_transaction_events
                    .entry(transaction_id)
                    .or_default()
                    .push(event);
            }
        }
    }

    let num_events_applied = mutate.num_changes();
    let num_events_aborted = num_events_seen - num_events_applied;

    info!(
        num_events_seen = num_events_seen,
        num_events_applied = num_events_applied,
        num_events_aborted = num_events_aborted,
        "Loaded metastore from disk."
    );

    Ok(())
}

#[instrument(skip(tablet_storage_path, checkpoint_storage_path))]
fn load_tablet_events(
    tablet_storage_path: &Path,
    checkpoint_storage_path: &Path,
    tablet_id: TabletId,
) -> io::Result<Vec<FileEvent>> {
    let tablet_file_path =
        crate::io::get_tablet_file_path(tablet_storage_path, tablet_id);
    let tablet_metadata = tablet_file_path.metadata()?;

    let mut checkpoint =
        super::checkpoint::read_tablet_checkpoint(checkpoint_storage_path, tablet_id);

    if tablet_metadata.len() <= checkpoint.observed_writer_position {
        return Ok(checkpoint.events);
    }

    // Heavy operation, scans the remaining data not yet observed by the checkpoint
    // which can potentially be a lot of data depending on write loads.
    let recovered_events = crate::io::metadata::load_tablet_metadata(
        tablet_storage_path,
        tablet_id,
        checkpoint.observed_writer_position,
    )?;
    info!(
        num_events = recovered_events.len(),
        "Recovered additional events"
    );

    checkpoint.events.extend(recovered_events);

    // Write the new checkpoint file so we don't have to scan it again.
    info!("Saving updated checkpoint file");
    super::checkpoint::persist_checkpoint(
        checkpoint_storage_path,
        tablet_id,
        &checkpoint,
    )?;

    Ok(checkpoint.events)
}

#[instrument(skip_all)]
fn parse_tablet_ids(tablet_storage_path: &Path) -> io::Result<Vec<TabletId>> {
    let mut tablets = Vec::new();

    for entry in tablet_storage_path.read_dir()? {
        let entry = entry?;
        let file_path = entry.path();
        let metadata = entry.metadata()?;

        if metadata.is_dir() || file_path.extension() != Some(OsStr::new("tablet")) {
            warn!(path = %file_path.display(), "Skipping unknown file in tablet directory");
            continue;
        }
        let raw_file_name = file_path.file_name().unwrap();
        let file_name = raw_file_name.to_string_lossy();

        let (tablet_id_str, _suffix) = file_name.split_once('.').unwrap();

        if let Ok(tablet_id) = TabletId::from_str(tablet_id_str) {
            tablets.push(tablet_id);
        } else {
            warn!(file_name = tablet_id_str, "Invalid tablet ID file name");
        }
    }

    tablets.sort();

    Ok(tablets)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_tablet_ids() {
        let dir = tempfile::tempdir().unwrap();

        let mut tablet_ids = vec![
            TabletId::new(),
            TabletId::new(),
            TabletId::new(),
            TabletId::new(),
            TabletId::new(),
        ];
        tablet_ids.sort();

        for tablet_id in tablet_ids.iter() {
            std::fs::write(
                dir.path()
                    .join(tablet_id.to_string())
                    .with_extension("tablet"),
                b"Hello",
            )
            .unwrap();
        }

        let parsed_ids =
            parse_tablet_ids(dir.path()).expect("Read tablet directory and parse files");
        assert_eq!(parsed_ids, tablet_ids, "Parsed tablet IDs should match");
    }

    #[test]
    fn test_ignore_invalid_file_names() {
        let dir = tempfile::tempdir().unwrap();

        std::fs::write(dir.path().join("example.txt"), b"Hello").unwrap();

        std::fs::write(dir.path().join("example.tablet"), b"Hello").unwrap();

        let parsed_ids =
            parse_tablet_ids(dir.path()).expect("Read tablet directory and parse files");
        assert!(parsed_ids.is_empty());

        let _guard = tempfile::tempdir_in(dir.path());
        let parsed_ids =
            parse_tablet_ids(dir.path()).expect("Read tablet directory and parse files");
        assert!(parsed_ids.is_empty());
    }
}
