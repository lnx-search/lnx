use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::io;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use tracing::{info, instrument, warn};

use crate::io::FileEvent;
use crate::metastore::{
    BulkMetastoreModifyOperation,
    Metastore,
    MetastoreError,
    TabletId,
};

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

        num_events_seen += events.len();

        merge_file_events(
            &mut seen_commit,
            &mut pending_transaction_events,
            &mut mutate,
            tablet_id,
            events,
        );
    }

    let num_events_applied = mutate.num_changes();
    let num_events_aborted = num_events_seen - num_events_applied;

    // We sort to ensure correct ordering of events and prevent ghost files.
    mutate
        .mutations
        .sort_unstable_by_key(|(_, event)| event.event_id);
    mutate.commit();

    info!(
        num_events_seen = num_events_seen,
        num_events_applied = num_events_applied,
        num_events_aborted = num_events_aborted,
        "Loaded metastore from disk."
    );

    Ok(())
}

fn merge_file_events(
    seen_commit: &mut BTreeSet<ulid::Ulid>,
    pending_transaction_events: &mut BTreeMap<ulid::Ulid, Vec<FileEvent>>,
    mutate: &mut BulkMetastoreModifyOperation,
    tablet_id: TabletId,
    events: Vec<FileEvent>,
) {
    for event in events {
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

    if tablet_metadata.len() == checkpoint.observed_writer_position {
        return Ok(checkpoint.events);
    }

    if tablet_metadata.len() < checkpoint.observed_writer_position {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "The tablet has less data than the checkpoint observed, file is corrupted",
        ));
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
    use std::time::Duration;

    use super::*;
    use crate::config::COMMIT_MARKER_PREFIX;
    use crate::metastore::checkpoint::TabletCheckpoint;

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

    #[test]
    fn test_checkpoint_and_tablet_invalid_data() {
        let dir = tempfile::tempdir().unwrap();

        let tablet_id = TabletId::new();

        std::fs::write(dir.path().join(format!("{tablet_id}.ckpt")), b"Hello").unwrap();
        std::fs::write(dir.path().join(format!("{tablet_id}.tablet")), b"Hello")
            .unwrap();

        let events = load_tablet_events(dir.path(), dir.path(), tablet_id)
            .expect("Retrieve checkpoints");
        assert!(events.is_empty());
    }

    #[test]
    fn test_checkpoint_and_tablet_checkpoint_up_to_date() {
        let dir = tempfile::tempdir().unwrap();

        let tablet_id = TabletId::new();
        let tablet_path = dir.path().join(format!("{tablet_id}.tablet"));
        let ckpt_path = dir.path().join(format!("{tablet_id}.ckpt"));

        let event = FileEvent::create(None, "example.txt".into(), 0..123);

        // This contains some blank bytes so the file exists, but doesn't have the
        // event itself, so we can tell if the tablet was used to get the event or the
        // checkpoint was used only.
        let mut tablet_data = Vec::new();
        tablet_data.extend_from_slice(&[1; 123]);
        std::fs::write(tablet_path, &tablet_data).unwrap();

        let checkpoint = rmp_serde::to_vec_named(&TabletCheckpoint {
            events: vec![event.clone()],
            observed_writer_position: tablet_data.len() as u64,
        })
        .unwrap();
        std::fs::write(ckpt_path, &checkpoint).unwrap();

        let retrieved_events = load_tablet_events(dir.path(), dir.path(), tablet_id)
            .expect("Retrieve checkpoints");
        assert_eq!(retrieved_events, &[event], "Events should match");
    }

    #[test]
    fn test_checkpoint_and_tablet_corrupted() {
        let dir = tempfile::tempdir().unwrap();

        let tablet_id = TabletId::new();
        let tablet_path = dir.path().join(format!("{tablet_id}.tablet"));
        let ckpt_path = dir.path().join(format!("{tablet_id}.ckpt"));

        let event = FileEvent::create(None, "example.txt".into(), 0..123);

        // This contains some blank bytes so the file exists, but doesn't have the
        // event itself, so we can tell if the tablet was used to get the event or the
        // checkpoint was used only.
        let mut tablet_data = Vec::new();
        tablet_data.extend_from_slice(&[1; 123]);
        std::fs::write(tablet_path, &tablet_data).unwrap();

        let checkpoint = rmp_serde::to_vec_named(&TabletCheckpoint {
            events: vec![event.clone()],
            observed_writer_position: 400,
        })
        .unwrap();
        std::fs::write(ckpt_path, &checkpoint).unwrap();

        let err = load_tablet_events(dir.path(), dir.path(), tablet_id)
            .expect_err("System should error because file sizes don't align");
        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn test_checkpoint_corrupted_and_tablet_recovery() {
        let dir = tempfile::tempdir().unwrap();

        let tablet_id = TabletId::new();
        let tablet_path = dir.path().join(format!("{tablet_id}.tablet"));
        let ckpt_path = dir.path().join(format!("{tablet_id}.ckpt"));

        let event = FileEvent::create(None, "example.txt".into(), 0..123);

        let mut tablet_data = Vec::new();
        tablet_data.extend_from_slice(&[1; 123]);
        tablet_data.extend_from_slice(&event.to_bytes());
        std::fs::write(tablet_path, &tablet_data).unwrap();

        std::fs::write(ckpt_path, b"corrupted data here").unwrap();

        let events = load_tablet_events(dir.path(), dir.path(), tablet_id)
            .expect("System should recover events from tablet directly");
        assert_eq!(events, vec![event], "recovered events should match");
    }

    #[test]
    fn test_checkpoint_corrupted_and_tablet_partial_recovery() {
        let dir = tempfile::tempdir().unwrap();

        let tablet_id = TabletId::new();
        let tablet_path = dir.path().join(format!("{tablet_id}.tablet"));
        let ckpt_path = dir.path().join(format!("{tablet_id}.ckpt"));

        let event = FileEvent::create(None, "example.txt".into(), 0..123);

        let mut tablet_data = Vec::new();
        tablet_data.extend_from_slice(&[1; 123]);
        // Skip the event serialize because we want to ensure it uses the checkpoint.
        tablet_data.extend_from_slice(&[1; 123]);
        tablet_data.extend_from_slice(&event.to_bytes());
        tablet_data.extend_from_slice(&[1; 123]);
        tablet_data.extend_from_slice(&event.to_bytes());
        std::fs::write(tablet_path, &tablet_data).unwrap();

        let checkpoint = rmp_serde::to_vec_named(&TabletCheckpoint {
            events: vec![event.clone()],
            observed_writer_position: 123,
        })
        .unwrap();
        std::fs::write(ckpt_path, &checkpoint).unwrap();

        let events = load_tablet_events(dir.path(), dir.path(), tablet_id)
            .expect("System should recover events from tablet directly");
        assert_eq!(
            events,
            vec![event.clone(), event.clone(), event],
            "recovered events should match"
        );
    }

    #[tokio::test]
    async fn test_merge_file_events_basic_ordering() {
        let metastore = Metastore::connect(":memory:").await.unwrap();
        let mut seen_commit = BTreeSet::new();
        let mut pending_transaction_events = BTreeMap::new();
        let mut mutate = metastore.begin_mutate();

        let sample_events = vec![
            FileEvent::create(None, "example1.txt".into(), 0..123),
            FileEvent::create(None, "example2.txt".into(), 0..123),
            FileEvent::create(None, "example3.txt".into(), 0..123),
            FileEvent::rename(None, "example3.txt".into(), "example4.txt".into()),
            FileEvent::delete(None, "example2.txt".into()),
        ];

        merge_file_events(
            &mut seen_commit,
            &mut pending_transaction_events,
            &mut mutate,
            TabletId::new(),
            sample_events,
        );

        assert_eq!(mutate.num_changes(), 5);
        assert!(seen_commit.is_empty());
        assert!(pending_transaction_events.is_empty());
    }

    #[tokio::test]
    async fn test_merge_file_events_transaction_commit() {
        let metastore = Metastore::connect(":memory:").await.unwrap();
        let mut seen_commit = BTreeSet::new();
        let mut pending_transaction_events = BTreeMap::new();
        let mut mutate = metastore.begin_mutate();

        let tx_id = ulid::Ulid::new();
        let sample_events = vec![
            FileEvent::create(Some(tx_id), "example1.txt".into(), 0..123),
            FileEvent::create(Some(tx_id), "example2.txt".into(), 0..123),
            FileEvent::create(Some(tx_id), "example3.txt".into(), 0..123),
            FileEvent::rename(Some(tx_id), "example3.txt".into(), "example4.txt".into()),
            FileEvent::delete(Some(tx_id), "example2.txt".into()),
        ];
        merge_file_events(
            &mut seen_commit,
            &mut pending_transaction_events,
            &mut mutate,
            TabletId::new(),
            sample_events.clone(),
        );
        assert_eq!(mutate.num_changes(), 0);
        assert_eq!(seen_commit.len(), 0);
        assert_eq!(pending_transaction_events.len(), 1);
        assert_eq!(pending_transaction_events.get(&tx_id), Some(&sample_events));

        let sample_events = vec![FileEvent::create(
            Some(tx_id),
            format!("{COMMIT_MARKER_PREFIX}/{tx_id}.commit"),
            0..0,
        )];
        merge_file_events(
            &mut seen_commit,
            &mut pending_transaction_events,
            &mut mutate,
            TabletId::new(),
            sample_events.clone(),
        );
        assert_eq!(mutate.num_changes(), 5);
        assert_eq!(seen_commit.len(), 1);
        assert!(pending_transaction_events.is_empty());
    }

    #[tokio::test]
    async fn test_merge_file_events_existing_commit_seen() {
        let metastore = Metastore::connect(":memory:").await.unwrap();
        let mut seen_commit = BTreeSet::new();
        let mut pending_transaction_events = BTreeMap::new();
        let mut mutate = metastore.begin_mutate();

        let tx_id = ulid::Ulid::new();
        let sample_events = vec![
            FileEvent::create(Some(tx_id), "example1.txt".into(), 0..123),
            FileEvent::create(Some(tx_id), "example2.txt".into(), 0..123),
            FileEvent::create(Some(tx_id), "example3.txt".into(), 0..123),
            FileEvent::create(
                Some(tx_id),
                format!("{COMMIT_MARKER_PREFIX}/{tx_id}.commit"),
                0..0,
            ),
        ];
        merge_file_events(
            &mut seen_commit,
            &mut pending_transaction_events,
            &mut mutate,
            TabletId::new(),
            sample_events.clone(),
        );
        assert_eq!(mutate.num_changes(), 3);
        assert_eq!(seen_commit.len(), 1);
        assert!(pending_transaction_events.is_empty());

        let sample_events = vec![
            FileEvent::rename(Some(tx_id), "example3.txt".into(), "example4.txt".into()),
            FileEvent::delete(Some(tx_id), "example2.txt".into()),
        ];
        merge_file_events(
            &mut seen_commit,
            &mut pending_transaction_events,
            &mut mutate,
            TabletId::new(),
            sample_events.clone(),
        );
        assert_eq!(mutate.num_changes(), 5);
        assert_eq!(seen_commit.len(), 1);
        assert!(pending_transaction_events.is_empty());
    }

    #[tokio::test]
    async fn test_read_metastore_from_disk_out_of_order_events() {
        let metastore = Metastore::connect(":memory:").await.unwrap();
        let dir = tempfile::tempdir().unwrap();

        let tablet_id_1 = TabletId::new();
        let tablet_1_path = dir.path().join(format!("{tablet_id_1}.tablet"));
        let ckpt_1_path = dir.path().join(format!("{tablet_id_1}.ckpt"));

        let tablet_id_2 = TabletId::new();
        let tablet_2_path = dir.path().join(format!("{tablet_id_2}.tablet"));
        let ckpt_2_path = dir.path().join(format!("{tablet_id_2}.ckpt"));

        std::fs::write(&tablet_1_path, b"test").unwrap();
        std::fs::write(&tablet_2_path, b"test").unwrap();

        let event_1 = FileEvent::create(None, "example1.txt".into(), 0..123);
        let event_2 = FileEvent::create(None, "example2.txt".into(), 0..123);
        let event_3 = FileEvent::create(None, "example3.txt".into(), 0..123);
        let event_4 = FileEvent::create(None, "example3.txt".into(), 0..123);
        let event_5 = FileEvent::create(None, "example4.txt".into(), 0..123);

        let checkpoint = rmp_serde::to_vec_named(&TabletCheckpoint {
            events: vec![event_4, event_5],
            observed_writer_position: 4,
        })
        .unwrap();
        std::fs::write(ckpt_1_path, &checkpoint).unwrap();

        let checkpoint = rmp_serde::to_vec_named(&TabletCheckpoint {
            events: vec![event_2, event_1, event_3],
            observed_writer_position: 4,
        })
        .unwrap();
        std::fs::write(ckpt_2_path, &checkpoint).unwrap();

        read_metastore_from_disk(
            metastore.clone(),
            dir.path().to_path_buf(),
            dir.path().to_path_buf(),
        )
        .expect("Read metadata state and apply correct ordering");

        let example_file = metastore
            .get_file("example3.txt")
            .expect("File should exist");
        assert_eq!(example_file.metadata.tablet_id, tablet_id_1);
        let example_file = metastore
            .get_file("example4.txt")
            .expect("File should exist");
        assert_eq!(example_file.metadata.tablet_id, tablet_id_1);
        let example_file = metastore
            .get_file("example1.txt")
            .expect("File should exist");
        assert_eq!(example_file.metadata.tablet_id, tablet_id_2);
    }
}
