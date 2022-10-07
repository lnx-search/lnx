use std::io::{ErrorKind, SeekFrom};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::{cmp, io};

use datacake_crdt::HLCTimestamp;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use crate::blocking::utils::read_metadata;
use crate::blocking::BlockingWriter;
use crate::deletes::Deletes;
use crate::meta_merger::{ManagedMeta, MetaFile};
use crate::{
    DELETES_FILE,
    MANAGED_FILE,
    METADATA_HEADER_SIZE,
    META_FILE,
    SPECIAL_FILES,
};

const BUFFER_SIZE: usize = 64 << 10;

/// Combines two or more existing segments into a single, larger segment.
///
/// This is the blocking variant of the combiners which wraps the `tokio::fs` system
/// which internally wraps the `std::fs` system in a threadpool.
///
/// This system is not the most efficient approach, it is designed for compatibility with
/// all operating systems rather than for performance.
pub struct BlockingCombiner {
    writer: BlockingWriter,

    deletes: Deletes,
    meta_file: MetaFile,
    managed_file: ManagedMeta,
}

impl BlockingCombiner {
    /// Create a new [BlockingCombiner] instance writing to the provided path.
    pub async fn create(
        path: &Path,
        index: String,
        segment_id: HLCTimestamp,
    ) -> io::Result<Self> {
        let writer = BlockingWriter::create(path, 0, index, segment_id).await?;

        Ok(Self {
            writer,
            deletes: Deletes::default(),
            meta_file: MetaFile::default(),
            managed_file: ManagedMeta::default(),
        })
    }

    /// Combines a segment into the new segment.
    ///
    /// This can be called multiple times, combining several smaller segments into a single,
    /// larger segment.
    pub async fn combine_segment(&mut self, segment_file: &Path) -> io::Result<()> {
        let mut segment = File::open(segment_file).await?;
        let metadata = read_metadata(&mut segment).await?;

        // Set the seek position back to the end of the metadata.
        segment
            .seek(SeekFrom::Start(METADATA_HEADER_SIZE as u64))
            .await?;

        let mut files = metadata
            .files()
            .iter()
            .map(|v| (v.1.clone(), v.0.clone()))
            .collect::<Vec<_>>();

        // We want to sort them by starting index so we minimise seeking overhead.
        files.sort_by_key(|v| v.0.start);

        for (range, path) in files {
            let start = self.writer.current_pos();

            if range.start != start {
                segment.seek(SeekFrom::Start(range.start)).await?;
            }

            if SPECIAL_FILES.contains(&path.as_str()) {
                self.merge_special_files(&mut segment, range, &path).await?;
                continue;
            }

            self.write_file(&mut segment, range).await?;

            let end = self.writer.current_pos();
            self.writer.add_file(&path, start..end);
        }

        // Add the history of the data. This lets us work out
        // what bits of segment data are where even after a merge.
        self.writer.add_history(metadata.segment_id());
        for historic_id in metadata.history() {
            self.writer.add_history(*historic_id);
        }

        Ok(())
    }

    /// Finalises any remaining buffers so that file is safely persisted to disk.
    pub async fn finalise(mut self) -> io::Result<PathBuf> {
        let meta = self
            .meta_file
            .to_json()
            .map_err(|e| io::Error::new(ErrorKind::Other, e))?;

        let managed = self
            .managed_file
            .to_json()
            .map_err(|e| io::Error::new(ErrorKind::Other, e))?;

        let deletes = self.deletes.to_compressed_bytes().await?;

        let meta_start = self.writer.current_pos();
        self.writer.write_all(&meta).await?;
        let meta_range = meta_start..self.writer.current_pos();

        let managed_start = self.writer.current_pos();
        self.writer.write_all(&managed).await?;
        let managed_range = managed_start..self.writer.current_pos();

        let deletes_start = self.writer.current_pos();
        self.writer.write_all(&deletes).await?;

        let deletes_range = deletes_start..self.writer.current_pos();

        self.writer.add_file(META_FILE, meta_range);
        self.writer.add_file(MANAGED_FILE, managed_range);
        self.writer.add_file(DELETES_FILE, deletes_range);

        self.writer.finalise().await
    }

    /// Abort the segment creation.
    pub async fn abort(self) -> io::Result<()> {
        self.writer.abort().await
    }

    async fn write_file(
        &mut self,
        reader: &mut File,
        range: Range<u64>,
    ) -> io::Result<()> {
        let len = range.end - range.start;
        let mut bytes_written = 0;
        let mut buffer = [0; BUFFER_SIZE];
        while bytes_written < len {
            let n = reader.read(&mut buffer[..]).await?;

            if n == 0 {
                break;
            }

            let n = cmp::min(len - bytes_written, n as u64) as usize;
            self.writer.write_all(&buffer[..n]).await?;

            bytes_written += n as u64;
        }

        // Reset the cursor incase we read over the number of bytes we wanted.
        reader.seek(SeekFrom::Start(range.end)).await?;

        Ok(())
    }

    async fn merge_special_files(
        &mut self,
        reader: &mut File,
        range: Range<u64>,
        path: &str,
    ) -> io::Result<()> {
        match path {
            p if p == META_FILE => {
                let data = read_range(reader, range.clone()).await?;

                let meta = MetaFile::from_json(&data)
                    .map_err(|e| io::Error::new(ErrorKind::Other, e))?;

                self.meta_file.merge(meta);
            },
            p if p == MANAGED_FILE => {
                let data = read_range(reader, range.clone()).await?;

                let meta = ManagedMeta::from_json(&data)
                    .map_err(|e| io::Error::new(ErrorKind::Other, e))?;

                self.managed_file.merge(meta);
            },
            p if p == DELETES_FILE => {
                let data = read_range(reader, range.clone()).await?;

                let deletes = Deletes::from_compressed_bytes(data).await?;

                self.deletes.merge(deletes);
            },
            _ => return Ok(()),
        };

        // Reset the cursor incase we read over the number of bytes we wanted.
        reader.seek(SeekFrom::Start(range.end)).await?;

        Ok(())
    }
}

/// Reads a set of bytes from the given file and makes sure the cursor is correctly
/// repositioned if more than the required amount of bytes are read.
///
/// NOTE:
///  The start of the `range` is ignored.
async fn read_range(reader: &mut File, range: Range<u64>) -> io::Result<Vec<u8>> {
    reader.seek(SeekFrom::Start(range.start)).await?;

    let mut data = vec![];

    let len = range.end - range.start;
    let mut bytes_written = 0;
    let mut buffer = [0; BUFFER_SIZE];
    while bytes_written < len {
        let n = reader.read(&mut buffer[..]).await?;

        if n == 0 {
            break;
        }

        let n = cmp::min(len - bytes_written, n as u64) as usize;
        data.extend_from_slice(&buffer[..n]);

        bytes_written += n as u64;
    }

    Ok(data)
}

#[cfg(test)]
mod tests {
    use std::env::temp_dir;
    use std::path::PathBuf;

    use datacake_crdt::get_unix_timestamp_ms;

    use super::*;
    use crate::blocking::exporter::BlockingExporter;

    async fn create_test_segments(
        prefix: &str,
        num: usize,
        clock: &mut HLCTimestamp,
    ) -> io::Result<(Vec<PathBuf>, Vec<PathBuf>, Vec<HLCTimestamp>)> {
        let mut segments = vec![];
        let mut sample_files = vec![];
        let mut history = vec![];

        for i in 0..num {
            let sample_file = temp_dir().join(format!("{}-sample-{}.txt", prefix, i));

            let segment_id = clock.send().unwrap();
            let path =
                temp_dir().join(format!("combined-seg-{}-{}-test.segment", prefix, i));

            let mut exporter =
                BlockingExporter::create(&path, 0, "test-index".to_string(), segment_id)
                    .await?;

            exporter.write_raw(&sample_file, b"Hello, World!").await?;
            exporter.finalise().await?;

            history.push(segment_id);
            sample_files.push(sample_file);
            segments.push(path);
        }

        Ok((segments, sample_files, history))
    }

    #[tokio::test]
    async fn test_single_merge_combiner() -> io::Result<()> {
        let mut clock = HLCTimestamp::new(get_unix_timestamp_ms(), 0, 0);
        let segment_id = clock.send().unwrap();

        let (segments, expected_files, history) =
            create_test_segments("single-merge", 2, &mut clock).await?;

        let combiner_file = temp_dir().join("combiner-single.segment");
        let mut combiner = BlockingCombiner::create(
            &combiner_file,
            "test-index".to_string(),
            segment_id,
        )
        .await?;

        for segment_path in segments {
            combiner.combine_segment(&segment_path).await?;
        }

        let file = combiner.finalise().await?;
        let mut file = File::open(file).await?;

        // Read it like a new file.
        file.seek(SeekFrom::Start(0)).await?;
        let metadata = read_metadata(&mut file).await?;

        assert_eq!(
            metadata.index(),
            "test-index",
            "Expected metadata index to be the same as what is provided to combiner."
        );

        for expected_file in expected_files {
            let file = metadata.get_file_bounds(&expected_file);
            assert!(
                file.is_some(),
                "Expected file to be transferred during combining {:?}",
                expected_file
            );
        }

        assert_eq!(
            metadata.history(),
            &history,
            "Expected segment history to match."
        );

        assert_eq!(
            metadata.segment_id(),
            segment_id,
            "Expected segment id to match provided id."
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_multi_merge_combiner() -> io::Result<()> {
        let mut clock = HLCTimestamp::new(get_unix_timestamp_ms(), 0, 0);
        let segment_id = clock.send().unwrap();

        let (segments, expected_files, history) =
            create_test_segments("multi-merge", 5, &mut clock).await?;

        let combiner_file = temp_dir().join("combiner-single.segment");
        let mut combiner = BlockingCombiner::create(
            &combiner_file,
            "test-index".to_string(),
            segment_id,
        )
        .await?;

        for segment_path in segments {
            combiner.combine_segment(&segment_path).await?;
        }

        let file = combiner.finalise().await?;
        let mut file = File::open(file).await?;

        // Read it like a new file.
        file.seek(SeekFrom::Start(0)).await?;
        let metadata = read_metadata(&mut file).await?;

        assert_eq!(
            metadata.index(),
            "test-index",
            "Expected metadata index to be the same as what is provided to combiner."
        );

        for expected_file in expected_files {
            let file = metadata.get_file_bounds(&expected_file);
            assert!(
                file.is_some(),
                "Expected file to be transferred during combining {:?}",
                expected_file
            );
        }

        assert_eq!(
            metadata.history(),
            &history,
            "Expected segment history to match."
        );

        assert_eq!(
            metadata.segment_id(),
            segment_id,
            "Expected segment id to match provided id."
        );

        Ok(())
    }

    async fn create_segment_with(
        name: &str,
        segment_id: HLCTimestamp,
        meta: Option<MetaFile>,
        managed: Option<ManagedMeta>,
        deletes: Option<Deletes>,
    ) -> io::Result<PathBuf> {
        let dir = temp_dir();

        let path = dir.join(format!("combined-seg-{}-test.segment", name));
        let mut exporter =
            BlockingExporter::create(&path, 0, "test-index".to_string(), segment_id)
                .await?;

        if let Some(meta) = meta {
            exporter
                .write_raw(Path::new(META_FILE), &meta.to_json().unwrap())
                .await?;
        }

        if let Some(managed) = managed {
            exporter
                .write_raw(Path::new(MANAGED_FILE), &managed.to_json().unwrap())
                .await?;
        }

        if let Some(deletes) = deletes {
            let buf = deletes.to_compressed_bytes().await?;
            exporter.write_raw(Path::new(DELETES_FILE), &buf).await?;
        }

        exporter.finalise().await?;

        Ok(path)
    }

    #[tokio::test]
    async fn test_meta_combiner() -> io::Result<()> {
        let mut clock = HLCTimestamp::new(get_unix_timestamp_ms(), 0, 0);

        let meta_1 = MetaFile {
            segments: vec![serde_json::Value::String("node-1-field".into())],
            opstamp: 1,
            ..Default::default()
        };

        let meta_2 = MetaFile {
            segments: vec![serde_json::Value::String("node-2-field".into())],
            opstamp: 4,
            ..Default::default()
        };

        let segment_1 = create_segment_with(
            "meta-segment-1",
            clock.send().unwrap(),
            Some(meta_1),
            None,
            None,
        )
        .await?;
        let segment_2 = create_segment_with(
            "meta-segment-2",
            clock.send().unwrap(),
            Some(meta_2),
            None,
            None,
        )
        .await?;

        let path = temp_dir().join("combined-seg-combiner-meta-test.segment");
        let mut combiner = BlockingCombiner::create(
            &path,
            "test-index".to_string(),
            clock.send().unwrap(),
        )
        .await?;
        combiner.combine_segment(&segment_1).await?;
        combiner.combine_segment(&segment_2).await?;

        let file = combiner.finalise().await?;
        let mut file = File::open(file).await?;

        // Read it like a new file.
        file.seek(SeekFrom::Start(0)).await?;
        let metadata = read_metadata(&mut file).await?;

        assert_eq!(
            metadata.index(),
            "test-index",
            "Expected metadata index to be the same as what is provided to ."
        );

        let ranger = metadata
            .get_file_bounds(Path::new(META_FILE))
            .expect("Expected meta file to exist within new segment.");

        let file = read_range(&mut file, ranger.start as u64..ranger.end as u64).await?;
        let meta = MetaFile::from_json(&file).expect("Meta file should not be corrupt.");

        assert_eq!(
            meta.opstamp, 4,
            "Expected higher opstamp to be taken when merging meta.json"
        );
        assert_eq!(
            meta.segments,
            vec![
                serde_json::Value::String("node-1-field".into()),
                serde_json::Value::String("node-2-field".into()),
            ],
            "Expected merged segments to match provided example.",
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_managed_combiner() -> io::Result<()> {
        let mut clock = HLCTimestamp::new(get_unix_timestamp_ms(), 0, 0);

        let managed_1 = ManagedMeta(vec!["node-1-field".to_string()]);
        let managed_2 = ManagedMeta(vec!["node-2-field".to_string()]);

        let segment_1 = create_segment_with(
            "managed-segment-1",
            clock.send().unwrap(),
            None,
            Some(managed_1),
            None,
        )
        .await?;
        let segment_2 = create_segment_with(
            "managed-segment-2",
            clock.send().unwrap(),
            None,
            Some(managed_2),
            None,
        )
        .await?;

        let path = temp_dir().join("combined-seg-combiner-managed-test.segment");
        let mut combiner = BlockingCombiner::create(
            &path,
            "test-index".to_string(),
            clock.send().unwrap(),
        )
        .await?;
        combiner.combine_segment(&segment_1).await?;
        combiner.combine_segment(&segment_2).await?;

        let file = combiner.finalise().await?;
        let mut file = File::open(file).await?;

        // Read it like a new file.
        file.seek(SeekFrom::Start(0)).await?;
        let metadata = read_metadata(&mut file).await?;

        assert_eq!(
            metadata.index(),
            "test-index",
            "Expected metadata index to be the same as what is provided to ."
        );

        let ranger = metadata
            .get_file_bounds(Path::new(MANAGED_FILE))
            .expect("Expected managed file to exist within new segment.");

        let file = read_range(&mut file, ranger.start as u64..ranger.end as u64).await?;
        let managed =
            ManagedMeta::from_json(&file).expect("Managed file should not be corrupt.");

        assert_eq!(
            managed.0,
            vec!["node-1-field".to_string(), "node-2-field".to_string(),],
            "Expected merged segments to match provided example.",
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_deletes_combiner() -> io::Result<()> {
        let mut clock = HLCTimestamp::new(get_unix_timestamp_ms(), 0, 0);

        let deletes_1 = Deletes(vec!["node-1-field".to_string()]);
        let deletes_2 = Deletes(vec!["node-2-field".to_string()]);

        let segment_1 = create_segment_with(
            "deletes-segment-1",
            clock.send().unwrap(),
            None,
            None,
            Some(deletes_1),
        )
        .await?;
        let segment_2 = create_segment_with(
            "deletes-segment-2",
            clock.send().unwrap(),
            None,
            None,
            Some(deletes_2),
        )
        .await?;

        let path = temp_dir().join("combined-seg-combiner-deletes-test.segment");

        let mut combiner = BlockingCombiner::create(
            &path,
            "test-index".to_string(),
            clock.send().unwrap(),
        )
        .await?;

        combiner.combine_segment(&segment_1).await?;
        combiner.combine_segment(&segment_2).await?;

        let file = combiner.finalise().await?;
        let mut file = File::open(file).await?;

        // Read it like a new file.
        file.seek(SeekFrom::Start(0)).await?;
        let metadata = read_metadata(&mut file).await?;

        assert_eq!(
            metadata.index(),
            "test-index",
            "Expected metadata index to be the same as what is provided to ."
        );

        let ranger = metadata
            .get_file_bounds(Path::new(DELETES_FILE))
            .expect("Expected managed file to exist within new segment.");

        let file = read_range(&mut file, ranger.start as u64..ranger.end as u64).await?;

        let managed = Deletes::from_compressed_bytes(file)
            .await
            .expect("Deletes file should not be corrupt.");

        assert_eq!(
            managed.0,
            vec!["node-1-field".to_string(), "node-2-field".to_string(),],
            "Expected merged segments to match provided example.",
        );

        Ok(())
    }
}
