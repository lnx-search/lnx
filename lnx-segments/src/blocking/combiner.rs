use std::io::SeekFrom;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::{cmp, io};

use datacake_crdt::HLCTimestamp;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use super::utils::read_range;
use crate::blocking::BlockingWriter;
use crate::SPECIAL_FILES;

/// Combines two or more existing segments into a single, larger segment.
///
/// This is the blocking variant of the combiners which wraps the `tokio::fs` system
/// which internally wraps the `std::fs` system in a threadpool.
///
/// This system is not the most efficient approach, it is designed for compatibility with
/// all operating systems rather than for performance.
pub struct BlockingCombiner {
    writer: BlockingWriter,
}

impl BlockingCombiner {
    /// Create a new [BlockingCombiner] instance writing to the provided path.
    pub async fn create(
        path: &Path,
        index: String,
        segment_id: HLCTimestamp,
    ) -> io::Result<Self> {
        let writer = BlockingWriter::create(path, 0, index, segment_id).await?;

        Ok(Self { writer })
    }

    /// Combines a segment into the new segment.
    ///
    /// This can be called multiple times, combining several smaller segments into a single,
    /// larger segment.
    pub async fn combine_segment(&mut self, segment_file: &Path) -> io::Result<()> {
        let mut segment = File::open(segment_file).await?;
        let metadata = super::utils::read_metadata(&mut segment).await?;

        let mut files = metadata
            .files()
            .iter()
            .map(|v| (v.1.clone(), v.0.clone()))
            .collect::<Vec<_>>();

        // We want to sort them by starting index so we minimise seeking overhead.
        files.sort_by_key(|v| v.0.start);

        // Set the seek position back to the end of the metadata.
        let mut cursor = 0;
        segment.seek(SeekFrom::Start(cursor)).await?;

        for (range, path) in files {
            let start = self.writer.current_pos();

            if range.start != cursor {
                segment.seek(SeekFrom::Start(range.start)).await?;
            }

            cursor += range.end - range.start;

            if SPECIAL_FILES.contains(&path.as_str()) {
                self.merge_special_files(&mut segment, range, Path::new(path.as_str()))
                    .await?;
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
    pub async fn finalise(self) -> io::Result<PathBuf> {
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
        let mut buffer = crate::new_buffer();
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
        path: &Path,
    ) -> io::Result<()> {
        let data = read_range(reader, range.clone()).await?;

        let file = crate::deserialize_special_file(data, path).await?;
        self.writer.write_special_file(file);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use datacake_crdt::get_unix_timestamp_ms;

    use super::*;
    use crate::blocking::exporter::BlockingExporter;
    use crate::blocking::utils::read_metadata;
    use crate::{
        Delete,
        DeleteValue,
        Deletes,
        ManagedMeta,
        MetaFile,
        SpecialFile,
        DELETES_FILE,
        MANAGED_FILE,
        META_FILE,
    };

    async fn create_test_segments(
        num: usize,
        clock: &mut HLCTimestamp,
    ) -> io::Result<(Vec<PathBuf>, Vec<PathBuf>, Vec<HLCTimestamp>)> {
        let mut segments = vec![];
        let mut sample_files = vec![];
        let mut history = vec![];

        for _ in 0..num {
            let sample_file = crate::get_random_tmp_file();

            let segment_id = clock.send().unwrap();
            let path = crate::get_random_tmp_file();

            let mut exporter =
                BlockingExporter::create(&path, 0, "test-index".to_string(), segment_id)
                    .await?;

            exporter
                .write_raw(&sample_file, b"Hello, World!".to_vec())
                .await?;
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
            create_test_segments(2, &mut clock).await?;

        let combiner_file = crate::get_random_tmp_file();

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
            create_test_segments(5, &mut clock).await?;

        let combiner_file = crate::get_random_tmp_file();

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
        segment_id: HLCTimestamp,
        meta: Option<MetaFile>,
        managed: Option<ManagedMeta>,
        deletes: Option<Deletes>,
    ) -> io::Result<PathBuf> {
        let path = crate::get_random_tmp_file();
        let mut exporter =
            BlockingExporter::create(&path, 0, "test-index".to_string(), segment_id)
                .await?;

        if let Some(meta) = meta {
            exporter.write_special_file(SpecialFile::Meta(meta)).await?;
        }

        if let Some(managed) = managed {
            exporter
                .write_special_file(SpecialFile::Managed(managed))
                .await?;
        }

        if let Some(deletes) = deletes {
            exporter
                .write_special_file(SpecialFile::Deletes(deletes))
                .await?;
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

        let segment_1 =
            create_segment_with(clock.send().unwrap(), Some(meta_1), None, None).await?;
        let segment_2 =
            create_segment_with(clock.send().unwrap(), Some(meta_2), None, None).await?;

        let path = crate::get_random_tmp_file();

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

        let segment_1 =
            create_segment_with(clock.send().unwrap(), None, Some(managed_1), None)
                .await?;
        let segment_2 =
            create_segment_with(clock.send().unwrap(), None, Some(managed_2), None)
                .await?;

        let path = crate::get_random_tmp_file();
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

        let expected_deletes = vec![
            Delete {
                field: "node-1-field".to_string(),
                value: DeleteValue::U64(0),
            },
            Delete {
                field: "node-2-field".to_string(),
                value: DeleteValue::U64(0),
            },
        ];

        let deletes_1 = Deletes(vec![expected_deletes[0].clone()]);
        let deletes_2 = Deletes(vec![expected_deletes[1].clone()]);

        let segment_1 =
            create_segment_with(clock.send().unwrap(), None, None, Some(deletes_1))
                .await?;
        let segment_2 =
            create_segment_with(clock.send().unwrap(), None, None, Some(deletes_2))
                .await?;

        let path = crate::get_random_tmp_file();

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
            managed.0, expected_deletes,
            "Expected merged segments to match provided example.",
        );

        Ok(())
    }
}
