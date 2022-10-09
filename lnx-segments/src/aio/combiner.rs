use std::io;
use std::io::ErrorKind;
use std::ops::Range;
use std::path::{Path, PathBuf};

use datacake_crdt::HLCTimestamp;
use glommio::io::DmaStreamReader;
use tokio::sync::oneshot;

use crate::aio::runtime::{AioRuntime, AioTask};
use crate::aio::AioWriter;
use crate::SPECIAL_FILES;

pub struct AioCombiner {
    tx: flume::Sender<Op>,
}

impl AioCombiner {
    /// Create a new [AioCombiner] instance writing to the provided path.
    pub async fn create(
        rt: &AioRuntime,
        path: &Path,
        index: String,
        segment_id: HLCTimestamp,
    ) -> io::Result<Self> {
        let (tx, rx) = flume::bounded(1);
        let setup = AioCombinerActorSetup {
            path: path.to_path_buf(),
            index,
            segment_id,
            rx,
        };

        let task = AioTask::from(setup);

        rt.spawn_actor(task).await.map_err(|_| {
            io::Error::new(
                ErrorKind::Other,
                "The IO scheduler and runtime is not currently running.",
            )
        })?;

        Ok(Self { tx })
    }

    /// Ingests a segment file and writes it to the new segment.
    pub async fn combine_segment(&mut self, segment_file: &Path) -> io::Result<()> {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send_async(Op::Combine {
                tx,
                fp: segment_file.to_path_buf(),
            })
            .await
            .map_err(|_| {
                io::Error::new(ErrorKind::Other, "Background actor shutdown.")
            })?;

        rx.await
            .expect("Background actor died while handling task.")
    }

    /// Finalises any remaining buffers so that file is safely persisted to disk.
    pub async fn finalise(self) -> io::Result<PathBuf> {
        let (tx, rx) = oneshot::channel();

        self.tx.send_async(Op::Finalise { tx }).await.map_err(|_| {
            io::Error::new(ErrorKind::Other, "Background actor shutdown.")
        })?;

        rx.await
            .expect("Background actor died while handling task.")
    }

    /// Abort the segment creation.
    pub async fn abort(self) -> io::Result<()> {
        let (tx, rx) = oneshot::channel();

        self.tx.send_async(Op::Abort { tx }).await.map_err(|_| {
            io::Error::new(ErrorKind::Other, "Background actor shutdown.")
        })?;

        rx.await
            .expect("Background actor died while handling task.")
    }
}

pub(super) struct AioCombinerActorSetup {
    path: PathBuf,
    index: String,
    segment_id: HLCTimestamp,
    rx: flume::Receiver<Op>,
}

impl AioCombinerActorSetup {
    pub(super) async fn run_actor(self) -> io::Result<()> {
        let writer =
            AioWriter::create(&self.path, 0, self.index, self.segment_id).await?;

        let actor = AioCombinerActor {
            rx: self.rx,
            writer,
        };

        actor.run_actor().await;

        Ok(())
    }
}

pub struct AioCombinerActor {
    rx: flume::Receiver<Op>,
    writer: AioWriter,
}

impl AioCombinerActor {
    pub(crate) async fn run_actor(mut self) {
        while let Ok(op) = self.rx.recv_async().await {
            match op {
                Op::Abort { tx } => {
                    let res = self.abort().await;
                    let _ = tx.send(res);
                    break;
                },
                Op::Finalise { tx } => {
                    let res = self.finalise().await;
                    let _ = tx.send(res);
                    break;
                },
                Op::Combine { tx, fp } => {
                    let res = self.combine_segment(&fp).await;
                    let _ = tx.send(res);
                },
            }
        }
    }

    /// Ingests a segment file and writes it to the new segment.
    pub async fn combine_segment(&mut self, segment_file: &Path) -> io::Result<()> {
        let segment = glommio::io::ImmutableFileBuilder::new(segment_file)
            .build_existing()
            .await?;

        let metadata = super::utils::read_metadata(&segment).await?;

        let mut segment_reader = segment
            .stream_reader()
            .with_read_ahead(10)
            .with_buffer_size(crate::BUFFER_SIZE)
            .build();

        let mut files = metadata
            .files()
            .iter()
            .map(|v| (v.1.clone(), v.0.clone()))
            .collect::<Vec<_>>();

        // We want to sort them by starting index so we minimise seeking overhead.
        files.sort_by_key(|v| v.0.start);

        let mut buffer = crate::new_buffer();
        let mut buffer_offset = 0;
        for (range, path) in files {
            super::utils::ensure_reader_at_range_start(
                &mut segment_reader,
                &mut buffer[..],
                &mut buffer_offset,
                range.start,
            )
            .await?;

            if SPECIAL_FILES.contains(&path.as_str()) {
                self.merge_special_files(
                    &mut segment_reader,
                    &mut buffer[..],
                    &mut buffer_offset,
                    range,
                    Path::new(path.as_str()),
                )
                .await?;
                continue;
            }

            let start = self.writer.current_pos();
            self.write_file(
                &mut segment_reader,
                &mut buffer[..],
                &mut buffer_offset,
                range,
            )
            .await?;
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

    /// Copies the given range of bytes from the reader to the writer.
    async fn write_file(
        &mut self,
        reader: &mut DmaStreamReader,
        buffer: &mut [u8],
        buffer_offset: &mut usize,
        range: Range<u64>,
    ) -> io::Result<()> {
        let len = (range.end - range.start) as usize;
        super::utils::copy_data(&mut self.writer, reader, buffer, buffer_offset, len)
            .await?;

        Ok(())
    }

    /// Reads the full range of data into memory to handle the file.
    ///
    /// Special files are typically files which are small but need to be merged in a specific way
    /// as they will collide due to having the same name in each segment.
    async fn merge_special_files(
        &mut self,
        reader: &mut DmaStreamReader,
        buffer: &mut [u8],
        buffer_offset: &mut usize,
        range: Range<u64>,
        path: &Path,
    ) -> io::Result<()> {
        let n_bytes = (range.end - range.start) as usize;
        let data =
            super::utils::read_n_bytes(reader, buffer, buffer_offset, n_bytes).await?;

        let file = crate::deserialize_special_file(data, path)
            .await?;
        self.writer.write_special_file(file);

        Ok(())
    }
}

pub enum Op {
    Combine {
        tx: oneshot::Sender<io::Result<()>>,
        fp: PathBuf,
    },
    Abort {
        tx: oneshot::Sender<io::Result<()>>,
    },
    Finalise {
        tx: oneshot::Sender<io::Result<PathBuf>>,
    },
}

#[cfg(test)]
mod tests {
    use datacake_crdt::get_unix_timestamp_ms;
    use tokio::fs::File;

    use crate::{ManagedMeta, MetaFile, Deletes, MANAGED_FILE, META_FILE, DELETES_FILE, SpecialFile};
    use super::*;
    use crate::aio::exporter::AioExporter;

    async fn create_test_segments(
        rt: &AioRuntime,
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
                AioExporter::create(rt, &path, 0, "test-index".to_string(), segment_id)
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
        let rt = crate::aio::create_runtime(1)?;

        let mut clock = HLCTimestamp::new(get_unix_timestamp_ms(), 0, 0);
        let segment_id = clock.send().unwrap();

        let (segments, expected_files, history) =
            create_test_segments(&rt, 2, &mut clock).await?;

        let combiner_file = crate::get_random_tmp_file();
        let mut combiner = AioCombiner::create(
            &rt,
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
        let metadata = crate::blocking::utils::read_metadata(&mut file).await?;

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
        let rt = crate::aio::create_runtime(1)?;

        let mut clock = HLCTimestamp::new(get_unix_timestamp_ms(), 0, 0);
        let segment_id = clock.send().unwrap();

        let (segments, expected_files, history) =
            create_test_segments(&rt, 5, &mut clock).await?;

        let combiner_file = crate::get_random_tmp_file();
        let mut combiner = AioCombiner::create(
            &rt,
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
        let metadata = crate::blocking::utils::read_metadata(&mut file).await?;

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
        rt: &AioRuntime,
        segment_id: HLCTimestamp,
        meta: Option<MetaFile>,
        managed: Option<ManagedMeta>,
        deletes: Option<Deletes>,
    ) -> io::Result<PathBuf> {
        let path = crate::get_random_tmp_file();
        let mut exporter =
            AioExporter::create(rt, &path, 0, "test-index".to_string(), segment_id)
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
        let rt = crate::aio::create_runtime(1)?;

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
            create_segment_with(&rt, clock.send().unwrap(), Some(meta_1), None, None)
                .await?;
        let segment_2 =
            create_segment_with(&rt, clock.send().unwrap(), Some(meta_2), None, None)
                .await?;

        let path = crate::get_random_tmp_file();
        let mut combiner = AioCombiner::create(
            &rt,
            &path,
            "test-index".to_string(),
            clock.send().unwrap(),
        )
        .await?;
        combiner.combine_segment(&segment_1).await?;
        combiner.combine_segment(&segment_2).await?;

        let file = combiner.finalise().await?;
        let mut file = File::open(file).await?;
        let metadata = crate::blocking::utils::read_metadata(&mut file).await?;

        assert_eq!(
            metadata.index(),
            "test-index",
            "Expected metadata index to be the same as what is provided to ."
        );

        let range = metadata
            .get_file_bounds(Path::new(META_FILE))
            .expect("Expected meta file to exist within new segment.");

        let data = crate::blocking::utils::read_range(
            &mut file,
            range.start as u64..range.end as u64,
        )
        .await?;

        let meta = MetaFile::from_json(&data).expect("Meta file should not be corrupt.");

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
        let rt = crate::aio::create_runtime(1)?;

        let mut clock = HLCTimestamp::new(get_unix_timestamp_ms(), 0, 0);

        let managed_1 = ManagedMeta(vec!["node-1-field".to_string()]);
        let managed_2 = ManagedMeta(vec!["node-2-field".to_string()]);

        let segment_1 =
            create_segment_with(&rt, clock.send().unwrap(), None, Some(managed_1), None)
                .await?;
        let segment_2 =
            create_segment_with(&rt, clock.send().unwrap(), None, Some(managed_2), None)
                .await?;

        let path = crate::get_random_tmp_file();
        let mut combiner = AioCombiner::create(
            &rt,
            &path,
            "test-index".to_string(),
            clock.send().unwrap(),
        )
        .await?;
        combiner.combine_segment(&segment_1).await?;
        combiner.combine_segment(&segment_2).await?;

        let file = combiner.finalise().await?;
        let mut file = File::open(file).await?;
        let metadata = crate::blocking::utils::read_metadata(&mut file).await?;

        assert_eq!(
            metadata.index(),
            "test-index",
            "Expected metadata index to be the same as what is provided to ."
        );

        let range = metadata
            .get_file_bounds(Path::new(MANAGED_FILE))
            .expect("Expected managed file to exist within new segment.");

        let data = crate::blocking::utils::read_range(
            &mut file,
            range.start as u64..range.end as u64,
        )
        .await?;

        let managed =
            ManagedMeta::from_json(&data).expect("Managed file should not be corrupt.");

        assert_eq!(
            managed.0,
            vec!["node-1-field".to_string(), "node-2-field".to_string(),],
            "Expected merged segments to match provided example.",
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_deletes_combiner() -> io::Result<()> {
        let rt = crate::aio::create_runtime(1)?;

        let mut clock = HLCTimestamp::new(get_unix_timestamp_ms(), 0, 0);

        let deletes_1 = Deletes(vec!["node-1-field".to_string()]);
        let deletes_2 = Deletes(vec!["node-2-field".to_string()]);

        let segment_1 =
            create_segment_with(&rt, clock.send().unwrap(), None, None, Some(deletes_1))
                .await
                .expect("created segment 1");
        let segment_2 =
            create_segment_with(&rt, clock.send().unwrap(), None, None, Some(deletes_2))
                .await
                .expect("created segment 2");
        println!("Copying files");

        let path = crate::get_random_tmp_file();
        let mut combiner = AioCombiner::create(
            &rt,
            &path,
            "test-index".to_string(),
            clock.send().unwrap(),
        )
        .await
        .expect("created combiner");

        println!("Copying files");
        combiner
            .combine_segment(&segment_1)
            .await
            .expect("combine segment 1");
        combiner
            .combine_segment(&segment_2)
            .await
            .expect("combine segment 2");

        println!("Finalising file");
        let file = combiner.finalise().await.expect("finalise writer.");
        let mut file = File::open(file).await?;
        let metadata = crate::blocking::utils::read_metadata(&mut file)
            .await
            .expect("read metadata");

        assert_eq!(
            metadata.index(),
            "test-index",
            "Expected metadata index to be the same as what is provided to ."
        );

        let range = metadata
            .get_file_bounds(Path::new(DELETES_FILE))
            .expect("Expected managed file to exist within new segment.");

        let data = crate::blocking::utils::read_range(
            &mut file,
            range.start as u64..range.end as u64,
        )
        .await
        .expect("get range");

        let managed = Deletes::from_compressed_bytes(data)
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
