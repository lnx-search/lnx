use std::io;
use std::io::ErrorKind;
use std::ops::Range;
use std::path::{Path, PathBuf};
use datacake_crdt::HLCTimestamp;
use glommio::io::DmaStreamReader;
use tokio::sync::oneshot;
use crate::aio::{AioWriter, BUFFER_SIZE};
use crate::aio::runtime::AioTask;
use crate::deletes::Deletes;
use crate::meta_merger::{ManagedMeta, MetaFile};
use crate::{DELETES_FILE, MANAGED_FILE, META_FILE, SPECIAL_FILES};


pub struct AioCombiner {
    tx: flume::Sender<Op>,
}

impl AioCombiner {
    /// Create a new [AioCombiner] instance writing to the provided path.
    pub async fn create(
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

        super::runtime::spawn_actor(task)
            .await
            .map_err(|_| io::Error::new(
                ErrorKind::Other,
                "The IO scheduler and runtime is not currently running.",
            ))?;

        Ok(Self {
            tx
        })
    }


    /// Ingests a segment file and writes it to the new segment.
    pub async fn combine_segment(&mut self, segment_file: &Path) -> io::Result<()> {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send_async(Op::Combine { tx, fp: segment_file.to_path_buf() })
            .await
            .map_err(|_| io::Error::new(ErrorKind::Other, "Background actor shutdown."))?;

        rx.await.expect("Background actor died while handling task.")
    }


    /// Finalises any remaining buffers so that file is safely persisted to disk.
    pub async fn finalise(self) -> io::Result<PathBuf> {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send_async(Op::Finalise { tx })
            .await
            .map_err(|_| io::Error::new(ErrorKind::Other, "Background actor shutdown."))?;

        rx.await.expect("Background actor died while handling task.")
    }

    /// Abort the segment creation.
    pub async fn abort(self) -> io::Result<()> {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send_async(Op::Abort { tx })
            .await
            .map_err(|_| io::Error::new(ErrorKind::Other, "Background actor shutdown."))?;

        rx.await.expect("Background actor died while handling task.")
    }
}

pub(super) struct AioCombinerActorSetup {
    path: PathBuf,
    index: String,
    segment_id: HLCTimestamp,
    rx: flume::Receiver<Op>,
}

impl AioCombinerActorSetup {
    pub(super) async fn run_actor(self) -> io::Result<()>  {
        let writer = AioWriter::create(
            &self.path,
            0,
            self.index,
            self.segment_id,
        )
            .await?;

        let actor = AioCombinerActor {
            rx: self.rx,
            writer,
            deletes: Deletes::default(),
            meta_file: MetaFile::default(),
            managed_file: ManagedMeta::default(),
        };

        actor.run_actor().await;

        Ok(())
    }
}


pub struct AioCombinerActor {
    rx: flume::Receiver<Op>,
    writer: AioWriter,

    deletes: Deletes,
    meta_file: MetaFile,
    managed_file: ManagedMeta,
}

impl AioCombinerActor {
    pub(crate) async fn run_actor(mut self) {
        while let Ok(op) = self.rx.recv_async().await {
            match op {
                Op::Abort { tx }  => {
                    let res = self.abort().await;
                    let _ = tx.send(res);
                    break;
                },
                Op::Finalise { tx }  => {
                    let res = self.finalise().await;
                    let _ = tx.send(res);
                    break;
                },
                Op::Combine { tx, fp }  => {
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

        let mut segment_reader = segment.stream_reader()
            .with_read_ahead(10)
            .with_buffer_size(BUFFER_SIZE)
            .build();

        let mut files = metadata
            .files()
            .iter()
            .map(|v| (v.1.clone(), v.0.clone()))
            .collect::<Vec<_>>();

        // We want to sort them by starting index so we minimise seeking overhead.
        files.sort_by_key(|v| v.0.start);

        let mut buffer = super::new_buffer();
        let mut buffer_offset = 0;
        for (range, path) in files {
            super::utils::ensure_reader_at_range_start(
                &mut segment_reader,
                &mut buffer[..],
                &mut buffer_offset,
                range.start,
            ).await?;

            if SPECIAL_FILES.contains(&path.as_str()) {
                self.merge_special_files(
                    &mut segment_reader,
                    &mut buffer[..],
                    &mut buffer_offset,
                    range,
                    &path,
                ).await?;
                continue;
            }

            let start = self.writer.current_pos();
            self.write_file(
                &mut segment_reader,
                &mut buffer[..],
                &mut buffer_offset,
                range,
            ).await?;
            let end = self.writer.current_pos();

            self.writer.add_file(&path, start..end);
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

    /// Copies the given range of bytes from the reader to the writer.
    async fn write_file(
        &mut self,
        reader: &mut DmaStreamReader,
        buffer: &mut [u8],
        buffer_offset: &mut usize,
        range: Range<u64>,
    ) -> io::Result<()> {
        let len = (range.end - range.start) as usize;
        super::utils::copy_data(&mut self.writer, reader, buffer, buffer_offset, len).await?;

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
        path: &str,
    ) -> io::Result<()> {
        let n_bytes = (range.end - range.start) as usize;
        let buf = super::utils::read_n_bytes(reader, buffer, buffer_offset, n_bytes)
            .await?;

        match path {
            p if p == META_FILE => {
                let meta = MetaFile::from_json(&buf)
                    .map_err(|e| io::Error::new(ErrorKind::Other, e))?;

                self.meta_file.merge(meta);
            },
            p if p == MANAGED_FILE => {
                let meta = ManagedMeta::from_json(&buf)
                    .map_err(|e| io::Error::new(ErrorKind::Other, e))?;

                self.managed_file.merge(meta);
            },
            p if p == DELETES_FILE => {
                let deletes = Deletes::from_compressed_bytes(buf).await?;

                self.deletes.merge(deletes);
            },
            _ => return Ok(()),
        };

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
mod raw_tests {
    use std::env::temp_dir;
    use std::fs::File;
    use datacake_crdt::get_unix_timestamp_ms;
    use glommio::io::ImmutableFileBuilder;
    use crate::aio::utils::{ensure_reader_at_range_start, read_metadata, read_n_bytes};
    use super::*;
    use crate::run_aio;

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
        let mut file = ImmutableFileBuilder::new(&file)
            .build_existing()
            .await?;

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
        let mut file = ImmutableFileBuilder::new(&file)
            .build_existing()
            .await?;

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
        let mut combiner = AioCombiner::create(
            &path,
            "test-index".to_string(),
            clock.send().unwrap(),
        )
        .await?;
        combiner.combine_segment(&segment_1).await?;
        combiner.combine_segment(&segment_2).await?;

        let file = combiner.finalise().await?;
        let mut file = ImmutableFileBuilder::new(&file)
            .build_existing()
            .await?;

        let mut reader = file.stream_reader().build();

        let metadata = read_metadata(&mut file).await?;

        assert_eq!(
            metadata.index(),
            "test-index",
            "Expected metadata index to be the same as what is provided to ."
        );

        let range = metadata
            .get_file_bounds(Path::new(META_FILE))
            .expect("Expected meta file to exist within new segment.");

        let data = read_range(&mut reader, range).await?;

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
        let mut combiner = AioCombiner::create(
            &path,
            "test-index".to_string(),
            clock.send().unwrap(),
        )
        .await?;
        combiner.combine_segment(&segment_1).await?;
        combiner.combine_segment(&segment_2).await?;

        let file = combiner.finalise().await?;
        let mut file = ImmutableFileBuilder::new(&file)
            .build_existing()
            .await?;

        let mut reader = file.stream_reader().build();

        // Read it like a new file.
        let metadata = read_metadata(&mut file).await?;

        assert_eq!(
            metadata.index(),
            "test-index",
            "Expected metadata index to be the same as what is provided to ."
        );

        let range = metadata
            .get_file_bounds(Path::new(MANAGED_FILE))
            .expect("Expected managed file to exist within new segment.");

        let data = read_range(&mut reader, range).await?;

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
        let mut combiner = AioCombiner::create(
            &path,
            "test-index".to_string(),
            clock.send().unwrap(),
        )
        .await?;

        combiner.combine_segment(&segment_1).await?;
        combiner.combine_segment(&segment_2).await?;

        let file = combiner.finalise().await?;
        let mut file = ImmutableFileBuilder::new(&file)
            .build_existing()
            .await?;

        let mut reader = file.stream_reader().build();

        let metadata = read_metadata(&mut file).await?;

        assert_eq!(
            metadata.index(),
            "test-index",
            "Expected metadata index to be the same as what is provided to ."
        );

        let range = metadata
            .get_file_bounds(Path::new(DELETES_FILE))
            .expect("Expected managed file to exist within new segment.");

        let data = read_range(&mut reader, range).await?;

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

    async fn read_range(
        reader: &mut DmaStreamReader,
        range: Range<usize>,
    ) -> io::Result<Vec<u8>> {
        let mut buffer = crate::aio::new_buffer();
        let mut buffer_offset = 0;
        ensure_reader_at_range_start(
            reader,
            &mut buffer[..],
            &mut buffer_offset,
            range.start as u64,
        ).await?;

        let len = range.end - range.start;
        let file = read_n_bytes(reader, &mut buffer, &mut buffer_offset, len).await?;

        Ok(file)
    }
}

#[cfg(test)]
mod actor_tests {
    use super::*;
    use crate::run_aio;
}
