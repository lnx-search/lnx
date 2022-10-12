use std::io;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use datacake_crdt::HLCTimestamp;
use tokio::sync::oneshot;

use crate::aio::runtime::{AioRuntime, AioTask};
use crate::aio::AioWriter;
use crate::{SpecialFile, IGNORED_FILES, IGNORED_PREFIX, SPECIAL_FILES};

pub struct AioExporter {
    tx: flume::Sender<Op>,
}

impl AioExporter {
    #[instrument(name = "aio-exporter", skip(rt))]
    /// Create a new [AioCombiner] instance writing to the provided path.
    pub async fn create(
        rt: &AioRuntime,
        path: &Path,
        size_hint: usize,
        index: String,
        segment_id: HLCTimestamp,
    ) -> io::Result<Self> {
        let (tx, rx) = flume::bounded(1);
        let setup = AioExporterActorSetup {
            path: path.to_path_buf(),
            index,
            size_hint,
            segment_id,
            rx,
        };

        let task = AioTask::from(setup);

        let (set_ready, ready) = oneshot::channel();
        rt.spawn_actor(task, set_ready)
            .await
            .map_err(|_| {
                io::Error::new(
                    ErrorKind::Other,
                    "The IO scheduler and runtime is not currently running.",
                )
            })?;

        ready.await.expect("Task was unexpectedly dropped.")?;

        info!("New exporter created!");
        
        Ok(Self { tx })
    }

    /// Write a new file to the exporter. The file path will be read and streamed from into
    /// the new segment.
    pub async fn write_file(&mut self, path: &Path) -> io::Result<()> {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send_async(Op::AddFile {
                tx,
                fp: path.to_path_buf(),
            })
            .await
            .map_err(|_| {
                io::Error::new(ErrorKind::Other, "Background actor shutdown.")
            })?;

        rx.await
            .expect("Background actor died while handling task.")
    }

    /// Write a new file to the exporter.
    ///
    /// Unlike the `write_file` method, this method takes a raw buffer which gets
    /// written out instead.
    pub async fn write_raw(&mut self, path: &Path, buff: Vec<u8>) -> io::Result<()> {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send_async(Op::AddBuffer {
                tx,
                buff,
                fp: path.to_path_buf(),
            })
            .await
            .map_err(|_| {
                io::Error::new(ErrorKind::Other, "Background actor shutdown.")
            })?;

        rx.await
            .expect("Background actor died while handling task.")
    }

    /// Writes a special file to the underlying writer.
    pub async fn write_special_file(&mut self, file: SpecialFile) -> io::Result<()> {
        self.tx
            .send_async(Op::AddSpecialFile { file })
            .await
            .map_err(|_| io::Error::new(ErrorKind::Other, "Background actor shutdown."))
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

pub(super) struct AioExporterActorSetup {
    path: PathBuf,
    index: String,
    size_hint: usize,
    segment_id: HLCTimestamp,
    rx: flume::Receiver<Op>,
}

impl AioExporterActorSetup {
    pub(super) async fn run_actor(self) -> io::Result<()> {
        let writer =
            AioWriter::create(&self.path, self.size_hint, self.index, self.segment_id)
                .await?;

        let actor = AioExporterActor {
            rx: self.rx,
            writer,
        };

        glommio::spawn_local(actor.run_actor()).detach();

        Ok(())
    }
}

pub struct AioExporterActor {
    rx: flume::Receiver<Op>,
    writer: AioWriter,
}

impl AioExporterActor {
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
                Op::AddFile { tx, fp } => {
                    let res = self.write_file(&fp).await;
                    let _ = tx.send(res);
                },
                Op::AddBuffer { tx, fp, buff } => {
                    let res = self.write_raw(&fp, &buff).await;
                    let _ = tx.send(res);
                },
                Op::AddSpecialFile { file } => {
                    self.write_special_file(file);
                },
            }
        }
    }

    /// Write a new file to the exporter. The file path will be read and streamed from into
    /// the new segment.
    async fn write_file(&mut self, path: &Path) -> io::Result<()> {
        let path_str = path.to_string_lossy();

        if path_str.starts_with(IGNORED_PREFIX)
            || IGNORED_FILES.contains(&path_str.as_ref())
        {
            return Ok(());
        }

        let segment = glommio::io::ImmutableFileBuilder::new(path)
            .build_existing()
            .await?;

        if SPECIAL_FILES.contains(&path_str.as_ref()) {
            let buf = segment.read_at(0, segment.file_size() as usize).await?;
            let file =
                crate::deserialize_special_file(buf.as_ref().to_vec(), path).await?;
            self.write_special_file(file);
            return Ok(());
        }

        let mut reader = segment
            .stream_reader()
            .with_read_ahead(10)
            .with_buffer_size(crate::BUFFER_SIZE)
            .build();

        let start = self.writer.current_pos() as u64;
        let mut buffer = crate::new_buffer();
        let mut buffer_offset = 0;

        super::utils::copy_data(
            &mut self.writer,
            &mut reader,
            &mut buffer[..],
            &mut buffer_offset,
            segment.file_size() as usize,
        )
        .await?;

        let end = self.writer.current_pos();

        self.writer.add_file(path, start..end);

        Ok(())
    }

    /// Write a new file to the exporter.
    ///
    /// Unlike the `write_file` method, this method takes a raw buffer which gets
    /// written out instead.
    pub async fn write_raw(&mut self, path: &Path, buf: &[u8]) -> io::Result<()> {
        let start = self.writer.current_pos();
        self.writer.write_all(buf).await?;
        let end = self.writer.current_pos();

        self.writer.add_file(path, start..end);

        Ok(())
    }

    /// Merges a special file into the current segment state.
    pub fn write_special_file(&mut self, special: SpecialFile) {
        self.writer.write_special_file(special)
    }

    /// Finalises any remaining buffers so that file is safely persisted to disk.
    pub async fn finalise(self) -> io::Result<PathBuf> {
        self.writer.finalise().await
    }

    /// Abort the segment creation.
    pub async fn abort(self) -> io::Result<()> {
        self.writer.abort().await
    }
}

pub enum Op {
    AddFile {
        tx: oneshot::Sender<io::Result<()>>,
        fp: PathBuf,
    },
    AddBuffer {
        tx: oneshot::Sender<io::Result<()>>,
        fp: PathBuf,
        buff: Vec<u8>,
    },
    AddSpecialFile {
        file: SpecialFile,
    },
    Finalise {
        tx: oneshot::Sender<io::Result<PathBuf>>,
    },
    Abort {
        tx: oneshot::Sender<io::Result<()>>,
    },
}

#[cfg(test)]
mod tests {
    use std::fs;

    use datacake_crdt::get_unix_timestamp_ms;
    use tokio::fs::File;

    use super::*;

    #[tokio::test]
    async fn test_exporter_create_and_finalise() -> io::Result<()> {
        let rt = crate::aio::create_runtime(1)?;

        let segment_id = HLCTimestamp::new(get_unix_timestamp_ms(), 0, 0);
        let path = crate::get_random_tmp_file();

        let exporter =
            AioExporter::create(&rt, &path, 0, "test-index".to_string(), segment_id)
                .await?;

        let file = exporter.finalise().await?;
        let mut file = File::open(file).await?;
        let metadata = crate::blocking::utils::read_metadata(&mut file).await?;

        assert_eq!(
            metadata.index(),
            "test-index",
            "Expected metadata index to be the same as what is provided to exporter."
        );
        assert!(
            metadata.files().is_empty(),
            "Expected metadata files index to be empty."
        );
        assert_eq!(
            metadata.segment_id(),
            segment_id,
            "Expected segment id to match provided id."
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_exporter_create_and_abort() -> io::Result<()> {
        let rt = crate::aio::create_runtime(1)?;

        let segment_id = HLCTimestamp::new(get_unix_timestamp_ms(), 0, 0);
        let path = crate::get_random_tmp_file();

        let exporter =
            AioExporter::create(&rt, &path, 0, "test-index".to_string(), segment_id)
                .await?;
        exporter.abort().await?;

        assert!(
            !path.exists(),
            "Expected segment to no longer exist after abort."
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_exporter() -> io::Result<()> {
        let rt = crate::aio::create_runtime(1)?;

        let sample_file = crate::get_random_tmp_file();
        fs::write(&sample_file, b"Hello, world!")?;

        let segment_id = HLCTimestamp::new(get_unix_timestamp_ms(), 0, 0);
        let path = crate::get_random_tmp_file();

        let mut exporter =
            AioExporter::create(&rt, &path, 0, "test-index".to_string(), segment_id)
                .await?;
        exporter.write_file(&sample_file).await?;

        let file = exporter.finalise().await?;
        let mut file = File::open(file).await?;
        let metadata = crate::blocking::utils::read_metadata(&mut file).await?;

        assert_eq!(
            metadata.index(),
            "test-index",
            "Expected metadata index to be the same as what is provided to exporter."
        );
        assert_eq!(
            metadata.files().get(sample_file.to_string_lossy().as_ref()),
            Some(&(0..13)),
            "Expected metadata files index to be empty.",
        );
        assert_eq!(
            metadata.segment_id(),
            segment_id,
            "Expected segment id to match provided id."
        );

        Ok(())
    }
}
