use std::io;
use std::path::{Path, PathBuf};
use datacake_crdt::HLCTimestamp;
use once_cell::sync::OnceCell;

use crate::aio::combiner::AioCombiner;
use crate::aio::exporter::AioExporter;
use crate::aio::runtime::AioRuntime;

use crate::blocking::combiner::BlockingCombiner;
use crate::blocking::exporter::BlockingExporter;
use crate::SpecialFile;

static RUNTIME: OnceCell<AioRuntime> = OnceCell::new();

pub enum AutoExporter {
    Blocking(Box<BlockingExporter>),
    Aio(AioExporter),
}

impl AutoExporter {
    /// Create a new exporter.
    pub async fn create(
        path: &Path,
        size_hint: usize,
        index: String,
        segment_id: HLCTimestamp,
    ) -> io::Result<Self> {
        let rt = match RUNTIME.get() {
            Some(rt) => rt,
            None => match crate::aio::create_runtime(calculate_io_threads()) {
                Ok(rt) => {
                    RUNTIME.get_or_init(|| rt)
                },
                Err(_) => {
                    let combiner = BlockingExporter::create(path, size_hint, index, segment_id)
                        .await?;

                    return Ok(Self::Blocking(Box::new(combiner)));
                },
            },
        };

        let combiner = AioExporter::create(rt,  path, size_hint, index, segment_id)
            .await?;

        Ok(Self::Aio(combiner))
    }

    /// Write a new file to the exporter. The file path will be read and streamed from into
    /// the new segment.
    pub async fn write_file(&mut self, path: &Path) -> io::Result<()> {
        match self {
            Self::Aio(exporter) => exporter.write_file(path).await,
            Self::Blocking(exporter) => exporter.write_file(path).await,
        }
    }

    /// Write a new file to the exporter.
    ///
    /// Unlike the `write_file` method, this method takes a raw buffer which gets
    /// written out instead.
    pub async fn write_raw(&mut self, path: &Path, buf: Vec<u8>) -> io::Result<()> {
        match self {
            Self::Aio(exporter) => exporter.write_raw(path, buf).await,
            Self::Blocking(exporter) => exporter.write_raw(path, buf).await,
        }
    }

    /// Merges a special file into the current segment state.
    pub async fn write_special_file(&mut self, special: SpecialFile) -> io::Result<()> {
        match self {
            Self::Aio(exporter) => exporter.write_special_file(special).await,
            Self::Blocking(exporter) => exporter.write_special_file(special).await,
        }
    }

    /// Finalises any remaining buffers so that file is safely persisted to disk.
    pub async fn finalise(self) -> io::Result<PathBuf> {
        match self {
            Self::Aio(exporter) => exporter.finalise().await,
            Self::Blocking(exporter) => exporter.finalise().await,
        }
    }

    /// Abort the segment creation.
    pub async fn abort(self) -> io::Result<()> {
        match self {
            Self::Aio(exporter) => exporter.abort().await,
            Self::Blocking(exporter) => exporter.abort().await,
        }
    }
}


pub enum AutoCombiner {
    Blocking(Box<BlockingCombiner>),
    Aio(AioCombiner),
}


impl AutoCombiner {
    /// Create a new exporter.
    pub async fn create(
        path: &Path,
        index: String,
        segment_id: HLCTimestamp,
    ) -> io::Result<Self> {


        let rt = match RUNTIME.get() {
            Some(rt) => rt,
            None => match crate::aio::create_runtime(calculate_io_threads()) {
                Ok(rt) => {
                    RUNTIME.get_or_init(|| rt)
                },
                Err(_) => {
                    let combiner = BlockingCombiner::create(path, index, segment_id)
                        .await?;

                    return Ok(Self::Blocking(Box::new(combiner)));
                },
            },
        };

        let combiner = AioCombiner::create(rt, path, index, segment_id)
            .await?;

        Ok(Self::Aio(combiner))
    }

    /// Write a new file to the exporter. The file path will be read and streamed from into
    /// the new segment.
    pub async fn combine_segment(&mut self, path: &Path) -> io::Result<()> {
        match self {
            Self::Aio(combiner) => combiner.combine_segment(path).await,
            Self::Blocking(combiner) => combiner.combine_segment(path).await,
        }
    }

    /// Finalises any remaining buffers so that file is safely persisted to disk.
    pub async fn finalise(self) -> io::Result<PathBuf> {
        match self {
            Self::Aio(combiner) => combiner.finalise().await,
            Self::Blocking(combiner) => combiner.finalise().await,
        }
    }

    /// Abort the segment creation.
    pub async fn abort(self) -> io::Result<()> {
        match self {
            Self::Aio(combiner) => combiner.abort().await,
            Self::Blocking(combiner) => combiner.abort().await,
        }
    }
}


fn calculate_io_threads() -> usize {
    let cpus = num_cpus::get();

    if cpus == 1 {
        return 1;
    }

    if cpus <= 8 {
        return cpus / 2;
    }

    8
}