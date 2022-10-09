// use std::fs::File;
// use std::io;
// use std::path::Path;
// use datacake_crdt::HLCTimestamp;
//
// use crate::blocking::combiner::BlockingCombiner;
//
// pub enum AutoExporter {
//
// }
//
//
// pub enum AutoCombiner {
//     Blocking(BlockingCombiner)
// }
//
//
// impl AutoCombiner {
//     /// Create a new exporter.
//     pub async fn create(
//         path: &Path,
//         size_hint: usize,
//         index: String,
//         segment_id: HLCTimestamp,
//     ) -> io::Result<Self> {
//
//
//
//     }
//
//     /// Write a new file to the exporter. The file path will be read and streamed from into
//     /// the new segment.
//     pub async fn write_file(&mut self, path: &Path) -> io::Result<()> {
//     }
//
//     /// Write a new file to the exporter.
//     ///
//     /// Unlike the `write_file` method, this method takes a raw buffer which gets
//     /// written out instead.
//     pub async fn write_raw(&mut self, path: &Path, buf: &[u8]) -> io::Result<()> {
//     }
//
//     /// Finalises any remaining buffers so that file is safely persisted to disk.
//     pub async fn finalise(self) -> io::Result<File> {
//     }
//
//     /// Abort the segment creation.
//     pub async fn abort(self) -> io::Result<()> {
//     }
// }
