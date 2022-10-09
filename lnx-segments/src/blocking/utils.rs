use std::io::{ErrorKind, SeekFrom};
use std::ops::Range;
use std::cmp;

use tokio::fs::File;
use tokio::io;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use crate::{get_metadata_offsets, Metadata, METADATA_HEADER_SIZE};

pub(crate) async fn read_metadata(file: &mut File) -> io::Result<Metadata> {
    file.seek(SeekFrom::End(-(METADATA_HEADER_SIZE as i64))).await?;

    let mut buffer = [0; METADATA_HEADER_SIZE];
    file.read_exact(&mut buffer).await?;

    let (start, len) = get_metadata_offsets(&buffer).map_err(|_| {
        io::Error::new(ErrorKind::InvalidData, "Unable to read index metadata.")
    })?;

    file.seek(SeekFrom::Start(start)).await?;

    let mut buffer = vec![];
    while buffer.len() < len as usize {
        let mut buf = [0; 1024];
        let n = file.read(&mut buf).await?;

        if n == 0 {
            break;
        }

        buffer.extend_from_slice(&buf[..n]);
    }

    let metadata = Metadata::from_bytes(&buffer[..len as usize])?;

    Ok(metadata)
}

/// Reads a set of bytes from the given file and makes sure the cursor is correctly
/// repositioned if more than the required amount of bytes are read.
///
/// NOTE:
///  The start of the `range` is ignored.
pub(crate) async fn read_range(reader: &mut File, range: Range<u64>) -> io::Result<Vec<u8>> {
    reader.seek(SeekFrom::Start(range.start)).await?;

    let mut data = vec![];

    let len = range.end - range.start;
    let mut bytes_written = 0;
    let mut buffer = crate::new_buffer();
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