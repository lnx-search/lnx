use std::io::{ErrorKind, SeekFrom};

use tokio::fs::File;
use tokio::io;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use crate::{get_metadata_offsets, Metadata, METADATA_HEADER_SIZE};

pub(crate) async fn read_metadata(file: &mut File) -> io::Result<Metadata> {
    let mut buffer = [0; METADATA_HEADER_SIZE];
    file.read_exact(&mut buffer).await?;

    let (start, len) = get_metadata_offsets(&buffer).map_err(|_| {
        io::Error::new(ErrorKind::InvalidData, "Unable to read index metadata.")
    })?;

    file.seek(SeekFrom::Start(start)).await?;

    let mut buffer = vec![];
    while buffer.len() < len as usize {
        let mut buff = [0; 1024];
        let n = file.read(&mut buff).await?;

        if n == 0 {
            break;
        }

        buffer.extend_from_slice(&buff[..n]);
    }

    let metadata = Metadata::from_bytes(&buffer[..len as usize])?;

    Ok(metadata)
}
