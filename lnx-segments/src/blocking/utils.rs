use std::io::{ErrorKind, SeekFrom};

use tokio::fs::File;
use tokio::io;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use crate::{get_metadata_offsets, Metadata, METADATA_HEADER_SIZE};

pub(crate) async fn read_metadata(file: &mut File) -> io::Result<Metadata> {
    file.seek(SeekFrom::End(-(METADATA_HEADER_SIZE as i64))).await?;

    let mut buffer = [0; METADATA_HEADER_SIZE];
    file.read_exact(&mut buffer).await?;

    println!("Buff: {:?}", &buffer);

    let (start, len) = get_metadata_offsets(&buffer).map_err(|_| {
        io::Error::new(ErrorKind::InvalidData, "Unable to read index metadata.")
    })?;

    println!("got: {} - {}", start, len);

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
