use std::{cmp, io};
use std::io::ErrorKind;
use futures_lite::AsyncReadExt;
use glommio::io::{DmaStreamReader, ImmutableFile};


use crate::{get_metadata_offsets, Metadata, METADATA_HEADER_SIZE};

pub(crate) async fn read_metadata(
    file: &ImmutableFile,
) -> io::Result<Metadata> {
    let file_size = file.file_size();
    let buf = file.read_at(file_size - METADATA_HEADER_SIZE as u64, METADATA_HEADER_SIZE)
        .await?;

    let (mut start, len) = get_metadata_offsets(&buf).map_err(|_| {
        io::Error::new(ErrorKind::InvalidData, "Unable to read index metadata.")
    })?;

    let mut len = len as usize;
    let mut buffer = vec![];
    while buffer.len() < len as usize {
        let buf = file.read_at(start, len).await?;

        if buf.is_empty() {
            break;
        }

        buffer.extend_from_slice(&buf);
        start += buf.len() as u64;
        len -= cmp::min(buf.len(), len);
    }

    let metadata = Metadata::from_bytes(&buffer[..len as usize])?;

    Ok(metadata)
}

/// Ensures that the current reader is at the start of the range or beyond it.
pub(super) async fn ensure_reader_at_range_start(
    reader: &mut DmaStreamReader,
    buffer: &mut [u8],
    buffer_offset: &mut usize,
    expected_start: u64,
) -> io::Result<()> {
    if reader.current_pos() > expected_start {
        let position_diff = (reader.current_pos() - expected_start) as usize;

        debug_assert!(
            position_diff <= *buffer_offset,
            "The difference between expected start and reader position suggests that \
            the buffer offset should at least be the same length of the delta but it is not. \
            This suggests that future data reads will be corrupted."
        );

        if position_diff >= *buffer_offset {
            return Ok(())
        }

        // We only want to keep the last n bytes from the offset.
        let retain_from = *buffer_offset - position_diff;
        buffer.copy_within(retain_from..*buffer_offset, 0);
        (*buffer_offset) = position_diff;

        return Ok(())
    }

    if reader.current_pos() == expected_start {
        // Reset the offset as any previously saved bytes are not needed.
        (*buffer_offset) = 0;
        return Ok(())
    }

    let mut remaining_len = (expected_start - reader.current_pos()) as usize;
    while reader.current_pos() < expected_start {
        let n = reader.read(&mut buffer[*buffer_offset..]).await?;

        if n == 0 {
            break;
        }

        retain_excess(n, remaining_len, buffer, buffer_offset);

        remaining_len -= cmp::min(n, remaining_len);
    }

    Ok(())
}


pub(super) async fn read_n_bytes(
    reader: &mut DmaStreamReader,
    buffer: &mut [u8],
    buffer_offset: &mut usize,
    expected_len: usize,
) -> io::Result<Vec<u8>> {
    let mut data = vec![];
    let mut remaining_len = expected_len;

    while data.len() < expected_len {
        let n = reader.read(&mut buffer[*buffer_offset..]).await?;

        if n == 0 {
            break;
        }

        let slice_at = cmp::min(n, remaining_len);
        data.extend_from_slice(&buffer[..slice_at]);

        retain_excess(n, remaining_len, buffer, buffer_offset);

        remaining_len -= slice_at;
    }

    Ok(data)
}


fn retain_excess(
    n_read: usize,
    target: usize,
    buffer: &mut [u8],
    buffer_offset: &mut usize,
) {
    // Adjust the offset so that we don't loose data.
    if n_read > target {
        (*buffer_offset) = n_read - target;

        let retain_byte_from = buffer.len() - *buffer_offset;

        // Move the remaining bytes to the front of the buffer.
        buffer.copy_within(retain_byte_from.., 0);
    } else {
        (*buffer_offset) = 0;
    }
}