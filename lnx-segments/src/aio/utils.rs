use std::{cmp, io};
use std::io::ErrorKind;
use futures_lite::AsyncReadExt;
use glommio::io::{DmaStreamReader, ImmutableFile};

use crate::{get_metadata_offsets, Metadata, METADATA_HEADER_SIZE};
use crate::aio::AioWriter;

pub(crate) async fn read_metadata(
    file: &ImmutableFile,
) -> io::Result<Metadata> {
    let file_size = file.file_size();

    if file_size < METADATA_HEADER_SIZE as u64 {
        return Err(io::Error::new(ErrorKind::InvalidData, "File size does not hold the minimum required data."));
    }

    let buf = file.read_at(file_size - METADATA_HEADER_SIZE as u64, METADATA_HEADER_SIZE)
        .await?;

    let (start, len) = get_metadata_offsets(&buf).map_err(|_| {
        io::Error::new(ErrorKind::InvalidData, "Unable to read index metadata.")
    })?;

    if file_size <= start || len > file_size {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!(
                "Apparent metadata positions do not align with the file itself. \
                file_size={}, metadata_start={}, len={}",
                file_size,
                start,
                len,
            )
        ));
    }

    let buffer = file.read_at(start, len as usize).await?;
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

pub(super) async fn copy_data(
    writer: &mut AioWriter,
    reader: &mut DmaStreamReader,
    buffer: &mut [u8],
    buffer_offset: &mut usize,
    expected_len: usize,
) -> io::Result<()> {
    let mut read_bytes = *buffer_offset;
    let mut remaining_len = expected_len;

    while read_bytes < expected_len {
        let n = reader.read(&mut buffer[*buffer_offset..]).await?;

        if n == 0 {
            break;
        }

        let slice_at = cmp::min(n, remaining_len);
        writer.write_all(&buffer[..slice_at]).await?;

        retain_excess(n, remaining_len, buffer, buffer_offset);

        read_bytes += n;
        remaining_len -= slice_at;
    }

    Ok(())
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

#[cfg(test)]
mod tests {
    use datacake_crdt::{get_unix_timestamp_ms, HLCTimestamp};
    use glommio::ByteSliceMutExt;
    use glommio::io::{DmaFile, DmaStreamReaderBuilder, ImmutableFileBuilder};
    use crate::run_aio;
    use super::*;

    static BUFFER_SAMPLE: &[u8] = b"Hello, world!";

    async fn get_temp_file() -> io::Result<DmaFile> {
        let fp = crate::get_random_tmp_file();

        let file = glommio::io::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .dma_open(fp)
            .await?;

        let mut buffer = file.alloc_dma_buffer(512);
        buffer.write_at(0, BUFFER_SAMPLE);

        file.write_at(buffer, 0).await?;
        file.fdatasync().await?;

        Ok(file)
    }

    #[test]
    fn test_metadata_reader() -> io::Result<()> {
        let fp = crate::get_random_tmp_file();
        let ts = HLCTimestamp::new(get_unix_timestamp_ms(), 0, 0);
        let metadata = Metadata::new("test-index".to_string(), ts);
        let mut raw_data = metadata.to_bytes()?;
        let len = raw_data.len();
        raw_data.extend_from_slice(&[0; 8]);
        raw_data.extend_from_slice(&len.to_be_bytes());

        std::fs::write(&fp, raw_data)?;

        let fut = move || async move {
            let file = ImmutableFileBuilder::new(fp)
                .build_existing()
                .await?;

            let metadata = read_metadata(&file)
                .await
                .expect("read metadata correctly.");

            assert!(metadata.files().is_empty(), "Expected metadata file to have no files linked.");
            assert_eq!(metadata.segment_id(), ts, "Expected metadata segment ids to match.");

            Ok(())
        };

        run_aio!(fut, "read-metadata")
    }

    #[test]
    fn test_ensure_reader_at_range_start() -> io::Result<()> {
        let fut = move || async move {
            const TARGET_LENGTH: u64 = 2;

            let file = get_temp_file().await?;
            let mut reader = DmaStreamReaderBuilder::new(file).build();

            let mut buffer = crate::new_buffer();
            let mut buffer_offset = 0;
            ensure_reader_at_range_start(
                &mut reader,
                &mut buffer[..],
                    &mut buffer_offset,
                TARGET_LENGTH,
            ).await?;

            assert!(reader.current_pos() >= TARGET_LENGTH, "Expected reader position to be at least upto the targeted start.");

            // This is potentially flakey, but there's not a whole lot we can do about it.
            if reader.current_pos() > TARGET_LENGTH {
                let diff = (reader.current_pos() - TARGET_LENGTH) as usize;
                assert_eq!(buffer_offset, diff, "Expected excess read data to be reflected in the offset.");
            }

            Ok::<_, io::Error>(())
        };

        run_aio!(fut, "ensure-reader-start")
    }

    #[test]
    fn test_read_n_bytes() -> io::Result<()> {
        let fut = move || async move {
            let file = get_temp_file().await?;
            let mut reader = DmaStreamReaderBuilder::new(file).build();

            let mut buffer = crate::new_buffer();
            let mut buffer_offset = 0;

            let buff = read_n_bytes(
                &mut reader,
                &mut buffer[..],
                &mut buffer_offset,
                BUFFER_SAMPLE.len(),
            ).await?;

            assert_eq!(&buff, BUFFER_SAMPLE, "Expected read buffer and sample to match.");

            Ok::<_, io::Error>(())
        };

        run_aio!(fut, "test-read-bytes")
    }

    #[test]
    fn test_copy_data() -> io::Result<()> {
        let fut = move || async move {
            let fp = crate::get_random_tmp_file();
            let segment_id = HLCTimestamp::new(get_unix_timestamp_ms(), 0, 0);

            let mut writer = AioWriter::create(
                &fp,
                0,
                "test-index".to_string(),
                segment_id
            ).await?;

            let file = get_temp_file().await?;
            let mut reader = DmaStreamReaderBuilder::new(file).build();

            let mut buffer = crate::new_buffer();
            let mut buffer_offset = 0;

            copy_data(
                &mut writer,
                &mut reader,
                &mut buffer[..],
                &mut buffer_offset,
                BUFFER_SAMPLE.len(),
            ).await?;

            assert_eq!(
                writer.current_pos(),
                BUFFER_SAMPLE.len() as u64,
                "Expected writer to have written {} bytes.",
                BUFFER_SAMPLE.len(),
            );

            Ok::<_, io::Error>(())
        };

        run_aio!(fut, "test-copy-data")
    }
}