use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::Path;
use std::{cmp, io};

use tracing::{info, instrument, warn};

use crate::io::event::{FileEvent, FOOTER_MAGIC_BYTES, FOOTER_MAGIC_BYTES_LEN};
use crate::metastore::TabletId;

#[instrument]
/// Loads all the metadata entries.
///
/// Unlike the primary readers and writers, this system uses regular buffered
/// IO for simplicity, it is generally not required to be the most optimized thing
/// in the worlds and is not reading tons of data.
pub fn load_tablet_metadata(
    base_path: &Path,
    tablet_id: TabletId,
    start_from: u64,
) -> io::Result<Vec<FileEvent>> {
    let file_path = super::get_tablet_file_path(base_path, tablet_id);

    let mut file = std::fs::File::open(file_path)?;
    file.seek(SeekFrom::Start(start_from))?;
    let reader = BufReader::with_capacity(10 << 20, file);

    let entries = load_tablet_metadata_from_reader(reader)?;
    info!(
        num_entries = entries.len(),
        "Read metadata entries from metadata file"
    );
    Ok(entries)
}

fn load_tablet_metadata_from_reader<R>(
    mut reader: BufReader<R>,
) -> io::Result<Vec<FileEvent>>
where
    R: Read + Seek,
{
    let mut entries = Vec::new();
    let mut temp_buffer = Vec::new();
    let mut temp_buffer_offset = 0;
    let mut scratch_space = [0; 64];
    let mut scratch_used = 0;

    loop {
        temp_buffer.clear();
        temp_buffer.extend_from_slice(&scratch_space[..scratch_used]);

        let n = fill_buffer(&mut reader, &mut temp_buffer, 2 << 10)?;
        if n == 0 {
            break;
        }

        // Incrementally work our way through the buffer scanning for the magic
        // bytes' indicator. Because we buffer in memory it is possible we have
        // more than one entry in the buffer so we need to check the full memory
        // hence the loop here with an increasing temp_buffer_offset.
        loop {
            let working_slice = &temp_buffer[temp_buffer_offset..];

            let byte_offset = working_slice
                .windows(FOOTER_MAGIC_BYTES_LEN)
                .position(|slice| slice == FOOTER_MAGIC_BYTES);

            let offset = match byte_offset {
                None => {
                    let end = temp_buffer.len();
                    scratch_used = cmp::min(scratch_space.len(), temp_buffer.len());
                    scratch_space[..scratch_used]
                        .copy_from_slice(&temp_buffer[end - scratch_used..]);
                    break;
                },
                Some(offset) => offset,
            };

            let buffer_with_magic_bytes = &working_slice[offset..];
            temp_buffer_offset += offset;
            // We always know the magic bytes are present by this stage, so we can
            // add their length to the offset.
            temp_buffer_offset += FOOTER_MAGIC_BYTES_LEN;

            match FileEvent::from_bytes(buffer_with_magic_bytes) {
                None => {
                    warn!("Tablet metadata entry is unable to be deserialized");
                    continue;
                },
                Some(footer) => {
                    // We can now add the length of the footer u32 indicating the
                    // msgpack bytes length.
                    temp_buffer_offset += FileEvent::FOOTER_LENGTH_BYTES;
                    entries.push(footer);
                },
            }
        }
    }

    Ok(entries)
}

pub(super) fn fill_buffer<R>(
    reader: &mut R,
    buffer: &mut Vec<u8>,
    length: usize,
) -> io::Result<usize>
where
    R: Read,
{
    let mut bytes_read = 0;
    let mut buf = [0; 1024];
    while buffer.len() < length {
        let remaining = cmp::min(length - buffer.len(), buf.len());
        let n = reader.read(&mut buf[..remaining])?;
        bytes_read += n;
        if n == 0 {
            break;
        }

        buffer.extend_from_slice(&buf[..n]);
    }
    Ok(bytes_read)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    static FOOTER_SAMPLE: &[u8] = &[
        95, 95, 76, 78, 88, 95, 66, 76, 79, 66, 95, 69, 78, 84, 82, 89, 95, 95, 36, 0,
        0, 0, 147, 206, 103, 95, 32, 28, 192, 129, 166, 68, 101, 108, 101, 116, 101,
        145, 179, 101, 120, 97, 109, 112, 108, 101, 47, 102, 111, 111, 47, 98, 97, 114,
        46, 116, 120, 116,
    ];

    #[test]
    fn test_fill_buffer() {
        let mut sample = Cursor::new(b"Hello, world!".to_vec());

        let mut selected = Vec::new();
        fill_buffer(&mut sample, &mut selected, 5).unwrap();
        assert_eq!(selected, b"Hello");

        let mut selected = Vec::new();
        fill_buffer(&mut sample, &mut selected, 2).unwrap();
        assert_eq!(selected, b", ");

        let mut selected = Vec::new();
        fill_buffer(&mut sample, &mut selected, 6).unwrap();
        assert_eq!(selected, b"world!");
    }

    #[test]
    fn test_fill_buffer_eof() {
        let mut sample = Cursor::new(b"Hello, world!".to_vec());

        let mut selected = Vec::new();
        let n = fill_buffer(&mut sample, &mut selected, 16)
            .expect("System should read the full buffer");
        assert_eq!(n, 13);
    }

    #[test]
    fn test_read_metadata_footer_with_reader() {
        let mut file_data = Vec::new();
        file_data.extend_from_slice(&(FOOTER_SAMPLE.len() as u32).to_le_bytes());
        file_data.extend_from_slice(&crc32fast::hash(FOOTER_SAMPLE).to_le_bytes());
        file_data.extend_from_slice(FOOTER_SAMPLE);

        let sample_metadata = Cursor::new(file_data);

        let entries = load_tablet_metadata_from_reader(BufReader::new(sample_metadata))
            .expect("System should read all entries");
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn test_read_metadata_footer_early_eof() {
        let sample_metadata = Cursor::new(Vec::new());

        let entries = load_tablet_metadata_from_reader(BufReader::new(sample_metadata))
            .expect("System should read all entries");
        assert!(entries.is_empty());
    }

    #[test]
    fn test_read_metadata_many_footer_with_reader() {
        let mut file_data = Vec::new();
        for _ in 0..3 {
            file_data.extend_from_slice(&(FOOTER_SAMPLE.len() as u32).to_le_bytes());
            file_data.extend_from_slice(&crc32fast::hash(FOOTER_SAMPLE).to_le_bytes());
            file_data.extend_from_slice(FOOTER_SAMPLE);
        }

        let sample_metadata = Cursor::new(file_data);

        let entries = load_tablet_metadata_from_reader(BufReader::new(sample_metadata))
            .expect("System should read all entries");
        assert_eq!(entries.len(), 3);
    }

    #[test]
    fn test_footer_corruption_return_one_entries() {
        let invalid_footer_bytes: &[u8] = &[
            148, 181, 101, 120, 97, 109, 112, 108, 101, 47, 102, 105, 108, 101, 47, 104,
            101, 114, 101, 46, 116, 120, 0, 171, 142, 115, 192, 95, 95, 76, 78, 88, 95,
            66, 76, 78, 84, 82, 89,
        ];

        let mut file_data = Vec::new();
        file_data.extend_from_slice(&(invalid_footer_bytes.len() as u32).to_le_bytes());
        file_data.extend_from_slice(invalid_footer_bytes);
        file_data.extend_from_slice(FOOTER_SAMPLE);

        let sample_metadata = Cursor::new(file_data);

        let entries = load_tablet_metadata_from_reader(BufReader::new(sample_metadata))
            .expect("System should read data OK");
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn test_read_from_file() {
        let dir = tempfile::tempdir().unwrap();
        let tablet_id = TabletId::new();

        let file_path = crate::io::get_tablet_file_path(dir.path(), tablet_id);

        let mut file_data = Vec::new();
        file_data.extend_from_slice(&(FOOTER_SAMPLE.len() as u32).to_le_bytes());
        file_data.extend_from_slice(&crc32fast::hash(FOOTER_SAMPLE).to_le_bytes());
        file_data.extend_from_slice(FOOTER_SAMPLE);
        std::fs::write(file_path, file_data).unwrap();

        let entries =
            load_tablet_metadata(dir.path(), tablet_id, 0).expect("Read metadata OK");
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn test_recover_metadata_many_footer_with_reader() {
        let sample_footer = FileEvent::create(
            Some(ulid::Ulid::new()),
            "example.txt".to_string(),
            0..123,
        );
        let footer_bytes = sample_footer.to_bytes();

        let mut file_data = Vec::new();
        for _ in 0..3 {
            file_data.extend_from_slice(&(FOOTER_SAMPLE.len() as u32).to_le_bytes());
            file_data.extend_from_slice(&crc32fast::hash(FOOTER_SAMPLE).to_le_bytes());
            file_data.extend_from_slice(&footer_bytes);
            file_data.extend_from_slice(&[0; 123]);
        }

        let sample_metadata = Cursor::new(file_data);

        let entries = load_tablet_metadata_from_reader(BufReader::new(sample_metadata))
            .expect("System should read all entries");
        assert_eq!(entries.len(), 3);
    }
}
