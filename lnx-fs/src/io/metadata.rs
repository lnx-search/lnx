use std::{cmp, io};
use std::io::{BufReader, ErrorKind, Read};
use std::path::Path;

use tracing::{instrument, warn};

use crate::io::actors::footer::FileEntryFooter;
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
) -> io::Result<Vec<FileEntryFooter>> {
    let file_path = super::utils::get_tablet_metadata_file_path(base_path, tablet_id);
    
    let file = std::fs::File::open(file_path)?;
    let reader = BufReader::new(file);
    
    load_tablet_metadata_from_reader(reader)
}

fn load_tablet_metadata_from_reader<R>(
    mut reader: R,
) -> io::Result<Vec<FileEntryFooter>>
where 
    R: Read,
{
    let mut entries = Vec::new();
    let mut temp_buffer = Vec::new();

    loop {
        temp_buffer.clear();
        
        let mut prefix_bytes = [0; 8];

        match reader.read_exact(&mut prefix_bytes) {
            Ok(()) => {},
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
            Err(other) => return Err(other),
        }

        let length = u32::from_le_bytes(prefix_bytes[0..4].try_into().unwrap()) as usize;
        let expected_checksum = u32::from_le_bytes(prefix_bytes[4..8].try_into().unwrap());

        read_n_bytes(&mut reader, &mut temp_buffer, length)?;
        let computed_checksum = crc32fast::hash(&temp_buffer);

        // A miss-match in checksums is recoverable, just not massively cheap and requires
        // a full re-scan of the metadata file since we can't completely trust the data
        // after this point.
        if computed_checksum != expected_checksum {
            warn!("Tablet metadata checksums do not match, system will re-scan tablet file to auto-repair");
            return Ok(Vec::new());
        }

        match FileEntryFooter::from_bytes(&temp_buffer) {
            None => {
                warn!("Tablet metadata entry is unable to be deserialized, system will re-scan tablet file to auto-repair");
                continue
            },
            Some(footer) => {
                entries.push(footer);
            }
        }
    }

    Ok(entries)
}

fn read_n_bytes<R>(
    reader: &mut R,
    buffer: &mut Vec<u8>,
    length: usize,
) -> io::Result<()>
where 
    R: Read,
{
    let mut buf = [0; 1024];
    while buffer.len() < length {
        let remaining = cmp::min(length - buffer.len(), buf.len());
        let n = reader.read(&mut buf[..remaining])?;
        if n == 0 {
            return Err(io::Error::from(ErrorKind::UnexpectedEof))
        }
        
        buffer.extend_from_slice(&buf[..n]);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    
    use super::*;
    
    static FOOTER_SAMPLE: &[u8] = &[
        148, 181, 101, 120, 97, 109, 112, 108, 101, 47, 102, 105, 108, 101, 47, 104,
        101, 114, 101, 46, 116, 120, 116, 146, 0, 123, 206, 0, 171, 142, 115, 192,
        95, 95, 76, 78, 88, 95, 66, 76, 79, 66, 95, 69, 78, 84, 82, 89, 95, 95,
    ];
    
    #[test]
    fn test_read_n_bytes() {
        let mut sample = Cursor::new(b"Hello, world!".to_vec());
        
        let mut selected = Vec::new();
        read_n_bytes(&mut sample, &mut selected, 5).unwrap();
        assert_eq!(selected, b"Hello");
        
        let mut selected = Vec::new();
        read_n_bytes(&mut sample, &mut selected, 2).unwrap();
        assert_eq!(selected, b", ");

        let mut selected = Vec::new();
        read_n_bytes(&mut sample, &mut selected, 6).unwrap();
        assert_eq!(selected, b"world!");
    }

    #[test]
    fn test_read_n_bytes_eof() {
        let mut sample = Cursor::new(b"Hello, world!".to_vec());

        let mut selected = Vec::new();
        let err = read_n_bytes(&mut sample, &mut selected, 16)
            .expect_err("System should return IO error");
        assert_eq!(err.kind(), ErrorKind::UnexpectedEof);
    }
    
    #[test]
    fn test_recover_footer_with_reader() {
        let mut file_data = Vec::new();
        file_data.extend_from_slice(&(FOOTER_SAMPLE.len() as u32).to_le_bytes());
        file_data.extend_from_slice(&crc32fast::hash(FOOTER_SAMPLE).to_le_bytes());
        file_data.extend_from_slice(FOOTER_SAMPLE);
        
        let sample_metadata = Cursor::new(file_data);
        
        let entries = load_tablet_metadata_from_reader(sample_metadata)
            .expect("System should read all entries");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].file_path, "example/file/here.txt");
    }

    #[test]
    fn test_recover_footer_early_eof() {
        let sample_metadata = Cursor::new(Vec::new());

        let entries = load_tablet_metadata_from_reader(sample_metadata)
            .expect("System should read all entries");
        assert!(entries.is_empty());
    }
    
    #[test]
    fn test_recover_many_footer_with_reader() {
        let mut file_data = Vec::new();
        for _ in 0..3 {
            file_data.extend_from_slice(&(FOOTER_SAMPLE.len() as u32).to_le_bytes());
            file_data.extend_from_slice(&crc32fast::hash(FOOTER_SAMPLE).to_le_bytes());
            file_data.extend_from_slice(FOOTER_SAMPLE);            
        }

        let sample_metadata = Cursor::new(file_data);

        let entries = load_tablet_metadata_from_reader(sample_metadata)
            .expect("System should read all entries");
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].file_path, "example/file/here.txt");
    }
    
    #[test]
    fn test_checksum_missmatch_return_no_entries() {
        let mut file_data = Vec::new();
        file_data.extend_from_slice(&(FOOTER_SAMPLE.len() as u32).to_le_bytes());
        file_data.extend_from_slice(&1234u32.to_le_bytes());
        file_data.extend_from_slice(FOOTER_SAMPLE);

        let sample_metadata = Cursor::new(file_data);

        let entries = load_tablet_metadata_from_reader(sample_metadata)
            .expect("System should read data OK");
        assert!(entries.is_empty());
    }


    #[test]
    fn test_footer_corruption_return_one_entries() {
        let invalid_footer_bytes: &[u8] = &[
            148, 181, 101, 120, 97, 109, 112, 108, 101, 47, 102, 105, 108, 101, 47, 104,
            101, 114, 101, 46, 116, 120, 0, 171, 142, 115, 192,
            95, 95, 76, 78, 88, 95, 66, 76, 78, 84, 82, 89,
        ];

        let mut file_data = Vec::new();
        file_data.extend_from_slice(&(invalid_footer_bytes.len() as u32).to_le_bytes());
        file_data.extend_from_slice(&crc32fast::hash(invalid_footer_bytes).to_le_bytes());
        file_data.extend_from_slice(invalid_footer_bytes);
        
        file_data.extend_from_slice(&(FOOTER_SAMPLE.len() as u32).to_le_bytes());
        file_data.extend_from_slice(&crc32fast::hash(FOOTER_SAMPLE).to_le_bytes());
        file_data.extend_from_slice(FOOTER_SAMPLE);

        let sample_metadata = Cursor::new(file_data);

        let entries = load_tablet_metadata_from_reader(sample_metadata)
            .expect("System should read data OK");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].file_path, "example/file/here.txt");
    }
    
    #[test]
    fn test_read_from_file() {
        let dir = tempfile::tempdir().unwrap();
        let tablet_id = TabletId::new();
    
        let file_path = crate::io::utils::get_tablet_metadata_file_path(dir.path(), tablet_id);;

        let mut file_data = Vec::new();
        file_data.extend_from_slice(&(FOOTER_SAMPLE.len() as u32).to_le_bytes());
        file_data.extend_from_slice(&crc32fast::hash(FOOTER_SAMPLE).to_le_bytes());
        file_data.extend_from_slice(FOOTER_SAMPLE);
        std::fs::write(file_path, file_data).unwrap();
        
        let entries = load_tablet_metadata(dir.path(), tablet_id)
            .expect("Read metadata OK");
        assert_eq!(entries.len(), 1);
    }
}