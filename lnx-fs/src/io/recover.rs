use std::{cmp, io, mem};
use std::io::{ErrorKind, Read, Seek, SeekFrom};
use std::path::Path;
use std::fs::{File, OpenOptions};
use tracing::{info, instrument, warn};

use crate::io::footer::{FileEntryFooter, FOOTER_MAGIC_BYTES, FOOTER_MAGIC_BYTES_LEN};
use crate::metastore::TabletId;


const SCAN_BLOCK_SIZE: u64 = if cfg!(test) { 1 << 10 } else { 4 << 20 };

#[instrument]
/// Attempts to recover the metadata associated with a given tablet
/// by reading from the end of the tablet and working its way towards
/// the start.
/// 
/// This is an expensive operation IO-wise because it requires scanning
/// a lot of data, but does allow for full recovery of the persisted
/// entries in the tablet.
pub fn recover_metadata_from_tablet(
    base_path: &Path,
    tablet_id: TabletId,
) -> io::Result<Vec<FileEntryFooter>> {
    info!("System will recover tablet metadata, this may take some time...");
    
    let file_path = super::utils::get_tablet_metadata_file_path(base_path, tablet_id);
    
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)?;
        
    let pos = truncate_file_to_first_magic_bytes(&mut file)?;
    if pos == 0 {
        drop(file);
        info!("Tablet has no valid data within it, removing...");
        std::fs::remove_file(file_path)?;
        return Ok(Vec::new())
    }
    
    // From now on, the first bytes we read from the end will be magic bytes.
    scan_footers(&mut file, pos)
}

#[instrument(skip_all)]
fn truncate_file_to_first_magic_bytes(file: &mut File) -> io::Result<u64> {
    let len = file.metadata()?.len();
    match find_first_magic_bytes(file, len)? {
        None => {
            info!("No magic bytes were found within the tablet file, no data to recover");
            Ok(0)
        },
        Some(pos) => {
            info!("Found magic bytes at position {pos}, truncating file to this position");
            file.set_len(pos)?;
            Ok(pos)
        },
    }
}

/// Locates the starting position of the first footer magic bytes reading from the
/// back of the file working towards the front.
fn find_first_magic_bytes<R>(reader: &mut R, len: u64) -> io::Result<Option<u64>>
where 
    R: Read + Seek,
{    
    let mut temp_buffer = Vec::with_capacity(SCAN_BLOCK_SIZE as usize);
    let mut scratch_space = [0; FOOTER_MAGIC_BYTES_LEN];
    let mut last_buffer_scratch_used = 0;
    
    let mut reader = BackwardsFileReader::new(reader, len);
    loop {
        let last_pos = reader.current_pos;
        
        let n = reader.fill_buffer(&mut temp_buffer, SCAN_BLOCK_SIZE as usize)?;
        if n == 0 {
            break
        }

        // Re-add the head of the last buffer we saw to the tail of the new buffer.
        temp_buffer.extend_from_slice(&scratch_space[..last_buffer_scratch_used]);
        
        let magic_bytes_pos = temp_buffer
            .windows(FOOTER_MAGIC_BYTES_LEN)
            .rev()
            .position(|window| window == FOOTER_MAGIC_BYTES);

        if let Some(byte_start_position) = magic_bytes_pos {
            return Ok(Some(
                last_pos
                    + last_buffer_scratch_used as u64
                    - byte_start_position as u64
            ))
        }

        // We need to keep the head of the buffer around because we might
        // have a partial match already for the magic bytes but not yet
        // see the full thing.
        let first_n = cmp::min(temp_buffer.len(), FOOTER_MAGIC_BYTES_LEN);
        scratch_space[..first_n].copy_from_slice(&temp_buffer[..first_n]);
        last_buffer_scratch_used = first_n;
      
        temp_buffer.clear();
    }
    
    Ok(None)
}

#[instrument(skip_all)]
/// Reads from the back of the file towards the front and sequentially
/// reads each footer and object within the tablet.
fn scan_footers<R>(reader: &mut R, len: u64) -> io::Result<Vec<FileEntryFooter>>
where 
    R: Read + Seek,
{
    let mut temp_buffer = Vec::with_capacity(1 << 10);
    
    let mut entries = Vec::new();    
    let mut reader = BackwardsFileReader::new(reader, len);
    loop {
        let n = reader.fill_buffer(&mut temp_buffer, 1 << 10)?;
        if n == 0 {
            break
        }

        let Some(length) = FileEntryFooter::get_footer_length(&temp_buffer) else { break };

        while temp_buffer.len() < length {
            let n = reader.fill_buffer(&mut temp_buffer, 1 << 10)?;
            if n == 0 {
                break
            }
        }
                
        let footer = FileEntryFooter::from_bytes(&temp_buffer)
            .ok_or_else(|| io::Error::new(
                ErrorKind::Other,
                "Footer could not be deserialized which indicated either a bug of corruption"
            ))?;

        
        entries.push(footer);
        
        temp_buffer.clear();
    }

    info!(num_entries = entries.len(), "Retrieved entries");

    Ok(entries)
}


/// A reader that reads from the back of the file towards the front.
/// 
/// The actual contents read are _not reversed_, the best way to think
/// about this reader is it produces windows of bytes working
/// backwards, but does not change the ordering of the bytes returned.
struct BackwardsFileReader<R> {
    reader: R,
    length: u64,
    current_pos: u64,
}

impl<R> BackwardsFileReader<R> {
    fn new(reader: R, length: u64) -> Self {
        Self {
            reader,
            length,
            current_pos: length,
        }
    }
}

impl<R> BackwardsFileReader<R> 
where 
    R: Read + Seek
{
    fn fill_buffer(&mut self, buffer: &mut Vec<u8>, limit: usize) -> io::Result<usize> {
        let read_n = cmp::min(limit, self.remaining_len());
        if read_n == 0 {
            return Ok(0)
        }
        
        self.prepare_file_cursor_for_read(read_n)?;
        super::utils::read_n_bytes(&mut self.reader, buffer, read_n)?;
        
        self.advance_cursor(read_n as u64);
        
        Ok(read_n)
    }
    
    /// Skip the next N bytes.
    fn skip_n(&mut self, n: u64) {
        self.advance_cursor(n);
    }
    
    fn advance_cursor(&mut self, bytes_read: u64) {
        self.current_pos -= bytes_read;
    }
    
    fn remaining_len(&self) -> usize {
        let n_already_read = self.length - self.current_pos;
        (self.length - n_already_read) as usize
    }
    
    fn prepare_file_cursor_for_read(&mut self, read_n: usize) -> io::Result<()> {
        let new_pos = self.current_pos - read_n as u64;
        self.reader
            .seek(SeekFrom::Start(new_pos))?;
        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use super::*;
    
    #[test]
    fn test_backwards_reader() {
        let mut buffer: Vec<u8> = Vec::new();
        buffer.extend_from_slice(&[0; 123]);
        buffer.extend_from_slice(&[1; 123]);
        buffer.extend_from_slice(&[2; 123]);
        buffer.extend_from_slice(&[3; 123]);
        buffer.extend_from_slice(&[4; 123]);
        
        let len = buffer.len();
        let reader = Cursor::new(buffer);
        
        let mut reader = BackwardsFileReader::new(reader, len as u64);
        
        let mut sample = Vec::new();
        reader.fill_buffer(&mut sample, 123).unwrap();        
        assert_eq!(&sample, &[4; 123]);
        
        let mut sample = Vec::new();
        reader.fill_buffer(&mut sample, 123).unwrap();
        assert_eq!(&sample, &[3; 123]);
        
        let mut sample = Vec::new();
        reader.fill_buffer(&mut sample, 123).unwrap();
        assert_eq!(&sample, &[2; 123]);
        
        let mut sample = Vec::new();
        reader.fill_buffer(&mut sample, 123).unwrap();
        assert_eq!(&sample, &[1; 123]);
        
        let mut sample = Vec::new();
        reader.fill_buffer(&mut sample, 123).unwrap();
        assert_eq!(&sample, &[0; 123]);
        
        let mut sample = Vec::new();
        let n = reader.fill_buffer(&mut sample, 123).unwrap();
        assert_eq!(n, 0);
    }
    
    #[test]
    fn test_find_magic_bytes_as_first_byte() {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&[0; 123]);
        buffer.extend_from_slice(FOOTER_MAGIC_BYTES);
        
        let len = buffer.len();
        let mut reader = Cursor::new(buffer);
        let pos = find_first_magic_bytes(&mut reader, len as u64)
            .expect("System should read bytes OK");
        assert_eq!(pos, Some(len as u64), "Position of magic bytes should start at the end of the file");
    }

    #[test]
    fn test_find_magic_bytes_as_last_byte() {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(FOOTER_MAGIC_BYTES);
        buffer.extend_from_slice(&[0; 123]);

        let len = buffer.len();
        let mut reader = Cursor::new(buffer);
        let pos = find_first_magic_bytes(&mut reader, len as u64)
            .expect("System should read bytes OK");
        assert_eq!(
            pos,
            Some(FOOTER_MAGIC_BYTES.len() as u64), 
            "Position of magic bytes should start at the start of the file + the bytes length"
        );
    }
    
    #[test]
    fn test_find_magic_bytes_in_middle() {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&[0; 123]);
        buffer.extend_from_slice(FOOTER_MAGIC_BYTES);
        buffer.extend_from_slice(&[0; 123]);

        let len = buffer.len();
        let mut reader = Cursor::new(buffer);
        let pos = find_first_magic_bytes(&mut reader, len as u64)
            .expect("System should read bytes OK");
        assert_eq!(
            pos,
            Some(123 + FOOTER_MAGIC_BYTES.len() as u64),
            "Position of magic bytes should start at the start of the file + the bytes length"
        );
    }
    
    #[test]
    fn test_search_buffer_with_no_magic_bytes() {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&[0; 123]);
        buffer.extend_from_slice(&[0; 123]);

        let len = buffer.len();
        let mut reader = Cursor::new(buffer);
        let pos = find_first_magic_bytes(&mut reader, len as u64)
            .expect("System should read bytes OK");
        assert_eq!(
            pos,
            None,
            "No magic bytes should be found in buffer"
        );
    }

    #[test]
    fn test_find_first_out_of_many() {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&[0; 123]);
        buffer.extend_from_slice(FOOTER_MAGIC_BYTES);
        buffer.extend_from_slice(&[0; 123]);
        buffer.extend_from_slice(FOOTER_MAGIC_BYTES);
        buffer.extend_from_slice(&[0; 123]);

        let len = buffer.len();
        let mut reader = Cursor::new(buffer);
        let pos = find_first_magic_bytes(&mut reader, len as u64)
            .expect("System should read bytes OK");
        assert_eq!(
            pos,
            Some((123 + 123 + FOOTER_MAGIC_BYTES_LEN + FOOTER_MAGIC_BYTES_LEN) as u64),
        );
    }
    
    #[test]
    fn test_large_buffer_scan_for_magic_bytes() {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&[0; 4 << 10]);
        buffer.extend_from_slice(FOOTER_MAGIC_BYTES);
        buffer.extend_from_slice(&[0; 16 << 10]);

        let len = buffer.len();
        let mut reader = Cursor::new(buffer);
        let pos = find_first_magic_bytes(&mut reader, len as u64)
            .expect("System should read bytes OK");
        assert_eq!(
            pos,
            Some((len - (16 << 10)) as u64),
            "Magic bytes should be found in middle of buffer"
        );
    }
    
    #[test]
    fn test_magic_bytes_split_between_iterations() {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&[0; 4 << 10]);
        buffer.extend_from_slice(FOOTER_MAGIC_BYTES);
        // Doing this means the first 7 bytes of the magic bytes are in the first
        // iteration buffer, and the rest in the second.
        buffer.extend_from_slice(&[0; SCAN_BLOCK_SIZE as usize - 7]);

        let len = buffer.len();
        let mut reader = Cursor::new(buffer);
        let pos = find_first_magic_bytes(&mut reader, len as u64)
            .expect("System should read bytes OK");
        assert_eq!(
            pos,
            Some((len - (SCAN_BLOCK_SIZE as usize - 7)) as u64),
            "Magic bytes should be found in middle of buffer"
        );
    }

    #[test]
    fn test_recovery_truncates_file_to_magic_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let tablet_id = TabletId::new();

        let mut buffer = Vec::new();
        buffer.extend_from_slice(&[0; 4 << 10]);
        buffer.extend_from_slice(FOOTER_MAGIC_BYTES);
        buffer.extend_from_slice(&[0; 16 << 10]);
        let len = buffer.len();

        let file_path = crate::io::utils::get_tablet_metadata_file_path(dir.path(), tablet_id);
        std::fs::write(&file_path, buffer).unwrap();

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file_path)
            .unwrap();
        let pos = truncate_file_to_first_magic_bytes(&mut file)
            .expect("Detect and truncate file OK");
        assert_eq!(pos as usize, len - (16 << 10), "Position file truncated to does not match");
        
        file.sync_all().unwrap();
        let actual_len = file.metadata().unwrap().len();
        assert_eq!(pos, actual_len, "Truncation should be performed correctly");
    }

    #[test]
    fn test_recovery_returns_zero_for_no_magic_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let tablet_id = TabletId::new();

        let mut buffer = Vec::new();
        buffer.extend_from_slice(&[0; 4 << 10]);
        buffer.extend_from_slice(&[0; 16 << 10]);

        let file_path = crate::io::utils::get_tablet_metadata_file_path(dir.path(), tablet_id);
        std::fs::write(&file_path, buffer).unwrap();

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file_path)
            .unwrap();
        let pos = truncate_file_to_first_magic_bytes(&mut file)
            .expect("Detect file OK");
        assert_eq!(pos as usize, 0, "Position does not match");
    }
    
    #[test]
    fn test_scan_footers_scan_one() {
        let sample_footer = FileEntryFooter {
            file_path: "example.txt".to_string(),
            data_range: 0..123,
            created_at: 1234,
            transaction_id: Some(ulid::Ulid::new()),
        };
        
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&[0; 123]);
        buffer.extend_from_slice(&sample_footer.to_bytes());
        buffer.extend_from_slice(FOOTER_MAGIC_BYTES);
        
        let len = buffer.len();
        let mut reader = Cursor::new(buffer);
        
        let entries = scan_footers(&mut reader, len as u64)
            .expect("Scan footers from tablet");
        assert_eq!(&entries, &[sample_footer]);
    }
}