use std::io;
use std::sync::Arc;

use i2o2::opcode::FSyncMode;

use super::DynamicGuard;

#[derive(Clone)]
/// A [RingFile] is a DIRECT IO file tied to a specific [i2o2::I2o2Scheduler] ring.
///
/// The file can be cleaply cloned and handles interactions between the store
/// and the IO scheduler.
pub struct RingFile {
    id: u64,
    ring_id: u32,
    inner: Arc<std::fs::File>,
    handle: i2o2::I2o2Handle<DynamicGuard>,
}

impl RingFile {
    #[inline]
    /// Returns the unique identified of the file.
    ///
    /// This is the resulting name of the file.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Submits a buffer to be written to the file.
    ///
    /// This does not wait for completion of the op, instead a [i2o2::ReplyReceiver]
    /// is returned which can be awaited at a later stage.
    pub async unsafe fn submit_write(
        &self,
        ptr: *const u8,
        len: usize,
        offset: u64,
        maybe_guard: Option<DynamicGuard>,
    ) -> io::Result<i2o2::ReplyReceiver> {
        let op =
            i2o2::opcode::Write::new(i2o2::types::Fixed(self.ring_id), ptr, len, offset);

        // SAFETY: our op is safe to send across the thread boundaries and the buffer
        //         is guaranteed to live at least as long as the ring requires as it
        //         is passed to our ring guard.
        let reply = unsafe {
            self.handle
                .submit_async(op, maybe_guard)
                .await
                .map_err(io::Error::other)?
        };

        Ok(reply)
    }

    /// Performs the equivalent of a `fdatasync(1)`.
    pub async fn fdatasync(&self) -> io::Result<()> {
        let op =
            i2o2::opcode::Fsync::new(i2o2::types::Fixed(self.ring_id), FSyncMode::Data);

        let reply = unsafe {
            self.handle
                .submit_async(op, None)
                .await
                .map_err(io::Error::other)?
        };

        let result = reply
            .await
            .map_err(|_| io::Error::other("io scheduler panicked"))?;

        if result < 0 {
            Err(io::Error::from_raw_os_error(-result))
        } else {
            Ok(())
        }
    }
}
