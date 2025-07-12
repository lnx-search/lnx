use std::io;
use std::io::ErrorKind;
use std::sync::Arc;

use i2o2::opcode::FSyncMode;

use super::DynamicGuard;




/// A [RingFile] is a DIRECT IO file tied to a specific [i2o2::I2o2Scheduler] ring.
pub struct RingFile {
    id: u64,
    ring_id: u32,
    _inner: Arc<std::fs::File>,
    handle: i2o2::I2o2Handle<DynamicGuard>,
    closed: bool,
}

impl RingFile {
    #[inline]
    /// Returns the unique identified of the file.
    ///
    /// This is the resulting name of the file.
    pub fn id(&self) -> u64 {
        self.id
    }

    #[inline]
    /// Returns whether the file is currently closed.
    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// Closes the ring file and unregisters it from the ring.
    pub async fn close(&mut self) -> io::Result<()> {
        self.handle
            .unregister_file_async(self.ring_id)
            .await
            .map_err(io::Error::other)?;
        self.closed = true;
        Ok(())
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
        self.ensure_open()?;

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
    pub async fn fdatasync(&self) {
        if let Err(error) = self.fdatasync_inner().await {
            // We cannot rely on tracing logging displaying the message before the abort.
            // We also can _never_ retry this operation and assume everything will then be okay,
            // hence the hard abort here.
            // See Postgres' paper trail: https://wiki.postgresql.org/wiki/Fsync_Errors
            // The big one we care about is Linux, but they all share similar behaviour which
            // means it is absolutely incorrect to assume that issuing another call to fsync()
            // after the first error.
            eprintln!("FATAL (file_id:{}): {error}", self.id);
            eprintln!("FATAL (file_id:{}): issuing fdatasync iop failed", self.id);
            eprintln!(
                "FATAL (file_id:{}): under normal conditions a fdatasync call should never fail, \
                 this likely means the underlying storage has failed",
                self.id
            );
            eprintln!(
                "FATAL (file_id:{}): system will abort in order to ensure data integrity, \
                recovery will be required upon startup",
                self.id
            );

            std::process::abort();
        }
    }

    async fn fdatasync_inner(&self) -> io::Result<()> {
        self.ensure_open()?;

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

    fn ensure_open(&self) -> io::Result<()> {
        if self.closed {
            Err(io::Error::new(ErrorKind::BrokenPipe, "file closed"))
        } else {
            Ok(())
        }
    }
}

impl Drop for RingFile {
    fn drop(&mut self) {
        if !self.is_closed() {
            tracing::warn!(file_id = self.id, "ring file was not explicitly closed");

            let handle = self.handle.clone();
            let ring_id = self.ring_id;
            tokio::task::spawn(async move {
                let result = handle.unregister_file_async(ring_id).await;
                if let Err(error) = result {
                    tracing::error!(error = %error, "failed to unregister file");
                }
            });
            self.closed = true;
        }
    }
}
