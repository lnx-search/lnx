use std::io;
use std::io::ErrorKind;
use std::os::fd::AsRawFd;
use std::sync::Arc;

use i2o2::opcode::FSyncMode;

use super::DynamicGuard;

pub type FileId = u64;

const MAX_NUM_REGISTERED_FILES: u32 = 10_000;

#[derive(Clone)]
/// The [IoScheduler] is a collection of [i2o2::I2o2Scheduler]s running together.
///
/// The scheduler allows any IOP to be issued to any one of the i2o2 schedulers.
pub(super) struct IoScheduler {
    handle: i2o2::I2o2Handle<DynamicGuard>,
    runtime_handle: Arc<std::thread::JoinHandle<io::Result<()>>>,
}

impl IoScheduler {
    #[cfg(test)]
    /// Creates a new [IoScheduler] for testing environments.
    pub(super) fn for_test() -> Self {
        Self::create().unwrap()
    }

    /// Creates a new [IoScheduler] with a given number of workers.
    pub(super) fn create() -> io::Result<Self> {
        let (thread_handle, handle) = i2o2::builder()
            .with_queue_size(256)
            // TOOD: Enable automatically: .with_coop_task_run(true)
            .with_num_registered_files(MAX_NUM_REGISTERED_FILES)
            .try_spawn()?;

        Ok(Self {
            handle,
            runtime_handle: Arc::new(thread_handle),
        })
    }

    /// Open a new [RingFile] located at the given path.
    pub(super) async fn make_ring_file(
        &self,
        id: FileId,
        file: std::fs::File,
    ) -> io::Result<RingFile> {
        let file = Arc::new(file);
        let ring_id = self
            .handle
            .register_file_async(file.as_raw_fd(), Some(file.clone()))
            .await
            .map_err(io::Error::other)?;
        Ok(RingFile {
            id,
            ring_id,
            handle: self.handle.clone(),
            inner: file,
            io_error_lockout: false,
            closed: false,
        })
    }
}

/// A [RingFile] is a DIRECT IO file.
pub struct RingFile {
    id: FileId,
    ring_id: u32,
    handle: i2o2::I2o2Handle<DynamicGuard>,
    inner: Arc<std::fs::File>,
    io_error_lockout: bool,
    closed: bool,
}

impl RingFile {
    #[inline]
    /// Returns the unique identified of the file.
    ///
    /// This is the resulting name of the file.
    pub fn id(&self) -> FileId {
        self.id
    }

    #[inline]
    /// Returns whether the file is currently closed.
    pub fn is_closed(&self) -> bool {
        self.closed
    }

    #[inline]
    /// Returns whether the file is locked out due to a prior IO error.
    pub fn is_locked_out(&self) -> bool {
        self.io_error_lockout
    }

    /// Get the file length reported.
    pub async fn get_len(&self) -> io::Result<u64> {
        let file = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let metadata = file.metadata()?;
            Ok(metadata.len())
        })
        .await
        .expect("spawn worker thread")
    }

    /// Closes the ring file and unregisters it from the ring.
    pub async fn close(&mut self) -> io::Result<()> {
        if self.is_closed() {
            return Ok(());
        }

        #[cfg(test)]
        fail::fail_point!("ringfile::close", |_| Err(io::Error::new(
            ErrorKind::BrokenPipe,
            "close err"
        )));

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
    ///
    /// # Safety
    ///
    /// The pointer must not be null and `len` must be within the valid bounds of the pointer.
    ///
    /// You must also ensure the pointer lives at least as long as the i2o2 scheduler
    /// requires, this can be done using the guard.
    pub async unsafe fn submit_write(
        &self,
        ptr: *const u8,
        len: usize,
        offset: u64,
        maybe_guard: Option<DynamicGuard>,
    ) -> io::Result<i2o2::ReplyReceiver> {
        self.ensure_safe_state()?;

        #[cfg(test)]
        fail::fail_point!("ringfile::submit_write", |_| Err(io::Error::other(
            "write err"
        )));

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

        tracing::trace!(
            file_id = self.id(),
            offset = offset,
            len = len,
            "submitted write IOP",
        );

        Ok(reply)
    }

    /// Submits a buffer to read an amount of bytes from the file.
    ///
    /// # Safety
    ///
    /// The pointer must not be null and `len` must be within the valid bounds of the pointer.
    ///
    /// You must ensure the pointer lives at least as long as the i2o2 scheduler
    /// requires, this can be done using the guard.
    pub async unsafe fn submit_read(
        &self,
        ptr: *mut u8,
        len: usize,
        offset: u64,
        maybe_guard: Option<DynamicGuard>,
    ) -> io::Result<i2o2::ReplyReceiver> {
        if self.closed {
            return Err(io::Error::new(ErrorKind::BrokenPipe, "file closed"));
        }

        #[cfg(test)]
        fail::fail_point!("ringfile::submit_read", |_| Err(io::Error::other(
            "read err"
        )));

        let op =
            i2o2::opcode::Read::new(i2o2::types::Fixed(self.ring_id), ptr, len, offset);

        // SAFETY: our op is safe to send across the thread boundaries and the buffer
        //         is guaranteed to live at least as long as the ring requires as it
        //         is passed to our ring guard.
        let reply = unsafe {
            self.handle
                .submit_async(op, maybe_guard)
                .await
                .map_err(io::Error::other)?
        };

        tracing::trace!(
            file_id = self.id(),
            offset = offset,
            len = len,
            "submitted read IOP",
        );

        Ok(reply)
    }

    /// Write an owned buffer to the file at a given offset.
    pub async fn write_buffer(&self, buffer: Vec<u8>, offset: u64) -> io::Result<()> {
        let data_ptr = buffer.as_ptr();
        let data_len = buffer.len();

        // SAFETY: We bundle the header bytes ownership with the pointers and give it to the scheduler.
        let reply = unsafe {
            self.submit_write(
                data_ptr,
                data_len,
                offset,
                Some(Arc::new(buffer) as DynamicGuard),
            )
            .await?
        };

        let result = get_reply_or_err(reply).await? as usize;
        if result != data_len {
            Err(io::Error::new(
                ErrorKind::StorageFull,
                "storage failed to allocate",
            ))
        } else {
            Ok(())
        }
    }

    /// Performs the equivalent of a `fdatasync(1)`.
    pub async fn fdatasync(&mut self) -> io::Result<()> {
        if let Err(error) = self.fdatasync_inner().await {
            self.io_error_lockout = true;
            tracing::error!(file_id = self.id(), error = ?error, "fsync failed");
            Err(error)
        } else {
            tracing::trace!(file_id = self.id(), "fsync completed");
            Ok(())
        }
    }

    async fn fdatasync_inner(&self) -> io::Result<()> {
        self.ensure_safe_state()?;

        #[cfg(test)]
        fail::fail_point!("ringfile::fdatasync", |_| Err(io::Error::other(
            "fsync err"
        )));

        let op =
            i2o2::opcode::Fsync::new(i2o2::types::Fixed(self.ring_id), FSyncMode::Data);

        let reply = unsafe {
            self.handle
                .submit_async(op, None)
                .await
                .map_err(io::Error::other)?
        };
        get_reply_or_err(reply).await?;
        Ok(())
    }

    /// Returns an IO error if the file is closed or locked out.
    pub fn ensure_safe_state(&self) -> io::Result<()> {
        if self.io_error_lockout {
            Err(io::Error::new(
                ErrorKind::ReadOnlyFilesystem,
                "file has become readonly due to a prior IO Error",
            ))
        } else if self.closed {
            Err(io::Error::new(ErrorKind::BrokenPipe, "file closed"))
        } else {
            Ok(())
        }
    }

    #[cfg(test)]
    pub(super) fn inner_file(&self) -> Arc<std::fs::File> {
        self.inner.clone()
    }
}

impl Drop for RingFile {
    fn drop(&mut self) {
        if !self.is_closed() {
            tracing::warn!(file_id = self.id(), "ring file was not explicitly closed");

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

pub(super) async fn get_reply_or_err(reply: i2o2::ReplyReceiver) -> io::Result<i32> {
    let result = reply.await.map_err(|e| {
        io::Error::new(
            ErrorKind::Interrupted,
            format!("IOP cancelled while writing header: {e}"),
        )
    })?;

    if result < 0 {
        Err(io::Error::from_raw_os_error(-result))
    } else {
        Ok(result)
    }
}
