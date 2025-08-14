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
            _inner: file,
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
    _inner: Arc<std::fs::File>,
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
        self.ensure_safe_state()?;

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
    pub async fn fdatasync(&mut self) -> io::Result<()> {
        if let Err(error) = self.fdatasync_inner().await {
            self.io_error_lockout = true;
            Err(error)
        } else {
            Ok(())
        }
    }

    async fn fdatasync_inner(&self) -> io::Result<()> {
        self.ensure_safe_state()?;

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

    fn ensure_safe_state(&self) -> io::Result<()> {
        if self.closed {
            Err(io::Error::new(ErrorKind::BrokenPipe, "file closed"))
        } else if self.io_error_lockout {
            Err(io::Error::new(ErrorKind::ReadOnlyFilesystem, "file has become readonly due to a prior IO Error"))
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

#[cfg(all(test, not(feature = "test-miri")))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_scheduler_ring_file_create() {
        let scheduler = IoScheduler::create()
            .expect("create scheduler failed");

        let file = tempfile::tempfile().unwrap();
        let ring_file = scheduler
            .make_ring_file(0, file)
            .await
            .expect("make ring file failed");
        assert_eq!(ring_file.id(), 0);
        assert_eq!(ring_file.ring_id, 0);

        drop(ring_file);
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
    
    #[tokio::test]
    async fn test_scheduler_ring_file_graceful_close() {
        let scheduler = IoScheduler::create()
            .expect("create scheduler failed");

        let file = tempfile::tempfile().unwrap();
        let mut ring_file = scheduler
            .make_ring_file(0, file)
            .await
            .expect("make ring file failed");

        assert!(!ring_file.is_closed());

        ring_file.close().await.expect("close failed");
        assert!(ring_file.is_closed());

        let result = ring_file.ensure_safe_state();
        assert!(matches!(result, Err(err) if err.kind() == ErrorKind::BrokenPipe));
    }

    #[tokio::test]
    async fn test_scheduler_ring_file_write() {
        let scheduler = IoScheduler::create()
            .expect("create scheduler failed");

        let file = tempfile::tempfile().unwrap();
        let mut ring_file = scheduler
            .make_ring_file(0, file)
            .await
            .expect("make ring file failed");

        let data = vec![1u8; 1024];
        let reply = unsafe {
            ring_file
                .submit_write(data.as_ptr(), data.len(), 0, None).await
                .expect("submit write failed")
        };

        let result = reply.await.expect("write reply failed");
        assert_eq!(result, 1024);

        ring_file.fdatasync().await;
    }
}