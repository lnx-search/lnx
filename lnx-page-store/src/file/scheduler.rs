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

async fn get_reply_or_err(reply: i2o2::ReplyReceiver) -> io::Result<i32> {
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

#[cfg(all(test, not(feature = "test-miri")))]
mod tests {
    use std::io::Write;
    use std::ptr;

    use super::*;

    #[tokio::test]
    async fn test_ring_file_create() {
        let ring_file = make_tmp_ring_rile().await;
        assert_eq!(ring_file.id(), 0);
        assert_eq!(ring_file.ring_id, 0);

        drop(ring_file);
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }

    #[tokio::test]
    async fn test_ring_file_graceful_close() {
        let mut ring_file = make_tmp_ring_rile().await;
        assert!(!ring_file.is_closed());

        ring_file.close().await.expect("close failed");
        assert!(ring_file.is_closed());

        let result = ring_file.ensure_safe_state();
        assert!(matches!(result, Err(err) if err.kind() == ErrorKind::BrokenPipe));
    }

    #[tokio::test]
    async fn test_ring_file_write() {
        let mut ring_file = make_tmp_ring_rile().await;

        let data = vec![1u8; 1024];
        let reply = unsafe {
            ring_file
                .submit_write(data.as_ptr(), data.len(), 0, None)
                .await
                .expect("submit write failed")
        };

        let result = reply.await.expect("write reply failed");
        assert_eq!(result, 1024);

        ring_file.fdatasync().await.expect("fdatasync failed");
    }

    #[tokio::test]
    async fn test_ring_file_fsync_lockout() {
        let scenario = fail::FailScenario::setup();
        fail::cfg("ringfile::fdatasync", "return").unwrap();

        let mut ring_file = make_tmp_ring_rile().await;

        let error = ring_file
            .fdatasync()
            .await
            .expect_err("fdatasync should fail");
        assert_eq!(error.kind(), ErrorKind::Other);

        assert!(ring_file.is_locked_out());

        // I pinky promise this pointer isn't touched.
        let error = unsafe {
            ring_file
                .submit_write(ptr::null(), 0, 0, None)
                .await
                .expect_err("submit write should fail")
        };
        assert_eq!(error.kind(), ErrorKind::ReadOnlyFilesystem);

        scenario.teardown();
    }

    #[tokio::test]
    async fn test_ring_file_fsync_lockout_on_i2o2_error() {
        let mut ring_file = make_tmp_ring_rile().await;

        let scenario = fail::FailScenario::setup();
        fail::cfg("i2o2::fail::poll_reply_future", "return(-12)").unwrap();

        let error = ring_file
            .fdatasync()
            .await
            .expect_err("fdatasync should fail");
        assert_eq!(error.kind(), ErrorKind::OutOfMemory);
        assert!(ring_file.is_locked_out());

        scenario.teardown();
    }

    #[tokio::test]
    async fn test_ring_file_handle_i2o2_cancel() {
        let mut ring_file = make_tmp_ring_rile().await;

        let scenario = fail::FailScenario::setup();
        fail::cfg("i2o2::fail::poll_reply_future", "return(cancelled)").unwrap();

        let reply = unsafe {
            ring_file
                .submit_write(b"".as_ptr(), 0, 0, None)
                .await
                .expect("submit write should succeed")
        };
        let error = get_reply_or_err(reply)
            .await
            .expect_err("write should return cancelled err");
        assert_eq!(error.kind(), ErrorKind::Interrupted);

        let error = ring_file
            .write_buffer(Vec::new(), 0)
            .await
            .expect_err("write should fail");
        assert_eq!(error.kind(), ErrorKind::Interrupted);

        let mut read_buffer = Vec::with_capacity(5);
        let reply = unsafe {
            ring_file
                .submit_read(read_buffer.as_mut_ptr(), 5, 0, None)
                .await
                .expect("submit read should succeed")
        };
        let error = get_reply_or_err(reply)
            .await
            .expect_err("write should return cancelled err");
        assert_eq!(error.kind(), ErrorKind::Interrupted);

        let error = ring_file
            .fdatasync()
            .await
            .expect_err("fdatasync should fail");
        assert_eq!(error.kind(), ErrorKind::Interrupted);
        assert!(ring_file.is_locked_out());

        scenario.teardown();
    }

    #[tokio::test]
    async fn test_write_buffer_out_of_storage_error() {
        let ring_file = make_tmp_ring_rile().await;

        let scenario = fail::FailScenario::setup();
        fail::cfg("i2o2::fail::poll_reply_future", "return(12)").unwrap();

        let buffer = b"Hello, World!".to_vec();
        let error = ring_file
            .write_buffer(buffer, 0)
            .await
            .expect_err("write should fail");
        assert_eq!(error.kind(), ErrorKind::StorageFull);

        scenario.teardown();
    }

    #[rstest::rstest]
    #[case::empty_file(0)]
    #[case::filled_file(200)]
    #[tokio::test]
    async fn test_ring_file_get_len(#[case] expected_len: u64) {
        let scheduler = IoScheduler::for_test();
        let tmp_file = tempfile::tempfile().unwrap();
        tmp_file.set_len(expected_len).unwrap();

        let file = scheduler
            .make_ring_file(1, tmp_file)
            .await
            .expect("Failed to make ring file");

        let len = file.get_len().await.expect("get len failed");
        assert_eq!(len, expected_len);
    }

    #[rstest::rstest]
    #[case::no_offset_write_zero(0, 0)]
    #[case::no_offset_write_unaligned(200, 0)]
    #[case::no_offset_write_aligned(4096, 0)]
    #[case::offset_write_unaligned(200, 5)]
    #[case::offset_write_aligned(4096, 5)]
    #[tokio::test]
    async fn test_ring_file_buffer_write(
        #[case] write_size: usize,
        #[case] offset: u64,
    ) {
        use std::os::unix::fs::FileExt;

        let ring_file = make_tmp_ring_rile().await;

        let data_buffer = vec![1; write_size];
        ring_file
            .write_buffer(data_buffer.clone(), offset)
            .await
            .expect("write buffer failed");

        let file = ring_file.inner.clone();
        let mut data_written = vec![0; write_size + offset as usize];
        let n_read = file.read_at(&mut data_written, 0).unwrap();
        assert_eq!(n_read, data_written.len());

        assert_eq!(&data_written[..offset as usize], vec![0; offset as usize]);
        assert_eq!(&data_written[offset as usize..], data_buffer);
    }

    #[rstest::rstest]
    #[case::no_offset_read_zero(0, 0)]
    #[case::no_offset_read_small(200, 0)]
    #[case::no_offset_read_large(128 << 10, 0)]
    #[case::offset_read_zero(0, 2)]
    #[case::offset_read_small(200, 2)]
    #[case::offset_read_large(128 << 10, 2)]
    #[tokio::test]
    async fn test_ring_file_read(#[case] read_len: usize, #[case] offset: u64) {
        let scheduler = IoScheduler::for_test();
        let mut tmp_file = tempfile::tempfile().unwrap();

        let mut data_buffer = vec![1; 256 << 10];
        data_buffer[0] = 4;
        data_buffer[1] = 3;
        data_buffer[2] = 2;
        tmp_file.write_all(&data_buffer).unwrap();
        tmp_file.sync_all().unwrap();

        let file = scheduler
            .make_ring_file(1, tmp_file)
            .await
            .expect("Failed to make ring file");

        let mut read_buffer = vec![0; read_len];

        // SAFETY: For testing purposes we don't really care about the cancellation.
        let reply = unsafe {
            file.submit_read(read_buffer.as_mut_ptr(), read_len, offset, None)
                .await
                .expect("submit read failed")
        };
        let reply = reply.await.expect("scheduler cancelled IOP");
        if reply < 0 {
            panic!("read errored: {}", io::Error::from_raw_os_error(-reply));
        }

        assert_eq!(reply as usize, read_len);

        assert_eq!(
            &read_buffer,
            &data_buffer[offset as usize..][..read_len],
            "read buffer does not match",
        );
    }

    async fn make_tmp_ring_rile() -> RingFile {
        let scheduler = IoScheduler::create().expect("create scheduler failed");
        let file = tempfile::tempfile().unwrap();
        scheduler
            .make_ring_file(0, file)
            .await
            .expect("make ring file failed")
    }
}
