use std::io;
use std::io::Write;
use std::time::{Duration, Instant};

use crossbeam::channel::{self, TrySendError};
use futures::channel::oneshot;
use lnx_executor::yielding::{Step, YieldingIo};

use crate::check_result;
use crate::yielding::AckOp;

#[derive(Debug, thiserror::Error)]
pub enum WriterError<B> {
    /// The writer has completed it's execution and is closed.
    Closed(B),
    /// The writer is currently full.
    Full(B),
    /// The writer closed mid op.
    Aborted,
}

pub struct YieldingWriterHandle<B = Vec<u8>>
where
    B: AsRef<[u8]> + Send + 'static,
{
    ops: channel::Sender<AckOp<WriteOp<B>, io::Result<()>>>,
}

impl<B> YieldingWriterHandle<B>
where
    B: AsRef<[u8]> + Send + 'static,
{
    /// Attempt to submit a buffer to the writer providing it is
    /// connected and not full.
    pub async fn submit_buffer(
        &self,
        buffer: B,
    ) -> Result<io::Result<()>, WriterError<B>> {
        let op = WriteOp::Write(buffer);
        let (tx, rx) = oneshot::channel();
        match self.ops.try_send(AckOp::new(op, tx)) {
            Ok(()) => rx.await.map_err(|_| WriterError::Aborted),
            Err(TrySendError::Disconnected(buffer)) => {
                Err(WriterError::Closed(buffer.op.unwrap_write()))
            },
            Err(TrySendError::Full(buffer)) => {
                Err(WriterError::Full(buffer.op.unwrap_write()))
            },
        }
    }

    /// Flushes the pending buffers.
    pub async fn flush(&self) -> Option<io::Result<()>> {
        let (tx, rx) = oneshot::channel();
        match self.ops.try_send(AckOp::new(WriteOp::Flush, tx)) {
            Ok(()) => rx.await.ok(),
            Err(TrySendError::Disconnected(_)) => None,
            Err(TrySendError::Full(_)) => None,
        }
    }
}

/// A specialised writer wrapper that only performs
/// enough operations to block *up to* a given duration.
pub struct YieldingWriter<W, B = Vec<u8>>
where
    W: Write + Send + 'static,
    B: AsRef<[u8]> + Send + 'static,
{
    writer: W,
    backlog: Option<AckOp<BufOrCopy<B>, io::Result<()>>>,
    ops: channel::Receiver<AckOp<WriteOp<B>, io::Result<()>>>,
}

impl<W, B> YieldingWriter<W, B>
where
    W: Write + Send + 'static,
    B: AsRef<[u8]> + Send + 'static,
{
    fn write_all(&mut self, buffer: &[u8]) -> io::Result<()> {
        self.writer.write_all(buffer)
    }

    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.writer.write(buffer)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl<W, B> YieldingIo for YieldingWriter<W, B>
where
    W: Write + Send + 'static,
    B: AsRef<[u8]> + Send + 'static,
{
    fn step(&mut self, limit: Duration) -> Step {
        let start = Instant::now();
        loop {
            // If we have a previously pending operation that needs
            // to be handled first. Unlike when we initially write data
            // backlogged operations will call write_all to ensure
            // the backlog is cleared.
            if let Some(msg) = self.backlog.take() {
                let res = self.write_all(msg.op.as_ref());
                check_result!(msg, res);

                if start.elapsed() >= limit {
                    return Step::WouldBlock(start.elapsed());
                }
            }

            let msg = match self.ops.try_recv() {
                Ok(op) => op,
                Err(_) => return Step::Completed(start.elapsed()),
            };

            match &msg.op {
                WriteOp::Write(buf) => {
                    let original_buffer = buf.as_ref();
                    let mut buffer = original_buffer;

                    loop {
                        let n = match self.write(buffer) {
                            Ok(n) => n,
                            Err(e) => {
                                msg.ack(Err(e));
                                return Step::Aborted;
                            },
                        };

                        if n == buffer.len() {
                            break;
                        }

                        buffer = &buffer[n..];

                        if start.elapsed() >= limit {
                            // No data was written
                            let msg = if original_buffer.len() == buffer.len() {
                                let op = BufOrCopy::Buf(msg.op.unwrap_write());
                                AckOp::new(op, msg.ack)
                            } else {
                                let op = BufOrCopy::Copy(buffer.to_owned());
                                AckOp::new(op, msg.ack)
                            };

                            self.backlog = Some(msg);
                            return Step::WouldBlock(start.elapsed());
                        }
                    }
                },
                WriteOp::Flush => check_result!(msg, self.flush()),
            }

            if start.elapsed() >= limit {
                return Step::WouldBlock(start.elapsed());
            }
        }
    }
}

pub enum BufOrCopy<T> {
    Buf(T),
    Copy(Vec<u8>),
}

impl<T: AsRef<[u8]>> AsRef<[u8]> for BufOrCopy<T> {
    fn as_ref(&self) -> &[u8] {
        match self {
            BufOrCopy::Buf(b) => b.as_ref(),
            BufOrCopy::Copy(b) => b.as_ref(),
        }
    }
}

pub enum WriteOp<T> {
    Write(T),
    Flush,
}

impl<T> WriteOp<T> {
    pub fn unwrap_write(self) -> T {
        match self {
            WriteOp::Write(buffer) => buffer,
            WriteOp::Flush => panic!("Write op was Flush not write."),
        }
    }
}
