use std::io;
use std::io::ErrorKind;

use bytes::{Bytes, BytesMut};
use flume::{RecvError, TrySendError};
use tracing::warn;

#[derive(Debug)]
/// A body is a stream of incoming bytes.
pub struct Body {
    incoming: flume::Receiver<io::Result<Option<Bytes>>>,
}

impl Body {
    /// Creates a new [Body] which can stream chunks via a channel.
    pub fn channel() -> (BodySender, Self) {
        let (tx, rx) = flume::bounded(2);
        (BodySender { tx }, Self { incoming: rx })
    }

    /// Creates a body that is empty.
    pub fn empty() -> Self {
        let (tx, slf) = Self::channel();
        tx.try_finish().expect("Body channel should have capacity");
        slf
    }

    /// Creates a body that is complete as a single [Bytes] object.
    pub fn complete(body: Bytes) -> Self {
        let (tx, slf) = Self::channel();
        tx.try_send(body)
            .expect("Body channel should have capacity");
        tx.try_finish().expect("Body channel should have capacity");
        slf
    }

    /// Consumes the body and collects all chunks.
    ///
    /// This method is not very memory efficient and shouldn't be
    /// used in performance or memory sensitive applications.
    pub async fn collect(self) -> io::Result<Bytes> {
        let mut buffer = BytesMut::new();
        while let Some(chunk) = self.next().await? {
            if buffer.is_empty() {
                buffer = BytesMut::from(chunk);
            } else {
                buffer.extend_from_slice(&chunk);
            }
        }
        Ok(buffer.freeze())
    }

    /// Attempts to read the next chunk of the available body.
    ///
    /// Returns `Ok(None)` if the body reading is complete.
    pub async fn next(&self) -> io::Result<Option<Bytes>> {
        self.incoming.recv_async().await.map_err(|e| {
            if matches!(e, RecvError::Disconnected) {
                warn!(
                    "IO body channel was disconnected before reading could be completed"
                );
            }
            io::Error::new(ErrorKind::Interrupted, e.to_string())
        })?
    }
}

/// The sender half of a [Body] stream.
pub struct BodySender {
    tx: flume::Sender<io::Result<Option<Bytes>>>,
}

impl BodySender {
    /// Sends a new chunk of the body.
    ///
    /// Returns `false` if the reader of the body has been dropped.
    pub async fn send(&self, chunk: Bytes) -> bool {
        self.tx.send_async(Ok(Some(chunk))).await.is_ok()
    }

    /// Attempts to send a new chunk of the body.
    ///
    /// Returns and error if the chunk cannot be sent without
    /// blocking or the channel has been disconnected.
    pub fn try_send(&self, chunk: Bytes) -> Result<(), TrySendError<Bytes>> {
        self.tx.try_send(Ok(Some(chunk))).map_err(|e| match e {
            TrySendError::Full(b) => TrySendError::Full(b.unwrap().unwrap()),
            TrySendError::Disconnected(b) => TrySendError::Full(b.unwrap().unwrap()),
        })
    }

    /// Signals the end of the body.
    ///
    /// Returns `false` if the reader of the body has been dropped.
    pub async fn finish(&self) -> bool {
        self.tx.send_async(Ok(None)).await.is_ok()
    }

    /// Attempts to send message to signal the end of the body.
    ///
    /// Returns and error if the chunk cannot be sent without
    /// blocking or the channel has been disconnected.
    pub fn try_finish(&self) -> Result<(), TrySendError<()>> {
        self.tx.try_send(Ok(None)).map_err(|e| match e {
            TrySendError::Full(_) => TrySendError::Full(()),
            TrySendError::Disconnected(_) => TrySendError::Full(()),
        })
    }

    /// Returns an IO error to the body reader.
    ///
    /// Returns `false` if the reader of the body has been dropped.
    pub async fn error(&self, error: io::Error) -> bool {
        self.tx.send_async(Err(error)).await.is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_body_empty() {
        let body = Body::empty();
        let chunk = body.next().await.expect("Body read should be OK");
        assert!(chunk.is_none());
    }

    #[tokio::test]
    async fn test_body_stream() {
        let (tx, body) = Body::channel();

        tokio::spawn(async move {
            let res = tx.try_send(Bytes::from_static(b"Hello, World!"));
            assert!(res.is_ok());

            for _ in 0..3 {
                tx.send(Bytes::from_static(b"Hello, World!")).await;
            }
            tx.finish().await;
        });

        let mut count = 0;
        while let Some(chunk) = body.next().await.unwrap() {
            assert_eq!(chunk, Bytes::from_static(b"Hello, World!"));
            count += 1;
        }
        assert_eq!(count, 4);
    }

    #[tokio::test]
    async fn test_body_many_chunks_collect() {
        let (tx, body) = Body::channel();

        tokio::spawn(async move {
            for _ in 0..3 {
                tx.send(Bytes::from_static(b"hello")).await;
            }
            tx.finish().await;
        });

        let body = body.collect().await.expect("Read body");
        assert_eq!(body, Bytes::from_static(b"hellohellohello"))
    }

    #[tokio::test]
    async fn test_body_dropped_mid_stream() {
        let (tx, body) = Body::channel();

        tokio::spawn(async move {
            for _ in 0..3 {
                tx.send(Bytes::from_static(b"hello")).await;
            }
        });

        let err = body.collect().await.expect_err("Read body should error");
        assert_eq!(err.kind(), ErrorKind::Interrupted);
    }

    #[test]
    fn test_try_methods() {
        let (tx, _body) = Body::channel();
        assert!(tx.try_send(Bytes::from_static(b"Hello, world!")).is_ok());
        assert!(tx.try_finish().is_ok());

        let (tx, _body) = Body::channel();
        assert!(tx.try_send(Bytes::from_static(b"Hello, world!")).is_ok());
        assert!(tx.try_send(Bytes::from_static(b"Hello, world!")).is_ok());
        assert!(tx.try_send(Bytes::from_static(b"Hello, world!")).is_err());

        let (tx, _body) = Body::channel();
        assert!(tx.try_send(Bytes::from_static(b"Hello, world!")).is_ok());
        assert!(tx.try_send(Bytes::from_static(b"Hello, world!")).is_ok());
        assert!(tx.try_finish().is_err());

        let (tx, body) = Body::channel();
        assert!(tx.try_send(Bytes::from_static(b"Hello, world!")).is_ok());
        drop(body);
        assert!(tx.try_send(Bytes::from_static(b"Hello, world!")).is_err());

        let (tx, body) = Body::channel();
        assert!(tx.try_send(Bytes::from_static(b"Hello, world!")).is_ok());
        drop(body);
        assert!(tx.try_finish().is_err());
    }

    #[tokio::test]
    async fn test_body_complete() {
        let msg = Bytes::from_static(b"Hello, World!");
        let body = Body::complete(msg.clone());
        let chunk = body.next().await.expect("Body read should be OK");
        assert_eq!(chunk, Some(msg));
        let chunk = body.next().await.expect("Body read should be OK");
        assert!(chunk.is_none());
    }

    #[tokio::test]
    async fn test_body_collect() {
        let msg = Bytes::from_static(b"Hello, World!");
        let body = Body::complete(msg.clone());
        let chunk = body.collect().await.expect("Body read should be OK");
        assert_eq!(chunk, msg);
    }

    #[tokio::test]
    async fn test_body_error() {
        let (sender, body) = Body::channel();
        sender
            .error(io::Error::new(ErrorKind::UnexpectedEof, "testing"))
            .await;
        let err = body.collect().await.expect_err("Body read should be OK");
        assert_eq!(err.kind(), ErrorKind::UnexpectedEof);

        let (sender, body) = Body::channel();
        sender.send(Bytes::from_static(b"partial data")).await;
        sender
            .error(io::Error::new(ErrorKind::UnexpectedEof, "testing"))
            .await;
        let err = body.collect().await.expect_err("Body read should be OK");
        assert_eq!(err.kind(), ErrorKind::UnexpectedEof);
    }
    #[tokio::test]
    async fn test_body_disconnected() {
        let (sender, body) = Body::channel();
        drop(sender);
        let err = body.collect().await.expect_err("Body read should be OK");
        assert_eq!(err.kind(), ErrorKind::Interrupted);
    }
}
