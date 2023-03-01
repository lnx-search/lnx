mod writer;

use futures::channel::oneshot;
pub use writer::{WriteOp, YieldingWriter, WriterError, YieldingWriterHandle};

#[macro_export]
macro_rules! check_result {
    ($ack:expr, $res:expr) => {{
        let is_err = $res.is_err();
        $ack.ack($res);

        if is_err {
            return Step::Aborted;
        }
    }};
}

pub struct AckOp<Op, R> {
    pub op: Op,
    pub ack: oneshot::Sender<R>,
}

impl<Op, R> AckOp<Op, R> {
    pub fn new(op: Op, ack: oneshot::Sender<R>) -> Self {
        Self { op, ack }
    }

    pub fn ack(self, resp: R) {
        let _ = self.ack.send(resp);
    }
}
