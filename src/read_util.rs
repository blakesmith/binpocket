use std::pin::Pin;

use async_std::io::{self, Read};
use async_std::stream::Stream;
use async_std::task::{Context, Poll};
use bytes::{Bytes, BytesMut};

pub fn bytes_stream<T>(reader: T) -> BytesBuf<T>
where
    T: Read + Unpin,
{
    BytesBuf { inner: reader }
}

#[derive(Debug)]
pub struct BytesBuf<T> {
    pub(crate) inner: T,
}

/// Adapted from async_std::ReadExt::bytes, to produce bytes::Bytes instead
/// of individual bytes.
impl<T: Read + Unpin> Stream for BytesBuf<T> {
    type Item = io::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut buf = BytesMut::with_capacity(1024);
        let rd = Pin::new(&mut self.inner);
        match futures_util::ready!(rd.poll_read(cx, &mut buf)) {
            Ok(0) => Poll::Ready(None),
            Ok(..) => Poll::Ready(Some(Ok(buf.freeze()))),
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => Poll::Pending,
            Err(e) => Poll::Ready(Some(Err(e))),
        }
    }
}
