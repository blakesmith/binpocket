use core::pin::Pin;
use futures::{
    stream::Stream,
    task::{Context, Poll},
};
use heed::types::ByteSlice;
use prost::Message;

use std::marker::PhantomData;

/// Utility to scan an lmdb database in chunks, and iteratively stream
/// the items to callers. Values in the database are assumed to be prost encoded
/// protobuf messages, which are decoded before being yielded to the stream. Chunks
/// are broken up into small transactions to avoid holding open a large transaction
/// to the database. Because we don't have an exclusive lock over the database, this
/// means that new items might be added even after we've iterated past that key, meaning
/// the item will not be discovered until the next time a scan is performed from the
/// beginning of the range.
pub struct LmdbProtoScanner<M: Message> {
    /// The last key seen by the stream. Used to fetch the 'next page' of scan
    /// results.
    last_key: Option<ByteSlice>,

    /// How many items are we going to read per transaction.
    chunk_size: usize,

    _phantom: PhantomData<M>,
}

pub enum ScanError {}

impl<M: Message> Stream for LmdbProtoScanner<M> {
    type Item = Result<M, ScanError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}
