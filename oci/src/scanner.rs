use core::future::Future;
use core::pin::Pin;
use futures::{
    stream::Stream,
    task::{Context, Poll},
};
use heed::types::ByteSlice;
use heed::Database;
use prost::Message;

use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

/// Utility to scan an lmdb database in chunks, and iteratively stream
/// the items to callers. Values in the database are assumed to be prost encoded
/// protobuf messages, which are decoded before being yielded to the stream. Chunks
/// are broken up into small transactions to avoid holding open a large transaction
/// to the database. Because we don't have an exclusive lock over the database, this
/// means that new items might be added even after we've iterated past that key, meaning
/// the item will not be discovered until the next time a scan is performed from the
/// beginning of the range.
pub struct LmdbProtoScanner<M: Message> {
    /// The underlying database.
    db: Database<ByteSlice, ByteSlice>,

    /// The last key seen by the stream. Used to fetch the 'next page' of scan
    /// results.
    last_key: Option<ByteSlice>,

    /// How many items are we going to read per transaction.
    chunk_size: usize,

    /// Current page of items
    current_page: Arc<Mutex<VecDeque<M>>>,

    /// Current in-progress DB fetch
    thunk: Option<Pin<Box<dyn Future<Output = Result<M, ScanError>> + Send>>>,
}

pub enum ScanError {}

impl<M: Message> Stream for LmdbProtoScanner<M> {
    type Item = Result<M, ScanError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut current_page = self.current_page.lock().unwrap();
        match current_page.pop_back() {
            Some(m) => Poll::Ready(Some(Ok(m))),
            None => {
                todo!()
            }
        }
    }
}
