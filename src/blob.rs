use async_std::{fs::File, io::Error as AIOError, io::Write};
use bytes::Buf;
use futures_util::{future, Stream, StreamExt};
use std::path::PathBuf;
use std::sync::Arc;
use uuid::Uuid;
use warp::{Filter, Rejection, Reply};

use crate::digest::{Digest, DigestAlgorithm};

/// Errors return from Blob Store operations.
#[derive(Debug)]
enum BlobStoreError {
    AIO(async_std::io::Error),
}

/// Used to store blobs. Blobs are keyed by an upload session identifier, until
/// they are finalized. At finalization time, a digest is computed, and used as
/// the primary identifier for the blob going forward. A blob effectively becomes
/// immutable once the upload has been completed. Callers must either 'finalize'
/// a blob upload, or cancel it.
pub trait BlobStore {
    type Writer: Write;

    /// Begin uploading a blob, returns the session_id as a Uuid
    fn start_upload(&self) -> Result<Uuid, BlobStoreError>;

    /// Finalize the upload session, producing an immutable Digest that will
    /// be used as the blob identifier going forward.
    fn finalize_upload(&self, session_id: &Uuid) -> Result<Digest, BlobStoreError>;

    /// Cancel the upload, removing all temporary upload state.
    fn cancel_upload(&self, session_id: &Uuid) -> Result<(), BlobStoreError>;
}

struct UploadSession<W: Write> {
    id: Uuid,
    writer: W,
    bytes_written: u64,
}

pub struct FsBlobStore {
    root_directory: PathBuf,
}

impl FsBlobStore {
    /// Open the filesystem blob store. Note: This is a blocking I/O call,
    /// since it's only run during startup.
    pub fn open(root_directory: PathBuf) -> Result<Self, std::io::Error> {
        Ok(Self { root_directory })
    }
}

impl BlobStore for FsBlobStore {
    type Writer = File;

    fn start_upload(&self) -> Result<Uuid, BlobStoreError> {
        Ok(Uuid::new_v4())
    }

    fn finalize_upload(&self, session_id: &Uuid) -> Result<Digest, BlobStoreError> {
        Ok(Digest::new(DigestAlgorithm::Sha256, "deadbeef".to_string()))
    }

    fn cancel_upload(&self, session_id: &Uuid) -> Result<(), BlobStoreError> {
        Ok(())
    }
}

impl<W: Write> UploadSession<W> {
    fn new(writer: W) -> Self {
        Self {
            id: Uuid::new_v4(),
            writer,
            bytes_written: 0,
        }
    }
}

pub fn routes<B: BlobStore + Send + Sync + 'static>(
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    blob_upload_start::<B>().or(blob_upload_put::<B>())
}

async fn receive_put_upload<B, S, BUF>(
    repository: String,
    session_id: Uuid,
    blob_store: Arc<B>,
    byte_stream: S,
) -> Result<impl Reply, Rejection>
where
    B: BlobStore + Send + Sync + 'static,
    BUF: Buf,
    S: Stream<Item = Result<BUF, warp::Error>> + Unpin,
{
    let upload_success = byte_stream
        .take_while(|buf| future::ready(buf.is_ok()))
        .map(Result::unwrap)
        .then(|buf| {
            // TODO: Do the write here!
            tracing::debug!("Got byte buffer, remaining: {}", buf.remaining());
            future::ok(0)
        })
        .all(|w: Result<usize, AIOError>| future::ready(w.is_ok()))
        .await;

    if upload_success {
        Ok(warp::http::StatusCode::CREATED)
    } else {
        Ok(warp::http::StatusCode::INTERNAL_SERVER_ERROR)
    }
}

fn blob_upload_put<B: BlobStore + Send + Sync + 'static>(
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::put()
        .and(warp::path!("v2" / String / "blobs" / "uploads" / Uuid))
        .and(warp::filters::ext::get::<Arc<B>>())
        .and(warp::filters::body::stream())
        .and_then(receive_put_upload)
}

fn blob_upload_start<B: BlobStore + Send + Sync + 'static>(
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::post()
        .and(warp::path!("v2" / String / "blobs" / "uploads"))
        .and(warp::filters::ext::get::<Arc<B>>())
        .map(|repository, blob_store: Arc<B>| {
            // TODO: Fix error handling
            let session_id = blob_store.start_upload().expect("Could not start upload");
            let location = format!(
                "/v2/{}/blobs/upload/{}",
                repository,
                session_id.to_hyphenated_ref()
            );
            warp::http::response::Builder::new()
                .header("Location", location)
                .status(202)
                .body(format!("Start blob upload for: {}", repository))
        })
}
