use async_std::{
    fs::{File, OpenOptions},
    io::Error as AIOError,
    io::{Write, WriteExt},
    task::{Context, Poll},
};
use bytes::Buf;
use core::pin::Pin;

use futures_util::{future, Future, FutureExt, Stream, StreamExt};
use std::path::PathBuf;
use std::sync::Arc;
use uuid::Uuid;
use warp::{Filter, Rejection, Reply};

use crate::digest::{Digest, DigestAlgorithm};

/// Errors return from Blob Store operations.
#[derive(Debug)]
pub enum BlobStoreError {
    AIO(async_std::io::Error),
    Unknown,
}

impl From<AIOError> for BlobStoreError {
    fn from(e: AIOError) -> Self {
        BlobStoreError::AIO(e)
    }
}

impl warp::reject::Reject for BlobStoreError {}

/// Used to store blobs. Blobs are keyed by an upload session identifier, until
/// they are finalized. At finalization time, a digest is computed, and used as
/// the primary identifier for the blob going forward. A blob effectively becomes
/// immutable once the upload has been completed. Callers must either 'finalize'
/// a blob upload, or cancel it.
pub trait BlobStore {
    type Writer: Write + Unpin + Send;

    /// Begin uploading a blob, returns the session_id as a Uuid. After calling,
    /// clients should now be able to call 'get_session'.
    fn start_upload(&self) -> Pin<Box<dyn Future<Output = Result<Uuid, BlobStoreError>> + Send>>;

    /// Retrieve the underlying session, and its associated writer, at the given
    /// byte offset position.
    fn get_session(
        &self,
        session_id: &Uuid,
        pos: u64,
    ) -> Pin<Box<dyn Future<Output = Result<UploadSession<Self::Writer>, BlobStoreError>> + Send>>;

    /// Finalize the upload session, producing an immutable Digest that will
    /// be used as the blob identifier going forward.
    fn finalize_upload(
        &self,
        session_id: &Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<Digest, BlobStoreError>> + Send>>;

    /// Cancel the upload, removing all temporary upload state.
    fn cancel_upload(
        &self,
        session_id: &Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<(), BlobStoreError>> + Send>>;
}

pub struct UploadSession<W: Write + Unpin + Send> {
    id: Uuid,
    writer: W,
    bytes_written: u64,
}

impl<W: Write + Unpin + Send> Write for UploadSession<W> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, AIOError>> {
        match Pin::new(&mut self.writer).poll_write(cx, buf) {
            Poll::Ready(Ok(bytes_written)) => {
                self.bytes_written += bytes_written as u64;
                Poll::Ready(Ok(bytes_written))
            }
            otherwise => otherwise,
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), AIOError>> {
        Pin::new(&mut self.writer).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), AIOError>> {
        Pin::new(&mut self.writer).poll_close(cx)
    }
}

pub struct FsBlobStore {
    root_directory: PathBuf,
}

impl FsBlobStore {
    /// Open the filesystem blob store. Note: This is a blocking I/O call,
    /// since it's only run during startup.
    pub fn open(root_directory: PathBuf) -> Result<Self, std::io::Error> {
        std::fs::create_dir_all(root_directory.join("sessions"))?;
        std::fs::create_dir_all(root_directory.join("blobs"))?;
        Ok(Self { root_directory })
    }
}

impl BlobStore for FsBlobStore {
    type Writer = File;

    fn start_upload(&self) -> Pin<Box<dyn Future<Output = Result<Uuid, BlobStoreError>> + Send>> {
        let session_id = Uuid::new_v4();
        Box::pin(
            OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(
                    self.root_directory
                        .join("sessions")
                        .join(format!("{}", session_id.to_hyphenated_ref())),
                )
                .then(move |_| future::ok(session_id)),
        )
    }

    fn get_session(
        &self,
        session_id: &Uuid,
        pos: u64,
    ) -> Pin<Box<dyn Future<Output = Result<UploadSession<Self::Writer>, BlobStoreError>> + Send>>
    {
        let session_cloned = session_id.clone();
        Box::pin(
            OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(
                    self.root_directory
                        .join("sessions")
                        .join(format!("{}", session_id.to_hyphenated_ref())),
                )
                .then(move |file| match file {
                    Ok(f) => future::ok(UploadSession::new(f)),
                    Err(e) => future::err(BlobStoreError::from(e)),
                }),
        )
    }

    fn finalize_upload(
        &self,
        session_id: &Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<Digest, BlobStoreError>> + Send>> {
        Box::pin(future::ok(Digest::new(
            DigestAlgorithm::Sha256,
            "deadbeef".to_string(),
        )))
    }

    fn cancel_upload(
        &self,
        session_id: &Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<(), BlobStoreError>> + Send>> {
        Box::pin(future::ok(()))
    }
}

impl<W: Write + Unpin + Send> UploadSession<W> {
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
    mut byte_stream: S,
) -> Result<impl Reply, Rejection>
where
    B: BlobStore + Send + Sync + 'static,
    BUF: Buf,
    S: Stream<Item = Result<BUF, warp::Error>> + Unpin,
{
    tracing::info!("Starting upload");
    let mut upload_session = blob_store.get_session(&session_id, 0).await?;
    tracing::info!("Chunking");
    while let Some(buf) = byte_stream.next().await {
        match buf {
            Ok(b) => {
                tracing::debug!("Got byte buffer, remaining: {}", b.remaining());
                if let Err(e) = upload_session.write_all(b.chunk()).await {
                    tracing::error!("Failed to write upload session data: {:?}", e);
                    return Ok(warp::http::StatusCode::INTERNAL_SERVER_ERROR);
                }
            }
            Err(e) => {
                tracing::error!("Failed to get http buffer: {:?}", e);
                return Ok(warp::http::StatusCode::INTERNAL_SERVER_ERROR);
            }
        }
    }

    Ok(warp::http::StatusCode::CREATED)
}

fn blob_upload_put<B: BlobStore + Send + Sync + 'static>(
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::put()
        .and(warp::path!("v2" / String / "blobs" / "uploads" / Uuid))
        .and(warp::filters::ext::get::<Arc<B>>())
        .and(warp::filters::body::stream())
        .and_then(receive_put_upload)
}

async fn start_blob_upload<B>(
    repository: String,
    blob_store: Arc<B>,
) -> Result<impl Reply, Rejection>
where
    B: BlobStore + Send + Sync + 'static,
{
    let session_id = blob_store.start_upload().await?;
    let location = format!(
        "/v2/{}/blobs/upload/{}",
        repository,
        session_id.to_hyphenated_ref()
    );

    Ok(warp::http::response::Builder::new()
        .header("Location", location)
        .status(202)
        .body(format!("Start blob upload for: {}", repository)))
}

fn blob_upload_start<B: BlobStore + Send + Sync + 'static>(
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::post()
        .and(warp::path!("v2" / String / "blobs" / "uploads"))
        .and(warp::filters::ext::get::<Arc<B>>())
        .and_then(start_blob_upload)
}
