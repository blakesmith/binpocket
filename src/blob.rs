use async_lock::{Mutex, RwLock};
use async_std::{
    fs::{File, OpenOptions},
    io::{prelude::SeekExt, Error as AIOError, SeekFrom, Write, WriteExt},
    task::{Context, Poll},
};
use async_trait::async_trait;
use bytes::Buf;
use core::pin::Pin;

use futures_util::{Stream, StreamExt};
use serde::Deserialize;
use sha2::{Digest, Sha256, Sha512};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use uuid::Uuid;
use warp::{http::StatusCode, Filter, Rejection, Reply};

use crate::error::{ErrorCode, ErrorResponse};
use crate::{
    digest,
    digest::{deserialize_digest_string, deserialize_optional_digest_string, DigestAlgorithm},
};

/// Errors return from Blob Store operations.
#[derive(Debug)]
#[allow(dead_code)]
pub enum BlobStoreError {
    AIO(async_std::io::Error),
    SessionNotFound(Uuid),
    UnsupportedDigest,
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
#[async_trait]
pub trait BlobStore {
    type Writer: Write + Unpin + Send;

    /// Begin uploading a blob, returns the session_id as a Uuid. After calling,
    /// clients should now be able to call 'get_session'.
    async fn start_upload(&self) -> Result<Uuid, BlobStoreError>;

    /// Retrieve the underlying session, and its associated writer, at the given
    /// byte offset position.
    async fn get_session(
        &self,
        session_id: &Uuid,
        pos: u64,
    ) -> Result<Arc<Mutex<UploadSession<Self::Writer>>>, BlobStoreError>;

    /// Finalize the upload session, producing an immutable Digest that will
    /// be used as the blob identifier going forward.
    async fn finalize_upload(
        &self,
        session_id: &Uuid,
        algorithm: DigestAlgorithm,
    ) -> Result<digest::Digest, BlobStoreError>;

    /// Cancel the upload, removing all temporary upload state.
    async fn cancel_upload(&self, session_id: &Uuid) -> Result<(), BlobStoreError>;
}

/// Represents the state that needs to be tracked by the server
/// during upload. Since this holds open writers, it must be actively
/// cleaned up from the outstanding upload sessions that the server
/// is tracking once an upload is completed or aborted.
pub struct UploadSession<W: Write + Unpin + Send> {
    id: Uuid,
    writer: W,
    bytes_written: u64,
    sha256: Sha256,
    sha512: Sha512,
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
                self.sha256.update(buf);
                self.sha512.update(buf);
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

impl<W: Write + Unpin + Send> UploadSession<W> {
    fn new(writer: W, id: Uuid) -> Self {
        Self {
            id,
            writer,
            bytes_written: 0,
            sha256: Sha256::new(),
            sha512: Sha512::new(),
        }
    }

    pub fn finalize(&mut self, algorithm: DigestAlgorithm) -> Option<digest::Digest> {
        let sha256 = std::mem::take(&mut self.sha256);
        let sha512 = std::mem::take(&mut self.sha512);
        let finalized = match algorithm {
            DigestAlgorithm::Sha256 => format!("{:x}", sha256.finalize()),
            DigestAlgorithm::Sha512 => format!("{:x}", sha512.finalize()),
            other => {
                tracing::error!("Unsupported digest algorithm: {}", other);
                return None;
            }
        };

        Some(digest::Digest::new(algorithm, finalized))
    }
}

pub struct FsBlobStore {
    root_directory: PathBuf,

    /// Sessions are potentially accessed across multiple different
    /// endpoint calls, since we want to support chunked uploading. We
    /// must wrap each session in its own lock for safety.
    ///
    /// TODO: We must periodically clean this up to prevent resource
    /// leaks!
    sessions: RwLock<HashMap<Uuid, Arc<Mutex<UploadSession<File>>>>>,
}

impl FsBlobStore {
    /// Open the filesystem blob store. Note: This is a blocking I/O call,
    /// since it's only run during startup.
    pub fn open(root_directory: PathBuf) -> Result<Self, std::io::Error> {
        std::fs::create_dir_all(root_directory.join("sessions"))?;
        std::fs::create_dir_all(root_directory.join("blobs"))?;
        Ok(Self {
            root_directory,
            sessions: RwLock::new(HashMap::new()),
        })
    }
}

#[async_trait]
impl BlobStore for FsBlobStore {
    type Writer = File;

    async fn start_upload(&self) -> Result<Uuid, BlobStoreError> {
        let session_id = Uuid::new_v4();
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(
                self.root_directory
                    .join("sessions")
                    .join(format!("{}", session_id.to_hyphenated_ref())),
            )
            .await?;
        let session = UploadSession::new(file, session_id.clone());
        self.sessions
            .write()
            .await
            .insert(session_id.clone(), Arc::new(Mutex::new(session)));
        Ok(session_id.clone())
    }

    async fn get_session(
        &self,
        session_id: &Uuid,
        pos: u64,
    ) -> Result<Arc<Mutex<UploadSession<Self::Writer>>>, BlobStoreError> {
        let session = self
            .sessions
            .read()
            .await
            .get(session_id)
            .ok_or(BlobStoreError::SessionNotFound(session_id.clone()))?
            .clone();

        session
            .lock()
            .await
            .writer
            .seek(SeekFrom::Start(pos))
            .await?;
        Ok(session)
    }

    async fn finalize_upload(
        &self,
        session_id: &Uuid,
        algorithm: DigestAlgorithm,
    ) -> Result<digest::Digest, BlobStoreError> {
        let session = self
            .sessions
            .write()
            .await
            .remove(session_id)
            .ok_or(BlobStoreError::SessionNotFound(session_id.clone()))?;

        // TODO: Move the session to blob storage!

        let mut session_guard = session.lock().await;
        session_guard
            .finalize(algorithm)
            .ok_or(BlobStoreError::UnsupportedDigest)
    }

    async fn cancel_upload(&self, session_id: &Uuid) -> Result<(), BlobStoreError> {
        let _ = self
            .sessions
            .write()
            .await
            .remove(session_id)
            .ok_or(BlobStoreError::SessionNotFound(session_id.clone()))?;
        Ok(())
    }
}

pub fn routes<B: BlobStore + Send + Sync + 'static>(
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    blob_upload_start::<B>().or(blob_upload_put::<B>())
}

#[derive(Debug, Deserialize)]
struct BlobPutQueryParams {
    #[serde(deserialize_with = "deserialize_optional_digest_string")]
    digest: Option<digest::Digest>,
}

async fn receive_put_upload<B, S, BUF>(
    repository: String,
    session_id: Uuid,
    query_params: BlobPutQueryParams,
    blob_store: Arc<B>,
    mut byte_stream: S,
) -> Result<impl Reply, Rejection>
where
    B: BlobStore + Send + Sync + 'static,
    BUF: Buf,
    S: Stream<Item = Result<BUF, warp::Error>> + Unpin,
{
    tracing::debug!(
        "Starting upload, repository: {}, session_id: {}",
        repository,
        session_id
    );
    let session = blob_store.get_session(&session_id, 0).await?;

    // Hold the upload session lock for the duration of the upload
    let mut upload_session = session.lock().await;
    while let Some(buf) = byte_stream.next().await {
        match buf {
            Ok(b) => {
                tracing::debug!("Got byte buffer, remaining: {}", b.remaining());
                if let Err(e) = upload_session.write_all(b.chunk()).await {
                    tracing::error!("Failed to write upload session data: {:?}", e);
                    return Ok(StatusCode::INTERNAL_SERVER_ERROR);
                }
            }
            Err(e) => {
                tracing::error!("Failed to get http buffer: {:?}", e);
                return Ok(StatusCode::INTERNAL_SERVER_ERROR);
            }
        }
    }

    // Upload is completed. Time to validate:
    //
    // If the client passed a 'digest' query parameter, validate that
    // the computed digest matches the one designated by the client,
    // and either finalize the upload (if they match), or abort if
    // they do not.

    match query_params.digest {
        Some(client_digest) => {
            // Make sure we drop the lock, so we
            // can finalize the session.

            drop(upload_session);
            let computed_digest = blob_store
                .finalize_upload(&session_id, client_digest.algorithm.clone())
                .await
                .map_err(|e| {
                    tracing::info!("Invalid digest: {:?}", e);
                    ErrorResponse::new(
                        StatusCode::BAD_REQUEST,
                        ErrorCode::DigestInvalid,
                        "Invalid digest algorithm. Must be one of: sha256, sha512".to_string(),
                    )
                })?;

            if computed_digest != client_digest {
                Err(ErrorResponse::new(
                    StatusCode::BAD_REQUEST,
                    ErrorCode::DigestInvalid,
                    format!(
                        "Computed digest {} does not match expected digest: {}, aborting",
                        computed_digest, client_digest
                    ),
                ))?
            } else {
                Ok(StatusCode::CREATED)
            }
        }
        None => Ok(StatusCode::CREATED),
    }
}

fn blob_upload_put<B: BlobStore + Send + Sync + 'static>(
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::put()
        .and(warp::path!("v2" / String / "blobs" / "uploads" / Uuid))
        .and(warp::query::<BlobPutQueryParams>())
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
