use async_lock::{Mutex, RwLock};
use async_std::{
    fs,
    fs::{File, OpenOptions},
    io::{prelude::SeekExt, Error as AIOError, ErrorKind, SeekFrom, Write, WriteExt},
    task::{Context, Poll},
};
use async_trait::async_trait;
use bytes::Buf;
use core::pin::Pin;

use futures_util::{Stream, StreamExt};
use serde::Deserialize;
use sha2::{Digest, Sha256, Sha512};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::Arc;
use uuid::Uuid;
use warp::{
    http::{Response, StatusCode},
    Filter, Rejection, Reply,
};

use crate::error::{ErrorCode, ErrorResponse};
use crate::range::ContentRange;
use crate::{
    digest,
    digest::{deserialize_optional_digest_string, DigestAlgorithm},
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
/// a blob upload, or 'cancel' it to clean up temporary session upload state.
#[async_trait]
pub trait BlobStore {
    type Writer: Write + Unpin + Send + Debug;

    /// Begin uploading a blob, returns the session_id as a Uuid. After calling,
    /// clients should now be able to call 'get_session'.
    async fn start_upload(&self) -> Result<Uuid, BlobStoreError>;

    /// Retrieve the underlying session, and its associated writer, at the given
    /// byte offset position. Clients must call 'start_upload' to create the upload
    /// session before calling this function.
    async fn get_session(
        &self,
        session_id: &Uuid,
        pos: u64,
    ) -> Result<Arc<Mutex<UploadSession<Self::Writer>>>, BlobStoreError>;

    /// Finalize the upload session, producing an immutable Digest that will
    /// be used as the blob identifier going forward. Implementations can
    /// remove any temporary state that's being held for upload sessions, and
    /// transition the blob to stable, long-term, immutable storage.
    async fn finalize_upload(
        &self,
        session_id: &Uuid,
        digest: &digest::Digest,
    ) -> Result<(), BlobStoreError>;

    /// Cancel the upload, removing all temporary upload state.
    async fn cancel_upload(&self, session_id: &Uuid) -> Result<(), BlobStoreError>;

    /// Check if a blob exists. Clients will call this to verify
    /// if the blob is already present before initiating a new
    /// blob upload.
    async fn blob_exists(&self, digest: &digest::Digest) -> Result<u64, BlobStoreError>;
}

/// Represents the state that needs to be tracked by the server
/// during upload. Since this holds open writers, it must be actively
/// cleaned up from the outstanding upload sessions that the server
/// is tracking once an upload is completed or aborted.
#[derive(Debug)]
pub struct UploadSession<W: Write + Unpin + Send + Debug> {
    id: Uuid,
    writer: W,
    bytes_written: u64,
    sha256: Sha256,
    sha512: Sha512,
}

impl<W: Write + Unpin + Send + Debug> Write for UploadSession<W> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, AIOError>> {
        match Pin::new(&mut self.writer).poll_write(cx, buf) {
            Poll::Ready(Ok(bytes_written)) => {
                self.bytes_written += bytes_written as u64;
                self.sha256.update(&buf[0..bytes_written]);
                self.sha512.update(&buf[0..bytes_written]);
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

impl<W: Write + Unpin + Send + Debug> UploadSession<W> {
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
        digest: &digest::Digest,
    ) -> Result<(), BlobStoreError> {
        let session = self
            .sessions
            .write()
            .await
            .remove(session_id)
            .ok_or(BlobStoreError::SessionNotFound(session_id.clone()))?;

        // Drop the session, closing the underlying file handle.
        drop(session);

        // Transition the upload session file to stable, immutable
        // blob storage.
        fs::rename(
            self.root_directory
                .join("sessions")
                .join(format!("{}", session_id.to_hyphenated_ref())),
            self.root_directory
                .join("blobs")
                .join(format!("{}", digest)),
        )
        .await?;

        Ok(())
    }

    async fn cancel_upload(&self, session_id: &Uuid) -> Result<(), BlobStoreError> {
        let _ = self
            .sessions
            .write()
            .await
            .remove(session_id)
            .ok_or(BlobStoreError::SessionNotFound(session_id.clone()))?;

        // Remove temporary session storage
        fs::remove_file(
            self.root_directory
                .join("sessions")
                .join(format!("{}", session_id.to_hyphenated_ref())),
        )
        .await?;

        Ok(())
    }

    async fn blob_exists(&self, digest: &digest::Digest) -> Result<u64, BlobStoreError> {
        match fs::metadata(
            self.root_directory
                .join("blobs")
                .join(format!("{}", digest)),
        )
        .await
        {
            Ok(metadata) => Ok(metadata.len()),
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(0),
            Err(err) => Err(err.into()),
        }
    }
}

#[derive(Debug, Deserialize)]
struct BlobPutQueryParams {
    #[serde(default)]
    #[serde(deserialize_with = "deserialize_optional_digest_string")]
    digest: Option<digest::Digest>,
}

async fn receive_upload<B, S, BUF>(
    repository: String,
    session_id: Uuid,
    query_params: BlobPutQueryParams,
    content_range: Option<ContentRange>,
    blob_store: Arc<B>,
    mut byte_stream: S,
) -> Result<Response<&'static str>, Rejection>
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
    let session = blob_store
        .get_session(
            &session_id,
            content_range.as_ref().map(|c| c.start).unwrap_or(0),
        )
        .await?;

    // Hold the upload session lock for the duration of the upload
    let mut upload_session = session.lock().await;
    while let Some(buf) = byte_stream.next().await {
        match buf {
            Ok(b) => {
                tracing::debug!("Got byte buffer, remaining: {}", b.remaining());
                if let Err(e) = upload_session.write_all(b.chunk()).await {
                    tracing::error!("Failed to write upload session data: {:?}", e);
                    return Ok(warp::http::response::Builder::new()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body("")
                        .unwrap());
                }
            }
            Err(e) => {
                tracing::error!("Failed to get http buffer: {:?}", e);
                return Ok(warp::http::response::Builder::new()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body("")
                    .unwrap());
            }
        }
    }

    if let Err(e) = upload_session.flush().await {
        tracing::error!("Failed to flush upload session: {:?}", e);
    }

    tracing::debug!("Upload complete. Session: {:?}", upload_session);

    // Upload is completed. Time to validate:
    //
    // If the client passed a 'digest' query parameter, validate that
    // the computed digest matches the one designated by the client,
    // and either finalize the upload (if they match), or abort if
    // they do not.

    match query_params.digest {
        Some(client_digest) => {
            let computed_digest = upload_session
                .finalize(client_digest.algorithm.clone())
                .ok_or_else(|| {
                    tracing::info!("Invalid digest");
                    ErrorResponse::new(
                        StatusCode::BAD_REQUEST,
                        ErrorCode::DigestInvalid,
                        "Invalid digest algorithm. Must be one of: sha256, sha512".to_string(),
                    )
                })?;

            if computed_digest != client_digest {
                blob_store.cancel_upload(&session_id).await?;
                Err(ErrorResponse::new(
                    StatusCode::BAD_REQUEST,
                    ErrorCode::DigestInvalid,
                    format!(
                        "Computed digest {} does not match expected digest: {}, aborting",
                        computed_digest, client_digest
                    ),
                ))?
            } else {
                // Upload session is completely valid: Promote to
                // blob storage, and signal to the client that the
                // blob is created.
                blob_store
                    .finalize_upload(&session_id, &computed_digest)
                    .await?;
                let location = format!("/v2/{}/blobs/{}", &repository, &computed_digest);
                Ok(warp::http::response::Builder::new()
                    .header("Location", location)
                    .status(StatusCode::CREATED)
                    .body("")
                    .unwrap())
            }
        }

        // No digest was passed: This came from a PATCH chunk
        // upload: We signal that we've accepted the chunk, and
        // assume that the client will make a PUT call with the digest
        // to finalize the upload.
        None => {
            let range = format!("0-{}", upload_session.bytes_written - 1);
            Ok(warp::http::response::Builder::new()
                .header(
                    "Location",
                    format!("/v2/{}/blobs/uploads/{}", &repository, &session_id),
                )
                .header(
                    "Docker-Upload-UUID",
                    session_id.to_hyphenated_ref().to_string(),
                )
                .header("Range", range)
                .status(StatusCode::ACCEPTED)
                .body("")
                .unwrap())
        }
    }
}

async fn receive_put_upload<B, S, BUF>(
    repository: String,
    session_id: Uuid,
    query_params: BlobPutQueryParams,
    blob_store: Arc<B>,
    byte_stream: S,
) -> Result<Response<&'static str>, Rejection>
where
    B: BlobStore + Send + Sync + 'static,
    BUF: Buf,
    S: Stream<Item = Result<BUF, warp::Error>> + Unpin,
{
    receive_upload(
        repository,
        session_id,
        query_params,
        None,
        blob_store,
        byte_stream,
    )
    .await
}

async fn receive_patch_upload<B, S, BUF>(
    repository: String,
    session_id: Uuid,
    content_range: Option<ContentRange>,
    blob_store: Arc<B>,
    byte_stream: S,
) -> Result<Response<&'static str>, Rejection>
where
    B: BlobStore + Send + Sync + 'static,
    BUF: Buf,
    S: Stream<Item = Result<BUF, warp::Error>> + Unpin,
{
    receive_upload(
        repository,
        session_id,
        BlobPutQueryParams { digest: None },
        content_range,
        blob_store,
        byte_stream,
    )
    .await
}

async fn start_session_or_blob_upload<B, S, BUF>(
    repository: String,
    query_params: BlobPutQueryParams,
    blob_store: Arc<B>,
    byte_stream: S,
) -> Result<Response<&'static str>, Rejection>
where
    B: BlobStore + Send + Sync + 'static,
    BUF: Buf,
    S: Stream<Item = Result<BUF, warp::Error>> + Unpin,
{
    let session_id = blob_store.start_upload().await?;
    match query_params.digest {
        Some(ref _client_digest) => {
            receive_upload(
                repository,
                session_id,
                query_params,
                None,
                blob_store,
                byte_stream,
            )
            .await
        }
        None => {
            let location = format!(
                "/v2/{}/blobs/uploads/{}",
                repository,
                session_id.to_hyphenated_ref()
            );

            Ok(warp::http::response::Builder::new()
                .header("Location", location)
                .status(202)
                .body("")
                .unwrap())
        }
    }
}

async fn check_blob_exists<B: BlobStore + Send + Sync + 'static>(
    _repository: String,
    digest_raw: String,
    blob_store: Arc<B>,
) -> Result<Response<&'static str>, Rejection>
where
    B: BlobStore + Send + Sync + 'static,
{
    let digest = match digest::Digest::try_from(&digest_raw as &str) {
        Ok(d) => d,
        Err(_err) => {
            return Ok(warp::http::response::Builder::new()
                .status(StatusCode::BAD_REQUEST)
                .body("")
                .unwrap())
        }
    };

    let size = blob_store.blob_exists(&digest).await?;
    if size > 0 {
        Ok(warp::http::response::Builder::new()
            .header("Docker-Content-Digest", format!("{}", digest))
            .header("Content-Length", size)
            .status(StatusCode::OK)
            .body("")
            .unwrap())
    } else {
        Ok(warp::http::response::Builder::new()
            .status(StatusCode::NOT_FOUND)
            .body("")
            .unwrap())
    }
}

fn blob_exists<B: BlobStore + Send + Sync + 'static>(
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::head()
        .and(warp::path!("v2" / String / "blobs" / String))
        .and(warp::filters::ext::get::<Arc<B>>())
        .and_then(check_blob_exists)
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

fn blob_upload_post<B: BlobStore + Send + Sync + 'static>(
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::post()
        .and(warp::path!("v2" / String / "blobs" / "uploads"))
        .and(warp::query::<BlobPutQueryParams>())
        .and(warp::filters::ext::get::<Arc<B>>())
        .and(warp::filters::body::stream())
        .and_then(start_session_or_blob_upload)
}

fn blob_upload_patch<B: BlobStore + Send + Sync + 'static>(
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::patch()
        .and(warp::path!("v2" / String / "blobs" / "uploads" / Uuid))
        .and(warp::header::optional::<ContentRange>("Content-Range"))
        .and(warp::filters::ext::get::<Arc<B>>())
        .and(warp::filters::body::stream())
        .and_then(receive_patch_upload)
}

pub fn routes<B: BlobStore + Send + Sync + 'static>(
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    blob_exists::<B>()
        .or(blob_upload_post::<B>())
        .or(blob_upload_put::<B>())
        .or(blob_upload_patch::<B>())
}
