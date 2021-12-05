use async_lock::{Mutex, RwLock};
use async_trait::async_trait;
use bytes::Buf;
use core::pin::Pin;
use core::task::{Context, Poll};

use futures_util::{stream, Stream, StreamExt};
use serde::Deserialize;
use sha2::{Digest, Sha256, Sha512};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::Arc;

use std::io::{ErrorKind, SeekFrom};
use tokio::{
    fs,
    fs::{File, OpenOptions},
    io::{AsyncRead, AsyncSeekExt, AsyncWrite, AsyncWriteExt},
};
use tokio_util::codec::{BytesCodec, FramedRead};

use uuid::Uuid;
use warp::{
    http::{Response, StatusCode},
    Filter, Rejection, Reply,
};

use super::{auth, auth::principal::Principal};
use crate::error::{ErrorCode, ErrorResponse};
use crate::range::ContentRange;
use crate::repository::{repository, Repository};
use crate::{
    digest,
    digest::{deserialize_optional_digest_string, DigestAlgorithm},
};

/// Errors return from Blob Store operations.
#[derive(Debug)]
#[allow(dead_code)]
pub enum BlobStoreError {
    Io(std::io::Error),
    SessionNotFound(Uuid),
    UnsupportedDigest,
    NotFound,
    Unknown,
}

impl From<std::io::Error> for BlobStoreError {
    fn from(e: std::io::Error) -> Self {
        BlobStoreError::Io(e)
    }
}

impl warp::reject::Reject for BlobStoreError {}

/// Metadata about a stored blob. Retrieved
/// by clients to validate the existence of
/// a blob.
pub struct BlobMetadata {
    byte_size: u64,
}

/// Used to store blobs. Blobs are keyed by an upload session identifier, until
/// they are finalized. At finalization time, a digest is computed, and used as
/// the primary identifier for the blob going forward. A blob effectively becomes
/// immutable once the upload has been completed. Callers must either 'finalize'
/// a blob upload, or 'cancel' it to clean up temporary session upload state.
#[async_trait]
pub trait BlobStore {
    type Writer: AsyncWrite + Unpin + Send + Debug;
    type Reader: AsyncRead + Unpin + Send + Debug;

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

    /// Fetch the blob's metadata, if it exists. This is mainly
    /// used to check if the blob already exists in the store, and
    /// to retrieve the blob content byte size.
    async fn blob_metadata(&self, digest: &digest::Digest) -> Result<BlobMetadata, BlobStoreError>;

    /// Read the raw blob contents.
    async fn get_blob(&self, digest: &digest::Digest) -> Result<Self::Reader, BlobStoreError>;
}

/// Represents the state that needs to be tracked by the server
/// during upload. Since this holds open writers, it must be actively
/// cleaned up from the outstanding upload sessions that the server
/// is tracking once an upload is completed or aborted.
#[derive(Debug)]
pub struct UploadSession<W: AsyncWrite + Unpin + Send + Debug> {
    id: Uuid,
    writer: W,
    bytes_written: u64,
    sha256: Sha256,
    sha512: Sha512,
}

impl<W: AsyncWrite + Unpin + Send + Debug> AsyncWrite for UploadSession<W> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
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

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.writer).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.writer).poll_shutdown(cx)
    }
}

impl<W: AsyncWrite + Unpin + Send + Debug> UploadSession<W> {
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
    type Reader = File;

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

    async fn blob_metadata(&self, digest: &digest::Digest) -> Result<BlobMetadata, BlobStoreError> {
        match fs::metadata(
            self.root_directory
                .join("blobs")
                .join(format!("{}", digest)),
        )
        .await
        {
            Ok(metadata) => Ok(BlobMetadata {
                byte_size: metadata.len(),
            }),
            Err(err) if err.kind() == ErrorKind::NotFound => Err(BlobStoreError::NotFound),
            Err(err) => Err(err.into()),
        }
    }

    async fn get_blob(&self, digest: &digest::Digest) -> Result<Self::Reader, BlobStoreError> {
        match File::open(
            self.root_directory
                .join("blobs")
                .join(format!("{}", digest)),
        )
        .await
        {
            Ok(file) => Ok(file),
            Err(err) if err.kind() == ErrorKind::NotFound => Err(BlobStoreError::NotFound),
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
    repository: Repository,
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
        "Starting upload, repository: {:?}, session_id: {}",
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
                let location = format!("/v2/{}/blobs/{}", &repository.name, &computed_digest);
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
                    format!("/v2/{}/blobs/uploads/{}", &repository.name, &session_id),
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
    repository: Repository,
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
    repository: Repository,
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
    repository: Repository,
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
                &repository.name,
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
    _repository: Repository,
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

    match blob_store.blob_metadata(&digest).await {
        Ok(metadata) => Ok(warp::http::response::Builder::new()
            .header("Docker-Content-Digest", format!("{}", digest))
            .header("Content-Length", metadata.byte_size)
            .status(StatusCode::OK)
            .body("")
            .unwrap()),
        Err(BlobStoreError::NotFound) => Ok(warp::http::response::Builder::new()
            .status(StatusCode::NOT_FOUND)
            .body("")
            .unwrap()),
        Err(err) => Err(err)?,
    }
}

async fn fetch_blob<B: BlobStore + Send + Sync + 'static>(
    _repository: Repository,
    digest_raw: String,
    blob_store: Arc<B>,
) -> Result<Response<hyper::Body>, Rejection>
where
    B: BlobStore + Send + Sync + 'static,
{
    let digest = match digest::Digest::try_from(&digest_raw as &str) {
        Ok(d) => d,
        Err(_err) => {
            return Ok(warp::http::response::Builder::new()
                .status(StatusCode::BAD_REQUEST)
                .body(hyper::Body::wrap_stream(stream::empty::<
                    Result<bytes::Bytes, std::io::Error>,
                >()))
                .unwrap())
        }
    };

    match blob_store.get_blob(&digest).await {
        Ok(reader) => Ok(warp::http::response::Builder::new()
            .header("Docker-Content-Digest", format!("{}", digest))
            .status(StatusCode::OK)
            .body(hyper::Body::wrap_stream(FramedRead::new(
                reader,
                BytesCodec::new(),
            )))
            .unwrap()),
        Err(BlobStoreError::NotFound) => {
            tracing::info!("Could not find digest: {}", digest);
            Ok(warp::http::response::Builder::new()
                .status(StatusCode::NOT_FOUND)
                .body(hyper::Body::wrap_stream(stream::empty::<
                    Result<bytes::Bytes, std::io::Error>,
                >()))
                .unwrap())
        }
        Err(err) => Err(err)?,
    }
}

fn blob_exists<B: BlobStore + Send + Sync + 'static>(
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::head()
        .and(
            auth::authenticate()
                .and(repository())
                .and_then(auth::authorize),
        )
        .and(warp::path!("blobs" / String))
        .and(warp::filters::ext::get::<Arc<B>>())
        .and_then(check_blob_exists)
        .boxed()
}

fn blob_get<B: BlobStore + Send + Sync + 'static>(
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::get()
        .and(
            auth::authenticate()
                .and(repository())
                .and_then(auth::authorize),
        )
        .and(warp::path!("blobs" / String))
        .and(warp::filters::ext::get::<Arc<B>>())
        .and_then(fetch_blob)
        .boxed()
}

fn blob_upload_put<B: BlobStore + Send + Sync + 'static>(
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::put()
        .and(
            auth::authenticate()
                .and(repository())
                .and_then(auth::authorize),
        )
        .and(warp::path!("blobs" / "uploads" / Uuid))
        .and(warp::query::<BlobPutQueryParams>())
        .and(warp::filters::ext::get::<Arc<B>>())
        .and(warp::filters::body::stream())
        .and_then(receive_put_upload)
        .boxed()
}

fn blob_upload_post<B: BlobStore + Send + Sync + 'static>(
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::post()
        .and(
            auth::authenticate()
                .and(repository())
                .and_then(auth::authorize),
        )
        .and(warp::path!("blobs" / "uploads"))
        .and(warp::query::<BlobPutQueryParams>())
        .and(warp::filters::ext::get::<Arc<B>>())
        .and(warp::filters::body::stream())
        .and_then(start_session_or_blob_upload)
        .boxed()
}

fn blob_upload_patch<B: BlobStore + Send + Sync + 'static>(
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::patch()
        .and(
            auth::authenticate()
                .and(repository())
                .and_then(auth::authorize),
        )
        .and(warp::path!("blobs" / "uploads" / Uuid))
        .and(warp::header::optional::<ContentRange>("Content-Range"))
        .and(warp::filters::ext::get::<Arc<B>>())
        .and(warp::filters::body::stream())
        .and_then(receive_patch_upload)
        .boxed()
}

pub fn routes<B: BlobStore + Send + Sync + 'static>(
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    blob_exists::<B>()
        .or(blob_get::<B>())
        .or(blob_upload_post::<B>())
        .or(blob_upload_put::<B>())
        .or(blob_upload_patch::<B>())
}
