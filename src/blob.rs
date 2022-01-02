use async_trait::async_trait;
use bytes::Buf;
use chrono::{offset::Utc, DateTime, Duration};
use core::pin::Pin;
use core::task::{Context, Poll};
use tokio::sync::{Mutex, RwLock};

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

use ulid::Ulid;
use warp::{
    http::{Response, StatusCode},
    Filter, Rejection, Reply,
};

use crate::auth::resource::Action;
use crate::error::{ErrorCode, ErrorResponse};
use crate::lock::{LockManager, LockRef};
use crate::manifest::ManifestStore;
use crate::range::ContentRange;
use crate::repository::{authorize_repository, Repository};
use crate::{
    digest,
    digest::{deserialize_optional_digest_string, DigestAlgorithm},
};

/// Errors return from Blob Store operations.
#[derive(Debug)]
#[allow(dead_code)]
pub enum BlobStoreError {
    Io(std::io::Error),
    SessionNotFound(Ulid),
    InvalidUlid(ulid::DecodeError),
    UnsupportedDigest,
    NotFound,
    Unknown,
}

impl From<std::io::Error> for BlobStoreError {
    fn from(e: std::io::Error) -> Self {
        BlobStoreError::Io(e)
    }
}

impl From<ulid::DecodeError> for BlobStoreError {
    fn from(e: ulid::DecodeError) -> Self {
        BlobStoreError::InvalidUlid(e)
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
///
/// Once blobs are written, they remain in storage until they are no longer
/// referenced by any manifests, at which point they will be garbage collected
/// and deleted.
///
/// Implementors of this trait can assume that concurrent access to individual
/// blobs is handled a LockingBlobStore implementation, which wraps around a
/// basic BlobStore operation that doesn't need to implement safe concurrent blob
/// access.
#[async_trait]
pub trait BlobStore {
    type Writer: AsyncWrite + Unpin + Send + Debug;
    type Reader: AsyncRead + Unpin + Send + Debug;

    /// Begin uploading a blob, returns the session_id as a Ulid. After calling,
    /// clients should now be able to call 'get_session'.
    async fn start_upload(&self) -> Result<Ulid, BlobStoreError>;

    /// Retrieve the underlying session, and its associated writer, at the given
    /// byte offset position. Clients must call 'start_upload' to create the upload
    /// session before calling this function.
    async fn get_session(
        &self,
        session_id: &Ulid,
        pos: u64,
    ) -> Result<Arc<Mutex<UploadSession<Self::Writer>>>, BlobStoreError>;

    /// Prune old sessions that are older than a given duration from the
    /// passed in current time. Because uploads can be interrupted and
    /// restarted, we give them a grace period where they can theoretically
    /// be initiated by the client again. This function implements the cleanup
    /// for sessions that get permanently abandoned.
    async fn prune_inactive_sessions(
        &self,
        older_than: Duration,
        current_time: DateTime<Utc>,
    ) -> Result<(u64, u64), BlobStoreError>;

    /// Finalize the upload session, producing an immutable Digest that will
    /// be used as the blob identifier going forward. Implementations can
    /// remove any temporary state that's being held for upload sessions, and
    /// transition the blob to stable, long-term, immutable storage.
    async fn finalize_upload(
        &self,
        session_id: &Ulid,
        digest: &digest::Digest,
    ) -> Result<(), BlobStoreError>;

    /// Cancel the upload, removing all temporary upload state.
    async fn cancel_upload(&self, session_id: &Ulid) -> Result<(), BlobStoreError>;

    /// Fetch the blob's metadata, if it exists. This is mainly
    /// used to check if the blob already exists in the store, and
    /// to retrieve the blob content byte size.
    async fn blob_metadata(&self, digest: &digest::Digest) -> Result<BlobMetadata, BlobStoreError>;

    /// Read the raw blob contents.
    async fn get_blob(&self, digest: &digest::Digest) -> Result<Self::Reader, BlobStoreError>;

    /// Delete a blob by its content digest. By the time this method is called
    /// it's guaranteed that there are no longer any references to this blob
    /// that are reachable.
    async fn delete_blob(&self, digest: &digest::Digest) -> Result<(), BlobStoreError>;
}

/// BlobStore implementation that performs locking on
/// blobs keyed by content digest before performing any
/// underlying operations.
///
/// We need granular locks on each blob, to make sure that
/// we:
///
/// 1. Maintain internal consistency on blob reference counting, to
///    make sure we only delete blobs that truly have no more manifest
///    references.
/// 2. We don't accidentally delete a blob that might have concurrent
///    reads being performed on it.
///
pub struct LockingBlobStore<B: BlobStore + Send + Sync + 'static> {
    /// The underlying BlobStore that this wraps.
    delegate: B,

    /// Blob locks. A LockRef must be acquired and used, for each blob
    /// before any read / write operations are perfomed the blob.
    locks: LockManager<digest::Digest>,
}

impl<B: BlobStore + Send + Sync + 'static> LockingBlobStore<B> {
    pub fn new(delegate: B) -> Self {
        Self {
            delegate,
            locks: LockManager::new(),
        }
    }

    pub fn locks(&self) -> BlobLocks {
        BlobLocks {
            locks: self.locks.clone(),
        }
    }
}

#[async_trait]
impl<B: BlobStore + Send + Sync + 'static> BlobStore for LockingBlobStore<B> {
    type Writer = B::Writer;
    type Reader = B::Reader;

    async fn start_upload(&self) -> Result<Ulid, BlobStoreError> {
        // No locking necessary. Sessions are transient, and exclusive
        // to a single client. Go straight to delegate store.
        self.delegate.start_upload().await
    }

    async fn get_session(
        &self,
        session_id: &Ulid,
        pos: u64,
    ) -> Result<Arc<Mutex<UploadSession<Self::Writer>>>, BlobStoreError> {
        // No locking necessary. Sessions are transient, and exclusive
        // to a single client. Go straight to delegate store.
        self.delegate.get_session(session_id, pos).await
    }

    async fn prune_inactive_sessions(
        &self,
        older_than: Duration,
        current_time: DateTime<Utc>,
    ) -> Result<(u64, u64), BlobStoreError> {
        self.delegate
            .prune_inactive_sessions(older_than, current_time)
            .await
    }

    async fn finalize_upload(
        &self,
        session_id: &Ulid,
        digest: &digest::Digest,
    ) -> Result<(), BlobStoreError> {
        // Exclusive lock on the blob before finalizing it into
        // permananent storage.
        let blob_lock = self.locks.acquire_ref(digest.clone());
        let _blob_lock_guard = blob_lock.write().await;

        self.delegate.finalize_upload(session_id, digest).await
    }

    async fn cancel_upload(&self, session_id: &Ulid) -> Result<(), BlobStoreError> {
        // No locking necessary, go straight to delegate store.
        self.delegate.cancel_upload(session_id).await
    }

    async fn blob_metadata(&self, digest: &digest::Digest) -> Result<BlobMetadata, BlobStoreError> {
        // Read lock on the blob before fetching metadata
        let blob_lock = self.locks.acquire_ref(digest.clone());
        let _blob_lock_guard = blob_lock.read().await;

        self.delegate.blob_metadata(digest).await
    }

    async fn get_blob(&self, digest: &digest::Digest) -> Result<Self::Reader, BlobStoreError> {
        // Read lock on the blob before reading blob content.
        let blob_lock = self.locks.acquire_ref(digest.clone());
        let _blob_lock_guard = blob_lock.read().await;

        self.delegate.get_blob(digest).await
    }

    async fn delete_blob(&self, digest: &digest::Digest) -> Result<(), BlobStoreError> {
        // We must acquire an exclusive write lock during
        // our blob delete operation.
        let blob_lock = self.locks.acquire_ref(digest.clone());
        let _blob_lock_guard = blob_lock.write().await;

        self.delegate.delete_blob(digest).await
    }
}

/// We need this struct wrapper, so that we can pass around
/// a type that warp can fetch using its 'AnyMap' in request
/// extensions. It basically just wraps the LockManager.
#[derive(Debug, Clone)]
pub struct BlobLocks {
    locks: LockManager<digest::Digest>,
}

impl BlobLocks {
    pub fn acquire_blob_lock_ref(&self, blob_id: digest::Digest) -> LockRef<digest::Digest> {
        self.locks.acquire_ref(blob_id)
    }
}

/// Represents the state that needs to be tracked by the server
/// during upload. Since this holds open writers, it must be actively
/// cleaned up from the outstanding upload sessions that the server
/// is tracking once an upload is completed or aborted.
#[derive(Debug)]
pub struct UploadSession<W: AsyncWrite + Unpin + Send + Debug> {
    id: Ulid,
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
    fn new(writer: W, id: Ulid) -> Self {
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

/// Background 'janitor' that cleans up old upload sessions, and
/// deletes blobs that are orphaned.
pub struct BlobJanitor<B: BlobStore + Send + Sync + 'static> {
    blob_store: Arc<B>,
    period: core::time::Duration,
}

impl<B: BlobStore + Send + Sync + 'static> BlobJanitor<B> {
    pub fn new(blob_store: Arc<B>, period: core::time::Duration) -> Self {
        Self { blob_store, period }
    }

    pub async fn cleanup(&self) -> Result<(), BlobStoreError> {
        tracing::info!("Blob janitor starting up with period: {:?}", self.period);
        let mut interval = tokio::time::interval(self.period);

        loop {
            let tick = interval.tick().await;
            tracing::info!("Blob janitor starting to clean up the mess, at: {:?}", tick);
            let (map_pruned, fs_pruned) = self
                .blob_store
                .prune_inactive_sessions(Duration::hours(1), Utc::now())
                .await?;
            tracing::info!(
                "Blob Janitor done. Cleaned up {} sessions in the session map, and {} sessions on the filesystem",
                map_pruned,
                fs_pruned
            )
        }
    }
}

pub struct FsBlobStore {
    root_directory: PathBuf,

    /// Sessions are potentially accessed across multiple different
    /// endpoint calls, since we want to support chunked uploading. We
    /// must wrap each session in its own lock for safety.
    ///
    /// This gets periodically cleaned up by the BlobJanitor.
    ///
    sessions: RwLock<HashMap<Ulid, Arc<Mutex<UploadSession<File>>>>>,
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

    async fn start_upload(&self) -> Result<Ulid, BlobStoreError> {
        let session_id = Ulid::new();
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(
                self.root_directory
                    .join("sessions")
                    .join(format!("{}", session_id.to_string())),
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
        session_id: &Ulid,
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

    async fn prune_inactive_sessions(
        &self,
        older_than: Duration,
        current_time: DateTime<Utc>,
    ) -> Result<(u64, u64), BlobStoreError> {
        let mut sessions_to_prune = Vec::new();
        let mut read_dir = fs::read_dir(self.root_directory.join("sessions")).await?;
        while let Some(entry) = read_dir.next_entry().await? {
            let filename = match entry.file_name().into_string() {
                Ok(utf8_string) => utf8_string,
                Err(invalid_file) => {
                    tracing::warn!("Invalid utf8 filename: {:?}", invalid_file);
                    continue;
                }
            };

            let session_id = ulid::Ulid::from_string(&filename)?;
            let session_datetime = session_id.datetime();

            if (current_time - session_datetime) > older_than {
                sessions_to_prune.push(session_id);
            }
        }

        // Now prune sessions
        let mut map_pruned = 0;
        let mut fs_pruned = 0;
        let mut sessions = self.sessions.write().await;
        for session_id in sessions_to_prune.iter() {
            if sessions.remove(&session_id).is_some() {
                map_pruned += 1;
            }
            if fs::remove_file(
                self.root_directory
                    .join("sessions")
                    .join(format!("{}", session_id.to_string())),
            )
            .await
            .is_ok()
            {
                fs_pruned += 1;
            }
        }

        Ok((map_pruned, fs_pruned))
    }

    async fn finalize_upload(
        &self,
        session_id: &Ulid,
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
                .join(format!("{}", session_id.to_string())),
            self.root_directory
                .join("blobs")
                .join(format!("{}", digest)),
        )
        .await?;

        Ok(())
    }

    async fn cancel_upload(&self, session_id: &Ulid) -> Result<(), BlobStoreError> {
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
                .join(format!("{}", session_id.to_string())),
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

    async fn delete_blob(&self, digest: &digest::Digest) -> Result<(), BlobStoreError> {
        let path = self
            .root_directory
            .join("blobs")
            .join(format!("{}", digest));

        fs::remove_file(&path).await?;
        Ok(())
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
    session_id: Ulid,
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
                tracing::trace!("Got byte buffer, remaining: {}", b.remaining());
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
                .header("Docker-Upload-UUID", session_id.to_string())
                .header("Range", range)
                .status(StatusCode::ACCEPTED)
                .body("")
                .unwrap())
        }
    }
}

async fn receive_put_upload<B, S, BUF>(
    repository: Repository,
    session_id: Ulid,
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
    session_id: Ulid,
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
                session_id.to_string()
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

fn blob_exists<B, M>() -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone
where
    M: ManifestStore + Send + Sync + 'static,
    B: BlobStore + Send + Sync + 'static,
{
    warp::head()
        .and(authorize_repository::<M>(Action::Read))
        .and(warp::path!("blobs" / String))
        .and(warp::filters::ext::get::<Arc<B>>())
        .and_then(check_blob_exists)
        .boxed()
}

fn blob_get<B, M>() -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone
where
    M: ManifestStore + Send + Sync + 'static,
    B: BlobStore + Send + Sync + 'static,
{
    warp::get()
        .and(authorize_repository::<M>(Action::Read))
        .and(warp::path!("blobs" / String))
        .and(warp::filters::ext::get::<Arc<B>>())
        .and_then(fetch_blob)
        .boxed()
}

fn blob_upload_put<B, M>() -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone
where
    M: ManifestStore + Send + Sync + 'static,
    B: BlobStore + Send + Sync + 'static,
{
    warp::put()
        .and(authorize_repository::<M>(Action::Write))
        .and(warp::path!("blobs" / "uploads" / Ulid))
        .and(warp::query::<BlobPutQueryParams>())
        .and(warp::filters::ext::get::<Arc<B>>())
        .and(warp::filters::body::stream())
        .and_then(receive_put_upload)
        .boxed()
}

fn blob_upload_post<B, M>() -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone
where
    M: ManifestStore + Send + Sync + 'static,
    B: BlobStore + Send + Sync + 'static,
{
    warp::post()
        .and(authorize_repository::<M>(Action::Write))
        .and(warp::path!("blobs" / "uploads"))
        .and(warp::query::<BlobPutQueryParams>())
        .and(warp::filters::ext::get::<Arc<B>>())
        .and(warp::filters::body::stream())
        .and_then(start_session_or_blob_upload)
        .boxed()
}

fn blob_upload_patch<B, M>() -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone
where
    M: ManifestStore + Send + Sync + 'static,
    B: BlobStore + Send + Sync + 'static,
{
    warp::patch()
        .and(authorize_repository::<M>(Action::Write))
        .and(warp::path!("blobs" / "uploads" / Ulid))
        .and(warp::header::optional::<ContentRange>("Content-Range"))
        .and(warp::filters::ext::get::<Arc<B>>())
        .and(warp::filters::body::stream())
        .and_then(receive_patch_upload)
        .boxed()
}

pub fn routes<B, M>() -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone
where
    M: ManifestStore + Send + Sync + 'static,
    B: BlobStore + Send + Sync + 'static,
{
    blob_exists::<B, M>()
        .or(blob_get::<B, M>())
        .or(blob_upload_post::<B, M>())
        .or(blob_upload_put::<B, M>())
        .or(blob_upload_patch::<B, M>())
}

#[cfg(test)]
use tempfile::tempdir;

#[tokio::test]
async fn test_fs_blob_store_pruning_inactive_upload_sessions() {
    let store_path = tempdir().unwrap().into_path();
    let blob_store = FsBlobStore::open(store_path).expect("Could not open FS blob store");
    let session_id = blob_store
        .start_upload()
        .await
        .expect("Could not start upload session");

    let _restarted_session = blob_store
        .get_session(&session_id, 0)
        .await
        .expect("Could not retrieve existing upload session");

    // Simulate more time than our duration passing.
    let duration = Duration::hours(1);
    let current_time = Utc::now() + duration + Duration::seconds(1);

    let (map_pruned, fs_pruned) = blob_store
        .prune_inactive_sessions(duration, current_time)
        .await
        .expect("Could not prune inactive sessions");

    assert_eq!(1, map_pruned);
    assert_eq!(1, fs_pruned);

    let not_found_session = blob_store
        .get_session(&session_id, 0)
        .await
        .expect_err("Inactive session should be missing");
    match not_found_session {
        BlobStoreError::SessionNotFound(_) => {}
        e => panic!("Session should be not found, instead got: {:?}", e),
    }
}
