use async_trait::async_trait;
use bp_protos::{manifest as protos, repository as repo_protos, ulid as ulid_util};
use bytes::{Bytes, BytesMut};
use chrono::offset::Utc;
use heed::types::{ByteSlice, OwnedType};
use heed::RwTxn;
use prost::Message;
use sha2::{Digest, Sha256};
use std::convert::TryFrom;
use std::sync::Arc;
use warp::{
    http::{Method, Response, StatusCode},
    Filter, Rejection, Reply,
};

use crate::auth::resource::Action;
use crate::blob::{BlobLocks, BlobStoreError};
use crate::digest;
use crate::error::{ErrorCode, ErrorResponse};
use crate::repository::{authorize_repository, Repository};

/// Simple increment / decrement enum.
/// TODO: Move somewhere else?
/// Replace with mixed integer functions once they land
/// in stable? https://github.com/rust-lang/rust/pull/87601
#[derive(Debug, Clone, Copy)]
pub enum CountDiff {
    Inc(u64),
    Dec(u64),
}

/// The minimum 'common denominator' fields we need on each layer
/// or media item.
#[allow(dead_code)]
pub struct CanonicalMedia {
    media_type: String,
    size: u64,
    digest: digest::Digest,
}

/// Represents the 'common denominator' image manifest that we operate
/// on top of.
#[allow(dead_code)]
pub struct CanonicalImageManifest {
    layers: Vec<CanonicalMedia>,
}

impl TryFrom<&protos::MediaV2> for CanonicalMedia {
    type Error = ImageManifestError;

    fn try_from(proto: &protos::MediaV2) -> Result<Self, Self::Error> {
        let digest = digest::Digest::try_from(proto.digest.as_str())
            .map_err(|msg| ImageManifestError::Conversion(msg))?;
        Ok(Self {
            media_type: proto.media_type.clone(),
            size: proto.size,
            digest,
        })
    }
}

impl TryFrom<&protos::ImageManifest> for CanonicalImageManifest {
    type Error = ImageManifestError;

    fn try_from(proto: &protos::ImageManifest) -> Result<Self, Self::Error> {
        match proto.manifest_version {
            Some(protos::image_manifest::ManifestVersion::V2(ref v2)) => {
                let mut layers = Vec::new();
                for layer in &v2.layers {
                    let media = CanonicalMedia::try_from(layer)?;
                    layers.push(media);
                }
                Ok(Self { layers })
            }
            None => Err(ImageManifestError::IllegalState(
                "Got no manifest version. Should always have one!",
            )),
        }
    }
}

#[derive(Debug)]
pub enum ImageManifestError {
    UnknownMediaType(String),
    Conversion(String),
    IllegalState(&'static str),
    Serde(serde_json::error::Error),
}

impl From<serde_json::error::Error> for ImageManifestError {
    fn from(err: serde_json::error::Error) -> Self {
        Self::Serde(err)
    }
}

pub fn parse_manifest_json(
    media_type: &str,
    json: &Bytes,
) -> Result<protos::ImageManifest, ImageManifestError> {
    let created_time = Utc::now().timestamp();
    match media_type {
        "application/vnd.docker.distribution.manifest.v2+json" => {
            let manifest_v2: protos::ImageManifestV2 = serde_json::from_slice(json)?;
            let manifest = protos::ImageManifest {
                status: protos::ManifestStatus::Active as i32,
                created_at_seconds: created_time,
                manifest_version: Some(protos::image_manifest::ManifestVersion::V2(manifest_v2)),
            };
            Ok(manifest)
        }
        _ => Err(ImageManifestError::UnknownMediaType(media_type.to_string())),
    }
}

#[derive(Debug)]
pub enum ManifestStoreError {
    ProstEncode(prost::EncodeError),
    ProstDecode(prost::DecodeError),
    NotFound,
    JoinError(tokio::task::JoinError),
    Lmdb(heed::Error),
    Conversion(ImageManifestError),
    DigestDecode(String),
}

impl warp::reject::Reject for ManifestStoreError {}

impl From<heed::Error> for ManifestStoreError {
    fn from(err: heed::Error) -> Self {
        match err {
            heed::Error::Mdb(heed::MdbError::NotFound) => ManifestStoreError::NotFound,
            err => ManifestStoreError::Lmdb(err),
        }
    }
}

impl From<tokio::task::JoinError> for ManifestStoreError {
    fn from(err: tokio::task::JoinError) -> Self {
        ManifestStoreError::JoinError(err)
    }
}

impl From<prost::EncodeError> for ManifestStoreError {
    fn from(err: prost::EncodeError) -> Self {
        ManifestStoreError::ProstEncode(err)
    }
}

impl From<prost::DecodeError> for ManifestStoreError {
    fn from(err: prost::DecodeError) -> Self {
        ManifestStoreError::ProstDecode(err)
    }
}

/// Statistics that get produced during a garbage collect operation. Mostly
/// used for blob reference count mark and sweep operations.
#[derive(Debug, Clone, Copy)]
pub struct GCStats {
    /// Number of items scanned during the gc run.
    items_scanned: u64,

    /// Number of items that are 'dead'
    items_dead: u64,

    /// Number of items that were 'sweeped' during the gc run.
    items_swept: u64,
}

/// Used to store and retrieve manifests. Manifests are keyed by
/// their content addressable digest.
#[async_trait]
pub trait ManifestStore {
    /// Get or create a repository by its name. We mostly use this
    /// to exchange a repository name for its unique identifier.
    async fn get_or_create_repository(
        &self,
        repository_name: &str,
    ) -> Result<repo_protos::Repository, ManifestStoreError>;

    /// Store a raw manifest, with the given digest as the
    /// key, the content_type of the payload, and the raw
    /// payload itself.
    async fn store_manifest(
        &self,
        digest: &digest::Digest,
        blob_locks: BlobLocks,
        manifest: &protos::ImageManifest,
        content_type: String,
        raw_manifest_payload: Bytes,
    ) -> Result<(), ManifestStoreError>;

    /// Lookup a manifest by its content addressable digest.
    async fn get_manifest(
        &self,
        digest: &digest::Digest,
    ) -> Result<protos::RawManifest, ManifestStoreError>;

    /// Delete a manifest by digest.
    async fn delete_manifest(
        &self,
        digest: &digest::Digest,
        blob_locks: BlobLocks,
    ) -> Result<(), ManifestStoreError>;

    /// Add a tag / reference in the repository to an already stored manifest. The
    /// manifest must be already stored via 'store_manifest' in the given repository
    /// for this to work.
    async fn tag_manifest(
        &self,
        repository: &Repository,
        reference: &str,
        manifest_digest: &digest::Digest,
    ) -> Result<(), ManifestStoreError>;

    /// Fetch all tags for a given repository.
    async fn get_repository_tags(
        &self,
        repository: &Repository,
    ) -> Result<protos::RepositoryTags, ManifestStoreError>;

    /// Lookup the image retention policy for the given
    /// repository.
    async fn get_repository_retention_policy(
        &self,
        repository: &Repository,
    ) -> Result<repo_protos::ImageRetentionPolicy, ManifestStoreError>;

    /// Execute a blocking garbage collect operation. Note that garbage
    /// collection does not happen asynchronously.
    fn garbage_collect_blobs(
        &self,
        blob_locks: BlobLocks,
        sweep_fn: Box<dyn FnMut(&digest::Digest) -> Result<(), ManifestStoreError> + Send + Sync>,
    ) -> Result<GCStats, ManifestStoreError>;
}

pub struct LmdbManifestStore {
    // Top level LMDB 'environment'. All sub databases below are part of
    // the same high-level 'database', and can all participate in transactions
    // together.
    env: heed::Env,

    // Repositories
    repositories: heed::Database<ByteSlice, ByteSlice>,

    // Protobuf parsed / validated manifests. Written with protos::ImageManifest.
    manifests: heed::Database<ByteSlice, ByteSlice>,

    // Reference counts for each blob that a manifest points to. We
    // need to track this since multiple manifests can point to a blob,
    // and we need to know when we can safetly delete blobs (once no more
    // manifests point to them).
    manifest_blob_references: heed::Database<ByteSlice, OwnedType<u64>>,

    // Raw manifests. Written with protos::RawManifest.
    raw_manifests: heed::Database<ByteSlice, ByteSlice>,

    // Repository tags. Written with protos::RepositoryTags.
    repository_tags: heed::Database<ByteSlice, ByteSlice>,
}

impl LmdbManifestStore {
    pub fn open(env: heed::Env) -> Result<Self, ManifestStoreError> {
        let manifests = env.create_database(Some("manifests"))?;
        let repositories = env.create_database(Some("repositories"))?;
        let manifest_blob_references = env.create_database(Some("manifest_blob_references"))?;
        let raw_manifests = env.create_database(Some("raw_manifests"))?;
        let repository_tags = env.create_database(Some("repository_tags"))?;
        Ok(Self {
            env,
            repositories,
            manifests,
            manifest_blob_references,
            raw_manifests,
            repository_tags,
        })
    }

    /// Adjust the reference count for all the layers / blobs that a
    /// particular manifest points to.
    fn apply_manifest_blob_references_diff(
        txn: &mut RwTxn,
        blob_locks: &BlobLocks,
        manifest_blob_references: heed::Database<ByteSlice, OwnedType<u64>>,
        manifest: &CanonicalImageManifest,
        diff: CountDiff,
    ) -> Result<(), ManifestStoreError> {
        for layer in &manifest.layers {
            Self::apply_blob_reference_diff(
                txn,
                blob_locks,
                &manifest_blob_references,
                &layer.digest,
                diff,
            )?;
        }
        Ok(())
    }

    fn apply_blob_reference_diff(
        txn: &mut RwTxn,
        blob_locks: &BlobLocks,
        manifest_blob_references: &heed::Database<ByteSlice, OwnedType<u64>>,
        digest: &digest::Digest,
        diff: CountDiff,
    ) -> Result<(), ManifestStoreError> {
        // We need to acquire any blob locks before we're adjusting
        // reference counts. If all our state was kept internally in
        // LMDB, this wouldn't be necessary, as the writer would enable
        // exclusive mutation access: However, because we store blobs externally
        // from LMDB, we also need to hold any locks before we adjust internal
        // state. This prevents external deletions / mutations from occuring
        let lock_ref = blob_locks.acquire_blob_lock_ref(digest.clone());
        let _blob_guard = futures::executor::block_on(async { lock_ref.write().await });

        let digest_bytes = digest.get_bytes();
        let mut ref_count = manifest_blob_references
            .get(txn, &digest_bytes)?
            .unwrap_or(0);

        // Use saturating addition / subtraction, to make sure we never
        // overflow. Should never happen in practice.
        ref_count = match diff {
            CountDiff::Inc(n) => ref_count.saturating_add(n),
            CountDiff::Dec(n) => ref_count.saturating_sub(n),
        };

        manifest_blob_references
            .put(txn, &digest_bytes, &ref_count)
            .map_err(|err| err.into())
    }
}

#[async_trait]
impl ManifestStore for LmdbManifestStore {
    async fn get_or_create_repository(
        &self,
        repository_name: &str,
    ) -> Result<repo_protos::Repository, ManifestStoreError> {
        let env = self.env.clone();
        let repositories = self.repositories.clone();
        let repository_name_owned = repository_name.to_string();

        tokio::task::spawn_blocking(move || {
            // We deliberately break this call into multiple transactions,
            // because it's not worth it for us to take out an exclusive
            // write lock on every single lookup, when creating new repositories
            // should be a relatively rare event.

            let repository_key = repository_name_owned.as_bytes();
            let read_txn = env.read_txn()?;
            match repositories.get(&read_txn, &repository_key)? {
                Some(buf) => {
                    let repo = repo_protos::Repository::decode(buf)?;
                    read_txn.commit()?;
                    Ok(repo)
                }
                None => {
                    read_txn.commit()?;
                    let mut write_txn = env.write_txn()?;

                    // Because we don't hold an exclusive lock on all reads, we might
                    // have a race where multiple transactions try to create a new repository.
                    // We need to do yet another lookup inside an exclusive write transaction
                    // to make sure no one has raced us to create the repo.
                    if let Some(buf) = repositories.get(&mut write_txn, &repository_key)? {
                        let repo = repo_protos::Repository::decode(buf)?;
                        tracing::info!(
                            "Another transaction created the repository, returning repo: {:?}",
                            repo
                        );
                        write_txn.abort()?;
                        return Ok(repo);
                    }

                    let new_repo = repo_protos::Repository {
                        id: Some(ulid_util::new_proto()),
                        repository_type: repo_protos::RepositoryType::OciV2 as i32,
                        name: repository_name_owned.to_string(),
                    };
                    tracing::info!("Creating repository: {:?}", new_repo);
                    let mut repository_buf = BytesMut::new();
                    new_repo.encode(&mut repository_buf)?;
                    repositories.put(&mut write_txn, &repository_key, &repository_buf)?;
                    write_txn.commit()?;
                    Ok(new_repo)
                }
            }
        })
        .await?
    }

    async fn store_manifest(
        &self,
        digest: &digest::Digest,
        blob_locks: BlobLocks,
        manifest: &protos::ImageManifest,
        content_type: String,
        raw_manifest_payload: Bytes,
    ) -> Result<(), ManifestStoreError> {
        let canonical_manifest = CanonicalImageManifest::try_from(manifest)
            .map_err(|err| ManifestStoreError::Conversion(err))?;

        // DB handles.
        let env = self.env.clone();
        let raw_manifests = self.raw_manifests.clone();
        let manifests = self.manifests.clone();
        let manifest_blob_references = self.manifest_blob_references.clone();

        // Manifest payloads
        let digest_bytes = digest.get_bytes();
        let raw_manifest = protos::RawManifest {
            status: protos::ManifestStatus::Active as i32,
            content_type: content_type,
            raw_payload: raw_manifest_payload.to_vec(),
        };
        let mut raw_manifest_buf = BytesMut::new();
        let mut manifest_buf = BytesMut::new();
        raw_manifest.encode(&mut raw_manifest_buf)?;
        manifest.encode(&mut manifest_buf)?;

        tokio::task::spawn_blocking(move || {
            let mut tx = env.write_txn()?;
            // Store both the raw manifest payload...
            raw_manifests.put(&mut tx, &digest_bytes, &raw_manifest_buf)?;

            // As well as the protobuf encoded version.
            manifests.put(&mut tx, &digest_bytes, &manifest_buf)?;

            // Now we need to increment the reference count
            // for every blob that this manifest references.
            Self::apply_manifest_blob_references_diff(
                &mut tx,
                &blob_locks,
                manifest_blob_references,
                &canonical_manifest,
                CountDiff::Inc(1),
            )?;

            tx.commit().map_err(|err| err.into())
        })
        .await?
    }

    async fn get_manifest(
        &self,
        digest: &digest::Digest,
    ) -> Result<protos::RawManifest, ManifestStoreError> {
        let env = self.env.clone();
        let raw_manifests = self.raw_manifests.clone();
        let digest_bytes = digest.get_bytes();
        tokio::task::spawn_blocking(move || {
            let tx = env.read_txn()?;
            let buf = match raw_manifests.get(&tx, &digest_bytes)? {
                Some(b) => Ok(b),
                None => Err(ManifestStoreError::NotFound),
            }?;
            let raw_manifest = protos::RawManifest::decode(buf)?;
            if raw_manifest.status == protos::ManifestStatus::Deleted as i32 {
                Err(ManifestStoreError::NotFound)
            } else {
                Ok(raw_manifest)
            }
        })
        .await?
    }

    async fn delete_manifest(
        &self,
        digest: &digest::Digest,
        blob_locks: BlobLocks,
    ) -> Result<(), ManifestStoreError> {
        let env = self.env.clone();
        let manifests = self.manifests.clone();
        let raw_manifests = self.raw_manifests.clone();
        let manifest_blob_references = self.manifest_blob_references.clone();
        let digest_bytes = digest.get_bytes();

        tokio::task::spawn_blocking(move || {
            let mut tx = env.write_txn()?;
            let mut raw_manifest = raw_manifests
                .get(&tx, &digest_bytes)?
                .ok_or(ManifestStoreError::NotFound)
                .and_then(|buf| protos::RawManifest::decode(buf).map_err(|err| err.into()))?;
            let mut manifest = manifests
                .get(&tx, &digest_bytes)?
                .ok_or(ManifestStoreError::NotFound)
                .and_then(|buf| protos::ImageManifest::decode(buf).map_err(|err| err.into()))?;

            // The manifest has already been deleted, bail early.
            if raw_manifest.status == protos::ManifestStatus::Deleted as i32
                || manifest.status == protos::ManifestStatus::Deleted as i32
            {
                return Err(ManifestStoreError::NotFound);
            }

            let canonical_manifest = CanonicalImageManifest::try_from(&manifest)
                .map_err(|err| ManifestStoreError::Conversion(err))?;

            raw_manifest.status = protos::ManifestStatus::Deleted as i32;
            manifest.status = protos::ManifestStatus::Deleted as i32;

            let mut raw_manifest_deleted = BytesMut::new();
            let mut manifest_deleted = BytesMut::new();

            raw_manifest.encode(&mut raw_manifest_deleted)?;
            manifest.encode(&mut manifest_deleted)?;

            raw_manifests.put(&mut tx, &digest_bytes, &raw_manifest_deleted)?;
            manifests.put(&mut tx, &digest_bytes, &manifest_deleted)?;

            // Decrement all blob references in this manifest
            Self::apply_manifest_blob_references_diff(
                &mut tx,
                &blob_locks,
                manifest_blob_references,
                &canonical_manifest,
                CountDiff::Dec(1),
            )?;

            tx.commit().map_err(|err| err.into())
        })
        .await?
    }

    async fn tag_manifest(
        &self,
        repository: &Repository,
        reference: &str,
        manifest_digest: &digest::Digest,
    ) -> Result<(), ManifestStoreError> {
        let env = self.env.clone();
        let repository_tags = self.repository_tags.clone();
        let digest_str = format!("{}", manifest_digest);
        let repository_name = repository.name.to_string();
        let reference_cloned = reference.to_string();
        let key = Vec::from(ulid_util::ulid_bytes(&repository.id));

        tokio::task::spawn_blocking(move || {
            let mut tx = env.write_txn()?;
            let new_tag = protos::TagReference {
                tag_name: reference_cloned,
                manifest_digest: digest_str,
            };

            let repo_tags = match repository_tags.get(&tx, &key)? {
                Some(b) => {
                    let mut existing = protos::RepositoryTags::decode(b)?;
                    existing.tag_references.push(new_tag);
                    Ok::<_, ManifestStoreError>(existing)
                }
                None => Ok(protos::RepositoryTags {
                    repository: repository_name,
                    tag_references: vec![new_tag],
                }),
            }?;

            let mut value = BytesMut::new();
            repo_tags.encode(&mut value)?;
            repository_tags.put(&mut tx, &key, &value)?;
            tx.commit().map_err(|err| err.into())
        })
        .await?
    }

    async fn get_repository_tags(
        &self,
        repository: &Repository,
    ) -> Result<protos::RepositoryTags, ManifestStoreError> {
        let env = self.env.clone();
        let repository_tags = self.repository_tags.clone();
        let key = Vec::from(ulid_util::ulid_bytes(&repository.id));

        tokio::task::spawn_blocking(move || {
            let tx = env.read_txn()?;
            let buf = repository_tags
                .get(&tx, &key)?
                .ok_or(ManifestStoreError::NotFound)?;
            Ok(protos::RepositoryTags::decode(buf)?)
        })
        .await?
    }

    async fn get_repository_retention_policy(
        &self,
        repository: &Repository,
    ) -> Result<repo_protos::ImageRetentionPolicy, ManifestStoreError> {
        // TODO: Until we have a way to define a repository level setting
        // for this, let's just pick a default.

        Ok(repo_protos::ImageRetentionPolicy {
            repository_id: Some(ulid_util::encode_to_proto(repository.id)),
            policy: Some(
                repo_protos::image_retention_policy::Policy::KeepRecentCount(
                    repo_protos::KeepRecentCountPolicy { last_count: 5 },
                ),
            ),
        })
    }

    fn garbage_collect_blobs(
        &self,
        blob_locks: BlobLocks,
        mut sweep_fn: Box<
            dyn FnMut(&digest::Digest) -> Result<(), ManifestStoreError> + Send + Sync,
        >,
    ) -> Result<GCStats, ManifestStoreError> {
        let mut items_scanned = 0;
        let mut items_dead = 0;
        let mut items_swept = 0;

        let txn = self.env.read_txn()?;
        let mut blob_ref_it = self.manifest_blob_references.iter(&txn)?.into_iter();
        tracing::info!("Starting blob reference scan");

        while let Some(Ok((key, value))) = blob_ref_it.next() {
            items_scanned += 1;

            if value == 0 {
                items_dead += 1;
                let digest = digest::Digest::try_from(key)
                    .map_err(|err| ManifestStoreError::DigestDecode(err))?;
                if sweep_fn(&digest).is_ok() {
                    items_swept += 1;
                }
            }
        }

        let stats = GCStats {
            items_scanned,
            items_dead,
            items_swept,
        };
        tracing::info!("Done with blob reference scan. Got stats: {:?}", stats);

        // TODO: Delete the actual blob references themselves that are 0, to free up
        // space.

        Ok(stats)
    }
}

async fn process_manifest_put<M: ManifestStore + Send + Sync + 'static>(
    repository: Repository,
    reference: String,
    content_type: String,
    manifest_store: Arc<M>,
    blob_locks: BlobLocks,
    body: Bytes,
) -> Result<Response<&'static str>, Rejection> {
    let location = format!("/v2/{}/manifests/{}", &repository.name, &reference);

    // Calculate the manifest digest.
    let mut sha256 = Sha256::new();
    sha256.update(&body);
    let digest = digest::Digest::new(
        digest::DigestAlgorithm::Sha256,
        format!("{:x}", sha256.finalize()),
    );
    let image_manifest = parse_manifest_json(&content_type, &body).map_err(|err| {
        tracing::debug!("Invalid manifest format: {:?}", err);
        ErrorResponse::new(
            StatusCode::BAD_REQUEST,
            ErrorCode::ManifestInvalid,
            format!("Invalid manifest format"),
        )
    })?;

    manifest_store
        .store_manifest(&digest, blob_locks, &image_manifest, content_type, body)
        .await?;
    manifest_store
        .tag_manifest(&repository, &reference, &digest)
        .await?;

    Ok(warp::http::response::Builder::new()
        .status(StatusCode::CREATED)
        .header("Location", location)
        .header("Docker-Content-Digest", format!("{}", digest))
        .body("")
        .unwrap())
}

async fn manifest_digest_for_tag<M>(
    repository: &Repository,
    tag: &str,
    manifest_store: &Arc<M>,
) -> Result<digest::Digest, Rejection>
where
    M: ManifestStore + Send + Sync + 'static,
{
    // TODO: Make this easily convert to a common rejection.
    let repository_tags = match manifest_store.get_repository_tags(repository).await {
        Ok(rt) => Ok(rt),
        Err(ManifestStoreError::NotFound) => Err(ErrorResponse::new(
            StatusCode::NOT_FOUND,
            ErrorCode::NameUnknown,
            format!("Failed to lookup repository tag for: {}", repository.name),
        )
        .into()),
        Err(err) => {
            tracing::error!("Error fetching tags: {:?}", err);
            Err(ErrorResponse::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorCode::Unknown,
                format!("Could not look up repository tags"),
            ))
        }
    }?;

    let manifest_ref = repository_tags
        .tag_references
        .iter()
        .find(|tr| tr.tag_name == tag);

    match manifest_ref {
        Some(tag_ref) => {
            let digest = match digest::Digest::try_from(&tag_ref.manifest_digest as &str) {
                Ok(d) => d,
                Err(err) => {
                    tracing::error!("Digest decode fail: {:?}", err);
                    return Err(ErrorResponse::new(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        ErrorCode::DigestInvalid,
                        format!("Failed to decode tag digest: {}", tag_ref.manifest_digest),
                    )
                    .into());
                }
            };
            Ok(digest)
        }
        None => Err(ErrorResponse::new(
            StatusCode::NOT_FOUND,
            ErrorCode::ManifestUnknown,
            format!("Could not find a manifest tagged with reference: {}", tag),
        )
        .into()),
    }
}

async fn process_manifest_get_or_head<M>(
    method: Method,
    repository: Repository,
    reference: String,
    manifest_store: Arc<M>,
) -> Result<Response<Vec<u8>>, Rejection>
where
    M: ManifestStore + Send + Sync + 'static,
{
    let digest = match digest::Digest::try_from(&reference as &str) {
        Ok(d) => d,
        Err(_err) => manifest_digest_for_tag(&repository, &reference, &manifest_store).await?,
    };
    let manifest = manifest_store.get_manifest(&digest).await?;

    let response = warp::http::response::Builder::new()
        .status(StatusCode::OK)
        .header("Content-Type", manifest.content_type)
        .header("Content-Length", manifest.raw_payload.len())
        .header("Docker-Content-Digest", format!("{}", digest));

    if method == Method::HEAD {
        // HEAD: Just send back headers
        Ok(response.body(Vec::new()).unwrap())
    } else {
        // GET request: Send the content payload as well.
        Ok(response.body(manifest.raw_payload).unwrap())
    }
}

async fn process_manifest_delete<M>(
    repository: Repository,
    reference: String,
    manifest_store: Arc<M>,
    blob_locks: BlobLocks,
) -> Result<Response<&'static str>, Rejection>
where
    M: ManifestStore + Send + Sync + 'static,
{
    let digest = match digest::Digest::try_from(&reference as &str) {
        Ok(d) => d,
        Err(_err) => manifest_digest_for_tag(&repository, &reference, &manifest_store).await?,
    };

    manifest_store.delete_manifest(&digest, blob_locks).await?;

    Ok(warp::http::response::Builder::new()
        .status(StatusCode::ACCEPTED)
        .body("")
        .unwrap())
}

fn manifest_put<M: ManifestStore + Send + Sync + 'static>(
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::put()
        .and(authorize_repository::<M>(Action::Write))
        .and(warp::path!("manifests" / String))
        .and(warp::header::<String>("Content-Type"))
        .and(warp::filters::ext::get::<Arc<M>>())
        .and(warp::filters::ext::get::<BlobLocks>())
        .and(warp::body::bytes())
        .and_then(process_manifest_put)
        .boxed()
}

fn manifest_get_or_head<M: ManifestStore + Send + Sync + 'static>(
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    (warp::get().or(warp::head()))
        .map(|_| ())
        .untuple_one() // Remove the unused method filter.
        .and(warp::method())
        .and(authorize_repository::<M>(Action::Read))
        .and(warp::path!("manifests" / String))
        .and(warp::filters::ext::get::<Arc<M>>())
        .and_then(process_manifest_get_or_head)
        .boxed()
}

fn manifest_delete<M: ManifestStore + Send + Sync + 'static>(
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::delete()
        .and(authorize_repository::<M>(Action::Write))
        .and(warp::path!("manifests" / String))
        .and(warp::filters::ext::get::<Arc<M>>())
        .and(warp::filters::ext::get::<BlobLocks>())
        .and_then(process_manifest_delete)
        .boxed()
}

pub fn routes<M: ManifestStore + Send + Sync + 'static>(
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    manifest_put::<M>()
        .or(manifest_get_or_head::<M>())
        .or(manifest_delete::<M>())
}

#[test]
fn test_deserializes_manifest_v2_from_json() {
    use self::protos;
    use std::fs::File;
    use std::path::PathBuf;

    let resource_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("resources/test");
    let v2_manifest = resource_dir.join("v2_manifest.json");
    let mut v2_manifest_file = File::open(v2_manifest).expect("Could not open v2 manifest file");
    let manifest: protos::ImageManifestV2 =
        serde_json::from_reader(&mut v2_manifest_file).expect("Could not deserialize v2 manifest");

    assert_eq!(2, manifest.schema_version);
    assert_eq!(
        "application/vnd.docker.distribution.manifest.v2+json",
        manifest.media_type
    );
    assert_eq!(21, manifest.layers.len());
    assert_eq!(
        "sha256:99046ad9247f8a1cbd1048d9099d026191ad9cda63c08aadeb704b7000a51717",
        manifest.layers[0].digest
    );
    assert_eq!(31361314, manifest.layers[0].size);
}
