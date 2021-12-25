use async_trait::async_trait;
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
use crate::blob::BlobLocks;
use crate::digest;
use crate::error::{ErrorCode, ErrorResponse};
use crate::repository::{authorize_repository, Repository};

pub mod protos {
    use serde::Deserialize;

    include!(concat!(env!("OUT_DIR"), "/binpocket.manifest.rs"));
}

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
    json: &bytes::Bytes,
) -> Result<protos::ImageManifest, ImageManifestError> {
    match media_type {
        "application/vnd.docker.distribution.manifest.v2+json" => {
            let manifest_v2: protos::ImageManifestV2 = serde_json::from_slice(json)?;
            let manifest = protos::ImageManifest {
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

/// Used to store and retrieve manifests. Manifests are keyed by
/// their content addressable digest.
#[async_trait]
pub trait ManifestStore {
    /// Store a raw manifest, with the given digest as the
    /// key, the content_type of the payload, and the raw
    /// payload itself.
    async fn store_manifest(
        &self,
        digest: &digest::Digest,
        blob_locks: BlobLocks,
        manifest: &protos::ImageManifest,
        content_type: String,
        raw_manifest_payload: bytes::Bytes,
    ) -> Result<(), ManifestStoreError>;

    /// Lookup a manifest by its content addressable digest.
    async fn get_manifest(
        &self,
        digest: &digest::Digest,
    ) -> Result<protos::RawManifest, ManifestStoreError>;

    /// Add a tag / reference in the repository to an already stored manifest. The
    /// manifest must be already stored via 'store_manifest' in the given repository
    /// for this to work.
    async fn tag_manifest(
        &self,
        repository: &str,
        reference: &str,
        manifest_digest: &digest::Digest,
    ) -> Result<(), ManifestStoreError>;

    /// Fetch all tags for a given repository.
    async fn get_repository_tags(
        &self,
        repository: &str,
    ) -> Result<protos::RepositoryTags, ManifestStoreError>;
}

pub struct LmdbManifestStore {
    // Top level LMDB 'environment'. All sub databases below are part of
    // the same high-level 'database', and can all participate in transactions
    // together.
    env: heed::Env,

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
        let manifest_blob_references = env.create_database(Some("manifest_blob_references"))?;
        let raw_manifests = env.create_database(Some("raw_manifests"))?;
        let repository_tags = env.create_database(Some("repository_tags"))?;
        Ok(Self {
            env,
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
        // exclusive access: However, because we store blobs externally
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
    async fn store_manifest(
        &self,
        digest: &digest::Digest,
        blob_locks: BlobLocks,
        manifest: &protos::ImageManifest,
        content_type: String,
        raw_manifest_payload: bytes::Bytes,
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
            content_type: content_type,
            raw_payload: raw_manifest_payload.to_vec(),
        };
        let mut raw_manifest_buf = bytes::BytesMut::new();
        let mut manifest_buf = bytes::BytesMut::new();
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
            Ok(protos::RawManifest::decode(buf)?)
        })
        .await?
    }

    async fn tag_manifest(
        &self,
        repository: &str,
        reference: &str,
        manifest_digest: &digest::Digest,
    ) -> Result<(), ManifestStoreError> {
        let env = self.env.clone();
        let repository_tags = self.repository_tags.clone();
        let digest_str = format!("{}", manifest_digest);
        let repository_cloned = repository.to_string();
        let reference_cloned = reference.to_string();
        let key = repository.to_string();

        tokio::task::spawn_blocking(move || {
            let mut tx = env.write_txn()?;
            let new_tag = protos::TagReference {
                tag_name: reference_cloned,
                manifest_digest: digest_str,
            };

            let repo_tags = match repository_tags.get(&tx, key.as_bytes())? {
                Some(b) => {
                    let mut existing = protos::RepositoryTags::decode(b)?;
                    existing.tag_references.push(new_tag);
                    Ok::<_, ManifestStoreError>(existing)
                }
                None => Ok(protos::RepositoryTags {
                    repository: repository_cloned,
                    tag_references: vec![new_tag],
                }),
            }?;

            let mut value = bytes::BytesMut::new();
            repo_tags.encode(&mut value)?;
            repository_tags.put(&mut tx, key.as_bytes(), &value)?;
            tx.commit().map_err(|err| err.into())
        })
        .await?
    }

    async fn get_repository_tags(
        &self,
        repository: &str,
    ) -> Result<protos::RepositoryTags, ManifestStoreError> {
        let env = self.env.clone();
        let repository_tags = self.repository_tags.clone();
        let key = repository.to_string();

        tokio::task::spawn_blocking(move || {
            let tx = env.read_txn()?;
            let buf = repository_tags
                .get(&tx, key.as_bytes())?
                .ok_or(ManifestStoreError::NotFound)?;
            Ok(protos::RepositoryTags::decode(buf)?)
        })
        .await?
    }
}

async fn process_manifest_put<M: ManifestStore + Send + Sync + 'static>(
    repository: Repository,
    reference: String,
    content_type: String,
    manifest_store: Arc<M>,
    blob_locks: BlobLocks,
    body: bytes::Bytes,
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
        .tag_manifest(&repository.name, &reference, &digest)
        .await?;

    Ok(warp::http::response::Builder::new()
        .status(StatusCode::CREATED)
        .header("Location", location)
        .header("Docker-Content-Digest", format!("{}", digest))
        .body("")
        .unwrap())
}

async fn manifest_digest_for_tag<M>(
    repository: &str,
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
            format!("Failed to lookup repository tag for: {}", repository),
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
        Err(_err) => manifest_digest_for_tag(&repository.name, &reference, &manifest_store).await?,
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

fn manifest_put<M: ManifestStore + Send + Sync + 'static>(
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::put()
        .and(authorize_repository(Action::Write))
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
        .and(authorize_repository(Action::Read))
        .and(warp::path!("manifests" / String))
        .and(warp::filters::ext::get::<Arc<M>>())
        .and_then(process_manifest_get_or_head)
        .boxed()
}

pub fn routes<M: ManifestStore + Send + Sync + 'static>(
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    manifest_put::<M>().or(manifest_get_or_head::<M>())
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
