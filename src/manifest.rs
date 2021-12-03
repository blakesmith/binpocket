use async_trait::async_trait;
use lmdb::Transaction;
use prost::Message;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::convert::TryFrom;
use std::sync::Arc;
use warp::{
    http::{Method, Response, StatusCode},
    Filter, Rejection, Reply,
};

use crate::error::{ErrorCode, ErrorResponse};
use crate::{digest, digest::deserialize_digest_string};

pub mod protos {
    include!(concat!(env!("OUT_DIR"), "/binpocket.manifest.rs"));
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct Media {
    media_type: String,
    size: u64,

    #[serde(deserialize_with = "deserialize_digest_string")]
    digest: digest::Digest,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct ImageManifest {
    schema_version: u8,
    config: Media,
    layers: Vec<Media>,
}

#[derive(Debug)]
pub enum ManifestStoreError {
    ProstEncode(prost::EncodeError),
    ProstDecode(prost::DecodeError),
    NotFound,
    JoinError(tokio::task::JoinError),
    Lmdb(lmdb::Error),
}

impl warp::reject::Reject for ManifestStoreError {}

impl From<lmdb::Error> for ManifestStoreError {
    fn from(err: lmdb::Error) -> Self {
        match err {
            lmdb::Error::NotFound => ManifestStoreError::NotFound,
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
        content_type: String,
        raw_payload: bytes::Bytes,
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
    env: Arc<lmdb::Environment>,
    db: lmdb::Database,
}

impl LmdbManifestStore {
    pub fn open(env: Arc<lmdb::Environment>) -> Result<Self, ManifestStoreError> {
        let db = env.create_db(Some("manifests"), lmdb::DatabaseFlags::empty())?;
        Ok(Self { env, db })
    }
}

#[async_trait]
impl ManifestStore for LmdbManifestStore {
    async fn store_manifest(
        &self,
        digest: &digest::Digest,
        content_type: String,
        raw_payload: bytes::Bytes,
    ) -> Result<(), ManifestStoreError> {
        let env = self.env.clone();
        let db = self.db.clone();
        let digest_bytes = digest.get_bytes();
        let raw_manifest = protos::RawManifest {
            content_type: content_type,
            raw_payload: raw_payload.to_vec(),
        };
        let mut value = bytes::BytesMut::new();
        raw_manifest.encode(&mut value)?;
        tokio::task::spawn_blocking(move || {
            let mut tx = env.begin_rw_txn()?;
            tx.put(db, &digest_bytes, &value, lmdb::WriteFlags::empty())?;
            tx.commit().map_err(|err| err.into())
        })
        .await?
    }

    async fn get_manifest(
        &self,
        digest: &digest::Digest,
    ) -> Result<protos::RawManifest, ManifestStoreError> {
        let env = self.env.clone();
        let db = self.db.clone();
        let digest_bytes = digest.get_bytes();
        tokio::task::spawn_blocking(move || {
            let tx = env.begin_ro_txn()?;
            let buf = match tx.get(db, &digest_bytes) {
                Ok(b) => Ok(b),
                Err(lmdb::Error::NotFound) => Err(ManifestStoreError::NotFound),
                Err(err) => Err(err.into()),
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
        let db = self.db.clone();
        let digest_str = format!("{}", manifest_digest);
        let repository_cloned = repository.to_string();
        let reference_cloned = reference.to_string();
        let key = format!("{}_tags", repository);

        tokio::task::spawn_blocking(move || {
            let mut tx = env.begin_rw_txn()?;
            let new_tag = protos::TagReference {
                tag_name: reference_cloned,
                manifest_digest: digest_str,
            };

            let repo_tags = match tx.get(db, &key) {
                Ok(b) => {
                    let mut existing = protos::RepositoryTags::decode(b)?;
                    existing.tag_references.push(new_tag);
                    Ok(existing)
                }
                Err(lmdb::Error::NotFound) => Ok(protos::RepositoryTags {
                    repository: repository_cloned,
                    tag_references: vec![new_tag],
                }),
                Err(err) => Err(ManifestStoreError::from(err)),
            }?;

            let mut value = bytes::BytesMut::new();
            repo_tags.encode(&mut value)?;
            tx.put(db, &key, &value, lmdb::WriteFlags::empty())?;
            tx.commit().map_err(|err| err.into())
        })
        .await?
    }

    async fn get_repository_tags(
        &self,
        repository: &str,
    ) -> Result<protos::RepositoryTags, ManifestStoreError> {
        let env = self.env.clone();
        let db = self.db.clone();
        let key = format!("{}_tags", repository);

        tokio::task::spawn_blocking(move || {
            let tx = env.begin_ro_txn()?;
            let buf = tx.get(db, &key)?;
            Ok(protos::RepositoryTags::decode(buf)?)
        })
        .await?
    }
}

async fn process_manifest_put<M: ManifestStore + Send + Sync + 'static>(
    repository: String,
    reference: String,
    content_type: String,
    manifest_store: Arc<M>,
    body: bytes::Bytes,
) -> Result<Response<&'static str>, Rejection> {
    let location = format!("/v2/{}/manifests/{}", &repository, &reference);

    // Calculate the manifest digest.
    let mut sha256 = Sha256::new();
    sha256.update(&body);
    let digest = digest::Digest::new(
        digest::DigestAlgorithm::Sha256,
        format!("{:x}", sha256.finalize()),
    );
    // TODO: Validate the manifest content before storing
    // it.
    manifest_store
        .store_manifest(&digest, content_type, body)
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

async fn process_manifest_get_or_head<M, E>(
    _either: E, // Dumb that we need this, because of our 'head' or 'get' filter
    method: Method,
    repository: String,
    reference: String,
    manifest_store: Arc<M>,
) -> Result<Response<Vec<u8>>, Rejection>
where
    M: ManifestStore + Send + Sync + 'static,
    E: Sized,
{
    let digest = manifest_digest_for_tag(&repository, &reference, &manifest_store).await?;
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
        .and(warp::path!("v2" / String / "manifests" / String))
        .and(warp::header::<String>("Content-Type"))
        .and(warp::filters::ext::get::<Arc<M>>())
        .and(warp::body::bytes())
        .and_then(process_manifest_put)
}

fn manifest_get_or_head<M: ManifestStore + Send + Sync + 'static>(
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    (warp::get().or(warp::head()))
        .and(warp::method())
        .and(warp::path!("v2" / String / "manifests" / String))
        .and(warp::filters::ext::get::<Arc<M>>())
        .and_then(process_manifest_get_or_head)
}

pub fn routes<M: ManifestStore + Send + Sync + 'static>(
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    manifest_put::<M>().or(manifest_get_or_head::<M>())
}
