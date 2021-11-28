use async_trait::async_trait;
use bytes::Bytes;
use lmdb::Transaction;
use prost::Message;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use warp::{
    http::{Response, StatusCode},
    Filter, Rejection, Reply,
};

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
        ManifestStoreError::Lmdb(err)
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
    async fn store_manifest(
        &self,
        digest: &digest::Digest,
        json_payload: bytes::Bytes,
    ) -> Result<(), ManifestStoreError>;

    async fn get_manifest(
        &self,
        digest: &digest::Digest,
    ) -> Result<bytes::Bytes, ManifestStoreError>;

    async fn tag_manifest(
        &self,
        repository: &str,
        reference: &str,
        manifest_digest: &digest::Digest,
    ) -> Result<(), ManifestStoreError>;
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
        json_payload: bytes::Bytes,
    ) -> Result<(), ManifestStoreError> {
        let env = self.env.clone();
        let db = self.db.clone();
        let digest_bytes = digest.get_bytes();
        tokio::task::spawn_blocking(move || {
            let mut tx = env.begin_rw_txn()?;
            tx.put(db, &digest_bytes, &json_payload, lmdb::WriteFlags::empty())?;
            tx.commit().map_err(|err| err.into())
        })
        .await?
    }

    async fn get_manifest(
        &self,
        digest: &digest::Digest,
    ) -> Result<bytes::Bytes, ManifestStoreError> {
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
            Ok(Bytes::copy_from_slice(buf))
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
}

async fn process_manifest_put<M: ManifestStore + Send + Sync + 'static>(
    repository: String,
    reference: String,
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
    manifest_store.store_manifest(&digest, body).await?;
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

fn manifest_put<M: ManifestStore + Send + Sync + 'static>(
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::put()
        .and(warp::path!("v2" / String / "manifests" / String))
        .and(warp::filters::ext::get::<Arc<M>>())
        .and(warp::body::bytes())
        .and_then(process_manifest_put)
}

pub fn routes<M: ManifestStore + Send + Sync + 'static>(
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    manifest_put::<M>()
}
