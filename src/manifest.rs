use async_trait::async_trait;
use bytes::Bytes;
use lmdb::Transaction;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use warp::{
    http::{Response, StatusCode},
    Filter, Rejection, Reply,
};

use crate::{digest, digest::deserialize_digest_string};

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
            tx.put(db, &digest_bytes, &json_payload, lmdb::WriteFlags::empty())
                .map_err(|err| err.into())
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
