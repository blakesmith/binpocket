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
    Lmdb(lmdb::Error),
}

impl From<lmdb::Error> for ManifestStoreError {
    fn from(err: lmdb::Error) -> Self {
        ManifestStoreError::Lmdb(err)
    }
}

/// Used to store and retrieve manifests. Manifests are keyed by
/// their content addressable digest.
#[async_trait]
pub trait ManifestStore {
    type ByteRef: AsRef<[u8]>;

    async fn store_manifest(
        &self,
        digest: &digest::Digest,
        json_payload: Self::ByteRef,
    ) -> Result<(), ManifestStoreError>;

    async fn get_manifest(
        &self,
        digest: &digest::Digest,
    ) -> Result<Self::ByteRef, ManifestStoreError>;
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
    type ByteRef = bytes::Bytes;

    async fn store_manifest(
        &self,
        digest: &digest::Digest,
        json_payload: Self::ByteRef,
    ) -> Result<(), ManifestStoreError> {
        let mut tx = self.env.begin_rw_txn()?;
        tx.put(
            self.db,
            &digest.get_bytes(),
            &json_payload,
            lmdb::WriteFlags::empty(),
        )
        .map_err(|err| err.into())
    }

    async fn get_manifest(
        &self,
        digest: &digest::Digest,
    ) -> Result<Self::ByteRef, ManifestStoreError> {
        let tx = self.env.begin_ro_txn()?;
        let buf = match tx.get(self.db, &digest.get_bytes()) {
            Ok(b) => Ok(b),
            Err(lmdb::Error::NotFound) => Err(ManifestStoreError::NotFound),
            Err(err) => Err(err.into()),
        }?;
        Ok(Bytes::copy_from_slice(buf))
    }
}

async fn process_manifest_put(
    repository: String,
    reference: String,
    body: bytes::Bytes,
) -> Result<Response<&'static str>, Rejection> {
    let location = format!("/v2/{}/manifests/{}", &repository, &reference);

    // Calculate the manifest digest.
    let mut sha256 = Sha256::new();
    sha256.update(body);
    let digest = digest::Digest::new(
        digest::DigestAlgorithm::Sha256,
        format!("{:x}", sha256.finalize()),
    );
    // TODO: Actually store the manifest

    Ok(warp::http::response::Builder::new()
        .status(StatusCode::CREATED)
        .header("Location", location)
        .header("Docker-Content-Digest", format!("{}", digest))
        .body("")
        .unwrap())
}

fn manifest_put() -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::put()
        .and(warp::path!("v2" / String / "manifests" / String))
        .and(warp::body::bytes())
        .and_then(process_manifest_put)
}

pub fn routes() -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    manifest_put()
}
