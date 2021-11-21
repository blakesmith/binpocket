mod digest;

use async_std::io::Write;
use digest::{deserialize_digest_string, Digest};
use hyper::Server;
use serde::Deserialize;
use std::{
    collections::HashMap,
    net::{SocketAddr, TcpListener},
    sync::{Arc, RwLock},
};
use tower::{make::Shared, ServiceBuilder};
use tower_http::{
    add_extension::AddExtensionLayer,
    compression::CompressionLayer,
    trace::{DefaultOnResponse, TraceLayer},
};
use tracing::metadata::Level;
use uuid::Uuid;
use warp::{Filter, Rejection, Reply};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct Media {
    media_type: String,
    size: u64,

    #[serde(deserialize_with = "deserialize_digest_string")]
    digest: Digest,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct ImageManifest {
    schema_version: u8,
    config: Media,
    layers: Vec<Media>,
}

enum BlobStoreError {}

/// Used to store blobs. Blobs are keyed by an upload session identifier, until
/// they are finalized. At finalization time, a digest is computed, and used as
/// the primary identifier for the blob going forward. A blob effectively becomes
/// immutable once the upload has been completed. Callers must either 'finalize'
/// a blob upload, or cancel it.
trait BlobStore {
    type Writer: Write;

    /// Begin uploading a blob, using the UploadSession#id as the key.
    fn start_upload(
        &self,
        session: &UploadSession,
        start_pos: u64,
    ) -> Result<Self::Writer, BlobStoreError>;

    /// Finalize the upload session, producing an immutable Digest that will
    /// be used as the blob identifier going forward.
    fn finalize_upload(&self, session: &UploadSession) -> Result<(), BlobStoreError>;

    /// Cancel the upload, removing all state about the upload.
    fn cancel_upload(&self, session: &UploadSession) -> Result<(), BlobStoreError>;
}

#[derive(Clone)]
struct State {
    uploads: UploadSessionManager,
}

struct UploadSession {
    id: Uuid,
}

impl UploadSession {
    fn new() -> Self {
        Self { id: Uuid::new_v4() }
    }
}

#[derive(Clone)]
struct UploadSessionManager {
    active_sessions: Arc<RwLock<HashMap<Uuid, UploadSession>>>,
}

impl UploadSessionManager {
    fn new() -> Self {
        Self {
            active_sessions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn start_session(&self) -> Uuid {
        let session = UploadSession::new();
        let id = session.id.clone();
        self.active_sessions
            .write()
            .unwrap()
            .insert(id.clone(), session);
        id
    }
}

fn blob_upload_start() -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::path!("v2" / String / "blobs" / "upload")
        .and(warp::filters::ext::get::<State>())
        .map(|repository, state: State| {
            let session_id = state.uploads.start_session();
            let location = format!(
                "/v2/{}/blobs/upload/{}",
                repository,
                session_id.to_hyphenated_ref()
            );
            warp::http::response::Builder::new()
                .header("Location", location)
                .status(202)
                .body(format!("Start blob upload for: {}", repository))
        })
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let state = State {
        uploads: UploadSessionManager::new(),
    };

    let routes = blob_upload_start();
    let warp_service = warp::service(routes);

    let service = ServiceBuilder::new()
        .layer(TraceLayer::new_for_http().on_response(DefaultOnResponse::new().level(Level::INFO)))
        .layer(CompressionLayer::new())
        .layer(AddExtensionLayer::new(state))
        .service(warp_service);
    let addr = SocketAddr::from(([0, 0, 0, 0], 3030));
    let listener = TcpListener::bind(addr).unwrap();

    tracing::info!("Listening on port: {}", 3030);

    Server::from_tcp(listener)
        .unwrap()
        .serve(Shared::new(service))
        .await
        .expect("Could not listen on server");
}
