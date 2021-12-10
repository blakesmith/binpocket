mod auth;
mod blob;
mod digest;
mod error;
mod manifest;
mod range;
mod repository;

use hyper::Server;
use manifest::LmdbManifestStore;
use std::{
    net::{SocketAddr, TcpListener},
    path::PathBuf,
    sync::Arc,
};
use tower::{make::Shared, ServiceBuilder};
use tower_http::{
    add_extension::AddExtensionLayer,
    compression::CompressionLayer,
    trace::{DefaultOnResponse, TraceLayer},
};
use tracing::metadata::Level;
use warp::{http::StatusCode, Filter, Rejection, Reply};

use crate::auth::{
    principal::{Principal, User},
    Authenticator, Authorizer, FixedBearerTokenAuthenticator,
};

fn version_root(auth_url: &str) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let realm = auth_url.to_string();
    warp::get()
        .and(warp::path::end())
        .map(move || {
            warp::http::response::Builder::new()
                .status(StatusCode::UNAUTHORIZED)
                .header("WWW-Authenticate", format!("Bearer realm=\"{}\"", realm))
                .body("")
                .unwrap()
        })
        .boxed()
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::fmt()
        .with_max_level(Level::DEBUG)
        .init();

    let blob_store = Arc::new(
        blob::FsBlobStore::open(PathBuf::from("/tmp/oci")).expect("Could not open blob store"),
    );

    let env = Arc::new(
        lmdb::Environment::new()
            .set_max_dbs(5)
            .open(&PathBuf::from("/tmp/lmdb_data"))
            .expect("Could not open LMDB database"),
    );
    let manifest_store =
        Arc::new(LmdbManifestStore::open(env).expect("Could not open manifest LMDB database"));

    let authenticator: Arc<Box<dyn Authenticator>> =
        Arc::new(Box::new(FixedBearerTokenAuthenticator {
            token: "a_global_test_token".to_string(),
            principal: Principal::User(User {
                name: "global_admin".to_string(),
            }),
        }));

    let auth_url = "http://127.0.0.1:3030/authorize";
    let authorizer = Arc::new(Authorizer {
        auth_url: auth_url.to_string(),
    });

    let routes = auth::routes()
        .or(warp::path("v2").and(
            (version_root(auth_url))
                .or(blob::routes::<blob::FsBlobStore>())
                .or(manifest::routes::<manifest::LmdbManifestStore>()),
        ))
        .recover(error::handle_rejection);

    let warp_service = warp::service(routes);

    let service = ServiceBuilder::new()
        .layer(TraceLayer::new_for_http().on_response(DefaultOnResponse::new().level(Level::INFO)))
        .layer(CompressionLayer::new())
        .layer(AddExtensionLayer::new(blob_store))
        .layer(AddExtensionLayer::new(manifest_store))
        .layer(AddExtensionLayer::new(authenticator))
        .layer(AddExtensionLayer::new(authorizer))
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
