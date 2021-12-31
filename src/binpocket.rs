use heed::EnvOpenOptions;
use hyper::Server;
use jwt_simple::algorithms::ES256KeyPair;
use manifest::LmdbManifestStore;
use serde::Deserialize;
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
    self, credential::JWTTokenGenerator, principal::User, resource::Scope, Authenticator,
    Authorizer, FixedPrincipalAuthenticator,
};
use crate::{
    blob::{self, FsBlobStore, LockingBlobStore},
    error, manifest,
};

#[derive(Deserialize)]
pub struct UserConfig {
    username: String,
    password: String,
    global_scopes: Vec<Scope>,
}

#[derive(Deserialize)]
pub struct Config {
    pub data_path: PathBuf,
    pub jwt_issuer: String,
    pub web_root: String,
    pub listen_port: u16,
    pub users: Vec<UserConfig>,
}

#[derive(Debug)]
pub enum BinpocketError {
    Io(std::io::Error),
    Lmdb(heed::Error),
    Hyper(hyper::Error),
    Manifest(manifest::ManifestStoreError),
}

impl From<hyper::Error> for BinpocketError {
    fn from(err: hyper::Error) -> Self {
        Self::Hyper(err)
    }
}

impl From<std::io::Error> for BinpocketError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

impl From<manifest::ManifestStoreError> for BinpocketError {
    fn from(err: manifest::ManifestStoreError) -> Self {
        Self::Manifest(err)
    }
}

impl From<heed::Error> for BinpocketError {
    fn from(err: heed::Error) -> Self {
        Self::Lmdb(err)
    }
}

fn version_root(auth_url: &str) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let realm = auth_url.to_string();
    warp::get()
        .and(warp::path::end())
        .and(auth::authenticate())
        .map(move |user| match user {
            None => warp::http::response::Builder::new()
                .status(StatusCode::UNAUTHORIZED)
                .header("WWW-Authenticate", format!("Bearer realm=\"{}\"", realm))
                .body("")
                .unwrap(),
            Some(_) => warp::http::response::Builder::new()
                .status(StatusCode::OK)
                .body("")
                .unwrap(),
        })
        .boxed()
}

pub async fn serve(config: &Config) -> Result<(), BinpocketError> {
    let blob_path = config.data_path.join("oci");
    let blob_store = Arc::new(LockingBlobStore::new(FsBlobStore::open(blob_path)?));

    let lmdb_path = config.data_path.join("lmdb");
    tokio::fs::create_dir_all(&lmdb_path).await?;
    let env = EnvOpenOptions::new().max_dbs(50).open(lmdb_path)?;
    let manifest_store = Arc::new(LmdbManifestStore::open(env)?);

    let key_pair = ES256KeyPair::generate(); // TODO: Save it somewhere stable
    let jwt_generator = Arc::new(JWTTokenGenerator::new(key_pair, &config.jwt_issuer));

    let mut fixed_authenticator = FixedPrincipalAuthenticator::new(jwt_generator.public_key());
    for user_config in &config.users {
        fixed_authenticator.add_user(
            User {
                name: user_config.username.clone(),
                global_scopes: user_config.global_scopes.clone(),
            },
            &user_config.password,
        );
    }
    let authenticator: Arc<Box<dyn Authenticator>> = Arc::new(Box::new(fixed_authenticator));

    let auth_url = format!("{}/token", config.web_root);
    let authorizer = Arc::new(Authorizer {
        auth_url: auth_url.to_string(),
    });

    let routes = auth::routes()
        .or(warp::path("v2").and(
            (version_root(&auth_url))
                .or(blob::routes::<
                    LockingBlobStore<FsBlobStore>,
                    manifest::LmdbManifestStore,
                >())
                .or(manifest::routes::<manifest::LmdbManifestStore>()),
        ))
        .recover(error::handle_rejection);

    let warp_service = warp::service(routes);

    let service = ServiceBuilder::new()
        .layer(TraceLayer::new_for_http().on_response(DefaultOnResponse::new().level(Level::INFO)))
        .layer(CompressionLayer::new())
        .layer(AddExtensionLayer::new(blob_store.locks()))
        .layer(AddExtensionLayer::new(blob_store))
        .layer(AddExtensionLayer::new(manifest_store))
        .layer(AddExtensionLayer::new(authenticator))
        .layer(AddExtensionLayer::new(authorizer))
        .layer(AddExtensionLayer::new(jwt_generator))
        .service(warp_service);
    let addr = SocketAddr::from(([0, 0, 0, 0], config.listen_port));
    let listener = TcpListener::bind(addr).unwrap();

    tracing::info!("Listening on port: {}", config.listen_port);

    Server::from_tcp(listener)
        .unwrap()
        .serve(Shared::new(service))
        .await
        .map_err(|err| err.into())
}
