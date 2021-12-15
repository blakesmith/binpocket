use hyper::Server;
use jwt_simple::algorithms::ES256KeyPair;
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
    self, credential::JWTTokenGenerator, principal::User, Authenticator, Authorizer,
    FixedPrincipalAuthenticator,
};
use crate::{blob, error, manifest};

pub struct Config {}

pub struct Binpocket {}

#[derive(Debug)]
pub enum BinpocketError {
    Hyper(hyper::Error),
}

impl From<hyper::Error> for BinpocketError {
    fn from(err: hyper::Error) -> Self {
        Self::Hyper(err)
    }
}

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

impl Binpocket {
    pub async fn serve(config: &Config) -> Result<(), BinpocketError> {
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

        let key_pair = ES256KeyPair::generate(); // TODO: Save it somewhere stable
        let jwt_generator = Arc::new(JWTTokenGenerator::new(key_pair, "binpocket"));

        let mut fixed_authenticator = FixedPrincipalAuthenticator::new(jwt_generator.public_key());
        fixed_authenticator.add_user(
            User {
                name: "fixed".to_string(),
            },
            "a_global_test_token",
        );
        let authenticator: Arc<Box<dyn Authenticator>> = Arc::new(Box::new(fixed_authenticator));

        let auth_url = "http://127.0.0.1:3030/token";
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
            .layer(
                TraceLayer::new_for_http().on_response(DefaultOnResponse::new().level(Level::INFO)),
            )
            .layer(CompressionLayer::new())
            .layer(AddExtensionLayer::new(blob_store))
            .layer(AddExtensionLayer::new(manifest_store))
            .layer(AddExtensionLayer::new(authenticator))
            .layer(AddExtensionLayer::new(authorizer))
            .layer(AddExtensionLayer::new(jwt_generator))
            .service(warp_service);
        let addr = SocketAddr::from(([0, 0, 0, 0], 3030));
        let listener = TcpListener::bind(addr).unwrap();

        tracing::info!("Listening on port: {}", 3030);

        Server::from_tcp(listener)
            .unwrap()
            .serve(Shared::new(service))
            .await
            .map_err(|err| err.into())
    }
}
