mod blob;
mod digest;
mod manifest;

use hyper::Server;
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

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::fmt()
        .with_max_level(Level::DEBUG)
        .init();

    let blob_store = Arc::new(
        blob::FsBlobStore::open(PathBuf::from("/tmp/blobs")).expect("Could not open blob store"),
    );

    let routes = blob::routes::<blob::FsBlobStore>();
    let warp_service = warp::service(routes);

    let service = ServiceBuilder::new()
        .layer(TraceLayer::new_for_http().on_response(DefaultOnResponse::new().level(Level::INFO)))
        .layer(CompressionLayer::new())
        .layer(AddExtensionLayer::new(blob_store))
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
