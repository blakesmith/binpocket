use hyper::Server;
use std::net::SocketAddr;
use std::net::TcpListener;
use tower::{make::Shared, ServiceBuilder};
use tower_http::{
    compression::CompressionLayer,
    trace::{DefaultOnResponse, TraceLayer},
};
use tracing::metadata::Level;
use uuid::Uuid;
use warp::{Filter, Rejection, Reply};

fn blob_upload_start() -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::path!("v2" / String / "blobs" / "upload").map(|repository| {
        let upload_id = Uuid::new_v4();
        let location = format!(
            "/v2/{}/blobs/upload/{}",
            repository,
            upload_id.to_hyphenated_ref()
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

    let routes = blob_upload_start();
    let warp_service = warp::service(routes);

    let service = ServiceBuilder::new()
        .layer(TraceLayer::new_for_http().on_response(DefaultOnResponse::new().level(Level::INFO)))
        .layer(CompressionLayer::new())
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
