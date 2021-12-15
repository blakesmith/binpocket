mod auth;
mod binpocket;
mod blob;
mod digest;
mod error;
mod manifest;
mod range;
mod repository;

use binpocket::{Binpocket, Config};
use std::path::PathBuf;
use tracing::metadata::Level;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::fmt()
        .with_max_level(Level::DEBUG)
        .init();

    let config = Config {
        data_path: PathBuf::from("/tmp/binpocket"),
        jwt_issuer: "binpocket".to_string(),
        web_root: "http://127.0.0.1:3030".to_string(),
        listen_port: 3030,
    };
    Binpocket::serve(&config)
        .await
        .expect("Could not serve binpocket");
}
