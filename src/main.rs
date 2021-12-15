mod auth;
mod binpocket;
mod blob;
mod digest;
mod error;
mod manifest;
mod range;
mod repository;

use binpocket::{Binpocket, Config};
use tracing::metadata::Level;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::fmt()
        .with_max_level(Level::DEBUG)
        .init();

    let config = Config {};
    Binpocket::serve(&config)
        .await
        .expect("Could not serve binpocket");
}
