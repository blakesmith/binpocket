mod auth;
mod binpocket;
mod blob;
mod digest;
mod error;
mod manifest;
mod range;
mod repository;

use binpocket::Config;
use clap::Parser;
use tracing::metadata::Level;

/// Run the binpocket server.
#[derive(Parser)]
#[clap(about, version, author)]
struct CmdOpts {
    /// Path to the YAML service configuration
    #[clap(short = 'c', long = "config", default_value = "binpocket.yaml")]
    config: String,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::fmt()
        .with_max_level(Level::DEBUG)
        .init();

    let opts = CmdOpts::parse();

    tracing::info!("Loading configuration from: {}", opts.config);
    let file = std::fs::File::open(&opts.config).expect("Could not open configuration file");
    let config: Config =
        serde_yaml::from_reader(file).expect("Could not deserialize configuration");
    binpocket::serve(&config)
        .await
        .expect("Could not serve binpocket");
}
