mod auth;
mod binpocket;
mod blob;
mod digest;
mod error;
mod lock;
mod manifest;
mod range;
mod repository;
mod scanner;

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

    /// Tracing level. One of 'info', 'debug', or 'trace'
    #[clap(short = 't', long = "tracing-level", default_value = "info")]
    tracing: String,
}

#[tokio::main]
async fn main() {
    let opts = CmdOpts::parse();

    let tracing_level = match opts.tracing.as_ref() {
        "info" => Level::INFO,
        "debug" => Level::DEBUG,
        "trace" => Level::TRACE,
        _ => Level::INFO,
    };

    tracing_subscriber::fmt::fmt()
        .with_max_level(tracing_level)
        .init();

    let file = std::fs::File::open(&opts.config).expect("Could not open configuration file");
    let config: Config =
        serde_yaml::from_reader(file).expect("Could not deserialize configuration");
    binpocket::serve(&config)
        .await
        .expect("Could not serve binpocket");
}
