use anyhow::Result;
use clap::Parser;
use pgcache_consistency::cli::Cli;
use pgcache_consistency::runner;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    runner::run(cli).await
}
