mod api;
mod config;
mod datasets;
mod indexer;

use anyhow::Result;
use config::AppConfig;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .pretty()
        .init();

    let config = AppConfig::from_env()?;
    info!(
        data_dir = %config.data_dir.display(),
        index_dir = %config.index_dir.display(),
        bind_addr = %config.bind_addr,
        "loaded configuration"
    );

    let datasets = datasets::prepare_datasets(&config).await?;
    info!(file_count = datasets.len(), "datasets ready");

    let prepared_index = indexer::prepare_index(&config, &datasets).await?;
    let app_state = api::AppState::new(prepared_index);
    let app = api::router(app_state);

    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    info!(addr = %config.bind_addr, "starting http server");
    axum::serve(listener, app).await?;

    Ok(())
}
