use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use flate2::read::GzDecoder;
use futures_util::TryStreamExt;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::task;
use tracing::{debug, info, warn};

use crate::config::AppConfig;

/// Files listed in the IMDb non-commercial dataset.
pub const DATASET_FILES: &[&str] = &[
    "name.basics.tsv.gz",
    "title.akas.tsv.gz",
    "title.basics.tsv.gz",
    "title.crew.tsv.gz",
    "title.episode.tsv.gz",
    "title.principals.tsv.gz",
    "title.ratings.tsv.gz",
];

const IMDB_BASE_URL: &str = "https://datasets.imdbws.com";

#[derive(Debug, Clone)]
pub struct DatasetFile {
    pub name: &'static str,
    pub gz_path: PathBuf,
    pub tsv_path: PathBuf,
}

impl DatasetFile {
    fn new(data_dir: &Path, name: &'static str) -> Self {
        let gz_path = data_dir.join(name);
        let tsv_name = name.trim_end_matches(".gz");
        let tsv_path = data_dir.join(tsv_name);
        Self {
            name,
            gz_path,
            tsv_path,
        }
    }
}

/// Downloads and decompresses all IMDb datasets, returning the local file mapping.
pub async fn prepare_datasets(config: &AppConfig) -> Result<Vec<DatasetFile>> {
    fs::create_dir_all(&config.data_dir)
        .await
        .with_context(|| format!("creating data directory at {}", config.data_dir.display()))?;

    let mut files: Vec<DatasetFile> = Vec::new();
    for name in DATASET_FILES {
        files.push(DatasetFile::new(&config.data_dir, name));
    }

    download_missing_files(&files).await?;
    decompress_archives(&files).await?;

    Ok(files)
}

async fn download_missing_files(files: &[DatasetFile]) -> Result<()> {
    let client = reqwest::Client::new();
    for file in files {
        if file.gz_path.exists() {
            debug!(path = %file.gz_path.display(), "dataset already downloaded");
            continue;
        }

        if file.tsv_path.exists() {
            debug!(path = %file.tsv_path.display(), "dataset already prepared");
            continue;
        }

        let url = format!("{}/{}", IMDB_BASE_URL, file.name);
        info!(%url, path = %file.gz_path.display(), "downloading dataset");

        let resp = client
            .get(&url)
            .send()
            .await
            .with_context(|| format!("requesting {}", url))?;

        if !resp.status().is_success() {
            anyhow::bail!("failed to download {}: status {}", url, resp.status());
        }

        let mut stream = resp.bytes_stream();
        let mut tmp_path = file.gz_path.clone();
        tmp_path.set_extension("tmp-download");
        let mut dest = fs::File::create(&tmp_path)
            .await
            .with_context(|| format!("creating {}", tmp_path.display()))?;

        while let Some(chunk) = stream.try_next().await? {
            dest.write_all(&chunk).await?;
        }
        dest.flush().await?;
        drop(dest);

        fs::rename(&tmp_path, &file.gz_path)
            .await
            .with_context(|| {
                format!("moving download into place for {}", file.gz_path.display())
            })?;
    }
    Ok(())
}

async fn decompress_archives(files: &[DatasetFile]) -> Result<()> {
    for file in files {
        if !file.gz_path.exists() {
            if file.tsv_path.exists() {
                debug!(
                    gz = %file.gz_path.display(),
                    tsv = %file.tsv_path.display(),
                    "compressed archive already removed"
                );
            } else {
                warn!(
                    gz = %file.gz_path.display(),
                    tsv = %file.tsv_path.display(),
                    "missing compressed archive; skipping decompression"
                );
            }
            continue;
        }

        if file.tsv_path.exists() {
            let gz_meta = fs::metadata(&file.gz_path).await.ok();
            let tsv_meta = fs::metadata(&file.tsv_path).await.ok();
            if let (Some(gz), Some(tsv)) = (gz_meta, tsv_meta)
                && let (Ok(gz_time), Ok(tsv_time)) = (gz.modified(), tsv.modified())
                && gz_time <= tsv_time
            {
                debug!(path = %file.tsv_path.display(), "decompression up to date");
                if let Err(err) = fs::remove_file(&file.gz_path).await {
                    warn!(
                        path = %file.gz_path.display(),
                        error = %err,
                        "failed to remove compressed archive"
                    );
                }
                continue;
            }
        }

        let gz_path = file.gz_path.clone();
        let tsv_path = file.tsv_path.clone();
        info!(
            gz = %gz_path.display(),
            tsv = %tsv_path.display(),
            "decompressing dataset"
        );

        task::spawn_blocking(move || decompress_sync(&gz_path, &tsv_path))
            .await
            .context("joining decompression task")??;

        if let Err(err) = fs::remove_file(&file.gz_path).await {
            warn!(
                path = %file.gz_path.display(),
                error = %err,
                "failed to remove compressed archive after decompression"
            );
        } else {
            debug!(path = %file.gz_path.display(), "removed compressed archive");
        }
    }
    Ok(())
}

fn decompress_sync(gz_path: &Path, tsv_path: &Path) -> Result<()> {
    let input =
        File::open(gz_path).with_context(|| format!("opening archive {}", gz_path.display()))?;
    let reader = BufReader::new(input);
    let mut decoder = GzDecoder::new(reader);

    let output = File::create(tsv_path)
        .with_context(|| format!("creating decompressed file {}", tsv_path.display()))?;
    let mut writer = BufWriter::new(output);

    std::io::copy(&mut decoder, &mut writer)
        .with_context(|| format!("decompressing {}", gz_path.display()))?;
    writer.flush()?;
    Ok(())
}
