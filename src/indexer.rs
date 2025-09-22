use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use csv::ReaderBuilder;
use tantivy::query::QueryParser;
use tantivy::schema::{Field, NumericOptions, STORED, STRING, Schema, TEXT, TantivyDocument};
use tantivy::{Index, IndexReader, ReloadPolicy};
use tokio::fs;
use tokio::task;
use tracing::info;

use crate::config::AppConfig;
use crate::datasets::DatasetFile;

#[derive(Debug, Clone)]
pub struct IndexFields {
    pub tconst: Field,
    pub primary_title: Field,
    pub original_title: Field,
    pub title_type: Field,
    pub start_year: Field,
    pub genres: Field,
    pub average_rating: Field,
    pub num_votes: Field,
}

impl IndexFields {
    fn new(schema: &Schema) -> Result<Self> {
        Ok(Self {
            tconst: schema
                .get_field("tconst")
                .map_err(|_| anyhow!("missing field tconst"))?,
            primary_title: schema
                .get_field("primaryTitle")
                .map_err(|_| anyhow!("missing field primaryTitle"))?,
            original_title: schema
                .get_field("originalTitle")
                .map_err(|_| anyhow!("missing field originalTitle"))?,
            title_type: schema
                .get_field("titleType")
                .map_err(|_| anyhow!("missing field titleType"))?,
            start_year: schema
                .get_field("startYear")
                .map_err(|_| anyhow!("missing field startYear"))?,
            genres: schema
                .get_field("genres")
                .map_err(|_| anyhow!("missing field genres"))?,
            average_rating: schema
                .get_field("averageRating")
                .map_err(|_| anyhow!("missing field averageRating"))?,
            num_votes: schema
                .get_field("numVotes")
                .map_err(|_| anyhow!("missing field numVotes"))?,
        })
    }
}

pub struct PreparedIndex {
    pub fields: IndexFields,
    pub reader: IndexReader,
    pub query_parser: QueryParser,
}

pub async fn prepare_index(config: &AppConfig, datasets: &[DatasetFile]) -> Result<PreparedIndex> {
    let mut lookup = HashMap::new();
    for dataset in datasets {
        lookup.insert(dataset.name, dataset);
    }

    let basics = lookup
        .get("title.basics.tsv.gz")
        .ok_or_else(|| anyhow!("missing title.basics dataset"))?;
    let ratings = lookup
        .get("title.ratings.tsv.gz")
        .ok_or_else(|| anyhow!("missing title.ratings dataset"))?;

    if !index_exists(&config.index_dir) {
        fs::create_dir_all(&config.index_dir)
            .await
            .with_context(|| {
                format!("creating index directory at {}", config.index_dir.display())
            })?;

        build_index(
            &config.index_dir,
            basics.tsv_path.clone(),
            ratings.tsv_path.clone(),
        )
        .await?;
    }

    let index = Index::open_in_dir(&config.index_dir)
        .with_context(|| format!("opening index at {}", config.index_dir.display()))?;
    let schema = index.schema();
    let fields = IndexFields::new(&schema)?;
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::OnCommitWithDelay)
        .try_into()
        .context("constructing index reader")?;
    let query_parser = QueryParser::for_index(
        &index,
        vec![fields.primary_title, fields.original_title, fields.genres],
    );

    Ok(PreparedIndex {
        fields,
        reader,
        query_parser,
    })
}

fn index_exists(index_dir: &Path) -> bool {
    index_dir.join("meta.json").exists()
}

fn build_schema() -> Schema {
    let mut schema_builder = Schema::builder();

    schema_builder.add_text_field("tconst", STRING | STORED);
    schema_builder.add_text_field("titleType", STRING | STORED);
    schema_builder.add_text_field("primaryTitle", TEXT | STORED);
    schema_builder.add_text_field("originalTitle", TEXT | STORED);
    schema_builder.add_text_field("genres", TEXT | STORED);

    let numeric_options = NumericOptions::default()
        .set_indexed()
        .set_stored()
        .set_fast();

    schema_builder.add_i64_field("startYear", numeric_options.clone());
    schema_builder.add_f64_field("averageRating", numeric_options.clone());
    schema_builder.add_i64_field("numVotes", numeric_options);

    schema_builder.build()
}

async fn build_index(index_dir: &Path, basics_path: PathBuf, ratings_path: PathBuf) -> Result<()> {
    let index_dir = index_dir.to_path_buf();
    task::spawn_blocking(move || build_index_sync(&index_dir, &basics_path, &ratings_path))
        .await??;
    Ok(())
}

fn build_index_sync(index_dir: &Path, basics_path: &Path, ratings_path: &Path) -> Result<()> {
    if index_dir.exists() {
        std::fs::remove_dir_all(index_dir)
            .with_context(|| format!("clearing existing index at {}", index_dir.display()))?;
    }
    std::fs::create_dir_all(index_dir)
        .with_context(|| format!("creating index directory {}", index_dir.display()))?;

    let schema = build_schema();
    let index = Index::create_in_dir(index_dir, schema.clone())
        .with_context(|| format!("creating index in {}", index_dir.display()))?;

    let mut writer = index
        .writer::<TantivyDocument>(256 * 1024 * 1024)
        .context("creating tantivy writer")?;

    let ratings_map = load_ratings_map(ratings_path)?;
    info!(count = ratings_map.len(), "loaded ratings lookup");

    let fields = IndexFields::new(&schema)?;

    let mut reader = ReaderBuilder::new()
        .delimiter(b'\t')
        .has_headers(true)
        .from_path(basics_path)
        .with_context(|| format!("opening {}", basics_path.display()))?;

    let mut record_count = 0usize;

    for result in reader.records() {
        let record = result.with_context(|| format!("reading {}", basics_path.display()))?;
        if record.len() < 9 {
            continue;
        }

        let tconst = record.get(0).unwrap_or_default().to_string();
        let title_type = record.get(1).unwrap_or_default().to_string();
        let primary_title = record.get(2).unwrap_or_default().to_string();
        let original_title = record
            .get(3)
            .filter(|value| *value != "\\N" && !value.is_empty())
            .map(|value| value.to_string());
        let start_year = parse_i64(record.get(5));
        let genres: Vec<String> = record
            .get(8)
            .map(|value| {
                value
                    .split(',')
                    .filter(|s| *s != "\\N" && !s.is_empty())
                    .map(|s| s.to_string())
                    .collect()
            })
            .unwrap_or_default();

        let mut doc = TantivyDocument::default();
        doc.add_text(fields.tconst, &tconst);
        doc.add_text(fields.title_type, &title_type);
        doc.add_text(fields.primary_title, &primary_title);
        if let Some(original_title) = original_title.as_ref() {
            doc.add_text(fields.original_title, original_title);
        }
        for genre in genres {
            doc.add_text(fields.genres, genre);
        }
        if let Some(year) = start_year {
            doc.add_i64(fields.start_year, year);
        }
        if let Some((rating, votes)) = ratings_map.get(&tconst) {
            doc.add_f64(fields.average_rating, *rating);
            doc.add_i64(fields.num_votes, *votes);
        }

        writer
            .add_document(doc)
            .context("adding document to index")?;
        record_count += 1;

        if record_count.is_multiple_of(50_000) {
            info!(processed = record_count, "indexing progress");
        }
    }

    info!(processed = record_count, "committing index");
    writer.commit().context("committing tantivy index")?;
    Ok(())
}

fn load_ratings_map(path: &Path) -> Result<HashMap<String, (f64, i64)>> {
    let mut map = HashMap::new();
    let mut reader = ReaderBuilder::new()
        .delimiter(b'\t')
        .has_headers(true)
        .from_path(path)
        .with_context(|| format!("opening {}", path.display()))?;

    for result in reader.records() {
        let record = result.with_context(|| format!("reading {}", path.display()))?;
        if record.len() < 3 {
            continue;
        }
        let tconst = record[0].to_string();
        let rating = parse_f64(record.get(1));
        let votes = parse_i64(record.get(2));
        if let (Some(rating), Some(votes)) = (rating, votes) {
            map.insert(tconst, (rating, votes));
        }
    }

    Ok(map)
}

fn parse_i64(value: Option<&str>) -> Option<i64> {
    let value = value?;
    if value.is_empty() || value == "\\N" {
        return None;
    }
    value.parse().ok()
}

fn parse_f64(value: Option<&str>) -> Option<f64> {
    let value = value?;
    if value.is_empty() || value == "\\N" {
        return None;
    }
    value.parse().ok()
}
