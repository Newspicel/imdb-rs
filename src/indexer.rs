use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

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

const TITLE_INDEX_SUBDIR: &str = "titles";
const NAME_INDEX_SUBDIR: &str = "names";

#[derive(Debug, Clone)]
pub struct TitleFields {
    pub tconst: Field,
    pub primary_title: Field,
    pub original_title: Field,
    pub title_type: Field,
    pub start_year: Field,
    pub genres: Field,
    pub average_rating: Field,
    pub num_votes: Field,
    pub search_titles: Field,
}

impl TitleFields {
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
            search_titles: schema
                .get_field("searchTitles")
                .map_err(|_| anyhow!("missing field searchTitles"))?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct NameFields {
    pub nconst: Field,
    pub primary_name: Field,
    pub primary_name_search: Field,
    pub birth_year: Field,
    pub death_year: Field,
    pub primary_profession: Field,
    pub known_for_titles: Field,
}

impl NameFields {
    fn new(schema: &Schema) -> Result<Self> {
        Ok(Self {
            nconst: schema
                .get_field("nconst")
                .map_err(|_| anyhow!("missing field nconst"))?,
            primary_name: schema
                .get_field("primaryName")
                .map_err(|_| anyhow!("missing field primaryName"))?,
            primary_name_search: schema
                .get_field("primaryNameSearch")
                .map_err(|_| anyhow!("missing field primaryNameSearch"))?,
            birth_year: schema
                .get_field("birthYear")
                .map_err(|_| anyhow!("missing field birthYear"))?,
            death_year: schema
                .get_field("deathYear")
                .map_err(|_| anyhow!("missing field deathYear"))?,
            primary_profession: schema
                .get_field("primaryProfession")
                .map_err(|_| anyhow!("missing field primaryProfession"))?,
            known_for_titles: schema
                .get_field("knownForTitles")
                .map_err(|_| anyhow!("missing field knownForTitles"))?,
        })
    }
}

#[derive(Clone)]
pub struct TitleIndex {
    pub schema: Schema,
    pub fields: TitleFields,
    pub reader: IndexReader,
    pub query_parser: QueryParser,
}

#[derive(Clone)]
pub struct NameIndex {
    pub fields: NameFields,
    pub reader: IndexReader,
    pub query_parser: QueryParser,
}

#[derive(Clone)]
pub struct PreparedIndexes {
    pub titles: TitleIndex,
    pub names: NameIndex,
}

pub async fn prepare_indexes(
    config: &AppConfig,
    datasets: &[DatasetFile],
) -> Result<PreparedIndexes> {
    let dataset_lookup: HashMap<&str, &DatasetFile> = datasets
        .iter()
        .map(|dataset| (dataset.name, dataset))
        .collect();

    let basics = dataset_lookup
        .get("title.basics.tsv.gz")
        .ok_or_else(|| anyhow!("missing title.basics dataset"))?;
    let ratings = dataset_lookup
        .get("title.ratings.tsv.gz")
        .ok_or_else(|| anyhow!("missing title.ratings dataset"))?;
    let akas = dataset_lookup
        .get("title.akas.tsv.gz")
        .ok_or_else(|| anyhow!("missing title.akas dataset"))?;
    let names = dataset_lookup
        .get("name.basics.tsv.gz")
        .ok_or_else(|| anyhow!("missing name.basics dataset"))?;
    let principals = dataset_lookup
        .get("title.principals.tsv.gz")
        .ok_or_else(|| anyhow!("missing title.principals dataset"))?;

    fs::create_dir_all(&config.index_dir)
        .await
        .with_context(|| format!("creating index root at {}", config.index_dir.display()))?;

    let title_index_dir = config.index_dir.join(TITLE_INDEX_SUBDIR);
    let name_index_dir = config.index_dir.join(NAME_INDEX_SUBDIR);

    let name_lookup = Arc::new(load_name_map(&names.tsv_path)?);
    let principals_map = Arc::new(load_principals_map(&principals.tsv_path, &name_lookup)?);

    let title_index = prepare_title_index(
        &title_index_dir,
        basics.tsv_path.clone(),
        ratings.tsv_path.clone(),
        akas.tsv_path.clone(),
        Arc::clone(&principals_map),
    )
    .await?;

    let name_index = prepare_name_index(&name_index_dir, names.tsv_path.clone()).await?;

    Ok(PreparedIndexes {
        titles: title_index,
        names: name_index,
    })
}

async fn prepare_title_index(
    index_dir: &Path,
    basics_path: PathBuf,
    ratings_path: PathBuf,
    akas_path: PathBuf,
    principals_map: Arc<HashMap<String, Vec<String>>>,
) -> Result<TitleIndex> {
    if !index_exists(index_dir) {
        build_title_index(
            index_dir,
            basics_path.clone(),
            ratings_path.clone(),
            akas_path.clone(),
            Arc::clone(&principals_map),
        )
        .await?;
    }

    let index = Index::open_in_dir(index_dir)
        .with_context(|| format!("opening title index at {}", index_dir.display()))?;
    let schema = index.schema();
    let fields = TitleFields::new(&schema)?;
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::OnCommitWithDelay)
        .try_into()
        .context("constructing title index reader")?;
    let mut query_parser = QueryParser::for_index(
        &index,
        vec![
            fields.primary_title,
            fields.original_title,
            fields.search_titles,
            fields.genres,
        ],
    );
    query_parser.set_field_boost(fields.primary_title, 2.0);
    query_parser.set_field_boost(fields.original_title, 1.2);
    query_parser.set_field_boost(fields.search_titles, 1.0);
    query_parser.set_field_boost(fields.genres, 0.3);
    query_parser.set_field_fuzzy(fields.primary_title, false, 1, true);
    query_parser.set_field_fuzzy(fields.original_title, false, 1, true);
    query_parser.set_field_fuzzy(fields.search_titles, false, 1, true);

    Ok(TitleIndex {
        schema,
        fields,
        reader,
        query_parser,
    })
}

async fn prepare_name_index(index_dir: &Path, names_path: PathBuf) -> Result<NameIndex> {
    if !index_exists(index_dir) {
        build_name_index(index_dir, names_path.clone()).await?;
    }

    let index = Index::open_in_dir(index_dir)
        .with_context(|| format!("opening name index at {}", index_dir.display()))?;
    let schema = index.schema();
    let fields = NameFields::new(&schema)?;
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::OnCommitWithDelay)
        .try_into()
        .context("constructing name index reader")?;
    let mut query_parser = QueryParser::for_index(
        &index,
        vec![fields.primary_name_search, fields.primary_profession],
    );
    query_parser.set_field_boost(fields.primary_name_search, 1.5);
    query_parser.set_field_fuzzy(fields.primary_name_search, false, 1, true);
    query_parser.set_field_fuzzy(fields.primary_profession, false, 1, true);

    Ok(NameIndex {
        fields,
        reader,
        query_parser,
    })
}

fn index_exists(index_dir: &Path) -> bool {
    index_dir.join("meta.json").exists()
}

fn build_title_schema() -> Schema {
    let mut schema_builder = Schema::builder();

    schema_builder.add_text_field("tconst", STRING | STORED);
    schema_builder.add_text_field("titleType", STRING | STORED);
    schema_builder.add_text_field("primaryTitle", TEXT | STORED);
    schema_builder.add_text_field("originalTitle", TEXT | STORED);
    schema_builder.add_text_field("genres", TEXT | STORED);
    schema_builder.add_text_field("searchTitles", TEXT);

    let numeric_options = NumericOptions::default()
        .set_indexed()
        .set_stored()
        .set_fast();

    schema_builder.add_i64_field("startYear", numeric_options.clone());
    schema_builder.add_f64_field("averageRating", numeric_options.clone());
    schema_builder.add_i64_field("numVotes", numeric_options);

    schema_builder.build()
}

fn build_name_schema() -> Schema {
    let mut schema_builder = Schema::builder();

    schema_builder.add_text_field("nconst", STRING | STORED);
    schema_builder.add_text_field("primaryName", TEXT | STORED);
    schema_builder.add_text_field("primaryNameSearch", TEXT);
    schema_builder.add_text_field("primaryProfession", TEXT | STORED);
    schema_builder.add_text_field("knownForTitles", TEXT | STORED);

    let numeric_options = NumericOptions::default()
        .set_indexed()
        .set_stored()
        .set_fast();

    schema_builder.add_i64_field("birthYear", numeric_options.clone());
    schema_builder.add_i64_field("deathYear", numeric_options);

    schema_builder.build()
}

async fn build_title_index(
    index_dir: &Path,
    basics_path: PathBuf,
    ratings_path: PathBuf,
    akas_path: PathBuf,
    principals_map: Arc<HashMap<String, Vec<String>>>,
) -> Result<()> {
    let index_dir = index_dir.to_path_buf();
    task::spawn_blocking(move || {
        build_title_index_sync(
            &index_dir,
            &basics_path,
            &ratings_path,
            &akas_path,
            &principals_map,
        )
    })
    .await??;
    Ok(())
}

fn build_title_index_sync(
    index_dir: &Path,
    basics_path: &Path,
    ratings_path: &Path,
    akas_path: &Path,
    principals_map: &HashMap<String, Vec<String>>,
) -> Result<()> {
    if index_dir.exists() {
        std::fs::remove_dir_all(index_dir)
            .with_context(|| format!("clearing existing index at {}", index_dir.display()))?;
    }
    std::fs::create_dir_all(index_dir)
        .with_context(|| format!("creating index directory {}", index_dir.display()))?;

    let schema = build_title_schema();
    let index = Index::create_in_dir(index_dir, schema.clone())
        .with_context(|| format!("creating title index in {}", index_dir.display()))?;

    let mut writer = index
        .writer::<TantivyDocument>(256 * 1024 * 1024)
        .context("creating title index writer")?;

    let ratings_map = load_ratings_map(ratings_path)?;
    info!(count = ratings_map.len(), "loaded ratings lookup");

    let aka_map = load_aka_map(akas_path)?;
    info!(count = aka_map.len(), "loaded aka titles");

    let fields = TitleFields::new(&schema)?;

    let mut reader = ReaderBuilder::new()
        .delimiter(b'\t')
        .has_headers(true)
        .flexible(true)
        .from_path(basics_path)
        .with_context(|| format!("opening {}", basics_path.display()))?;

    let mut record_count = 0usize;

    for result in reader.records() {
        let record = result.with_context(|| format!("reading {}", basics_path.display()))?;

        let Some(tconst_raw) = record.get(0) else {
            continue;
        };
        if tconst_raw.is_empty() || tconst_raw == "\\N" {
            continue;
        }
        let tconst = tconst_raw.to_string();

        let title_type = record.get(1).unwrap_or_default().to_string();

        let Some(primary_title_raw) = record.get(2) else {
            continue;
        };
        let primary_title = primary_title_raw.to_string();

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
        doc.add_text(fields.search_titles, &primary_title);
        if let Some(original_title) = original_title.as_ref() {
            doc.add_text(fields.original_title, original_title);
            doc.add_text(fields.search_titles, original_title);
        }

        if let Some(aka_titles) = aka_map.get(&tconst) {
            let mut seen = HashSet::new();
            seen.insert(primary_title.clone());
            if let Some(original_title) = original_title.as_ref() {
                seen.insert(original_title.clone());
            }
            for aka in aka_titles {
                if seen.insert(aka.clone()) {
                    doc.add_text(fields.search_titles, aka);
                }
            }
        }

        if let Some(names) = principals_map.get(&tconst) {
            for name in names {
                doc.add_text(fields.search_titles, name);
            }
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
            .context("adding document to title index")?;
        record_count += 1;

        if record_count.is_multiple_of(50_000) {
            info!(processed = record_count, "title indexing progress");
        }
    }

    info!(processed = record_count, "committing title index");
    writer.commit().context("committing title index")?;
    Ok(())
}

async fn build_name_index(index_dir: &Path, names_path: PathBuf) -> Result<()> {
    let index_dir = index_dir.to_path_buf();
    task::spawn_blocking(move || build_name_index_sync(&index_dir, &names_path)).await??;
    Ok(())
}

fn build_name_index_sync(index_dir: &Path, names_path: &Path) -> Result<()> {
    if index_dir.exists() {
        std::fs::remove_dir_all(index_dir)
            .with_context(|| format!("clearing existing index at {}", index_dir.display()))?;
    }
    std::fs::create_dir_all(index_dir)
        .with_context(|| format!("creating index directory {}", index_dir.display()))?;

    let schema = build_name_schema();
    let index = Index::create_in_dir(index_dir, schema.clone())
        .with_context(|| format!("creating name index in {}", index_dir.display()))?;

    let mut writer = index
        .writer::<TantivyDocument>(128 * 1024 * 1024)
        .context("creating name index writer")?;

    let fields = NameFields::new(&schema)?;

    let mut reader = ReaderBuilder::new()
        .delimiter(b'\t')
        .has_headers(true)
        .flexible(true)
        .from_path(names_path)
        .with_context(|| format!("opening {}", names_path.display()))?;

    let mut record_count = 0usize;

    for result in reader.records() {
        let record = result.with_context(|| format!("reading {}", names_path.display()))?;

        let Some(nconst_raw) = record.get(0) else {
            continue;
        };
        if nconst_raw.is_empty() || nconst_raw == "\\N" {
            continue;
        }
        let nconst = nconst_raw.to_string();

        let primary_name = record.get(1).unwrap_or_default().to_string();
        if primary_name.is_empty() {
            continue;
        }

        let birth_year = parse_i64(record.get(2));
        let death_year = parse_i64(record.get(3));
        let primary_profession = record.get(4).unwrap_or_default().to_string();
        let known_for_titles = record.get(5).unwrap_or_default().to_string();

        let mut doc = TantivyDocument::default();
        doc.add_text(fields.nconst, &nconst);
        doc.add_text(fields.primary_name, &primary_name);
        doc.add_text(fields.primary_name_search, &primary_name);
        if !primary_profession.is_empty() {
            doc.add_text(fields.primary_profession, &primary_profession);
            doc.add_text(fields.primary_name_search, &primary_profession);
        }
        if !known_for_titles.is_empty() {
            doc.add_text(fields.known_for_titles, &known_for_titles);
        }
        if let Some(year) = birth_year {
            doc.add_i64(fields.birth_year, year);
        }
        if let Some(year) = death_year {
            doc.add_i64(fields.death_year, year);
        }

        writer
            .add_document(doc)
            .context("adding document to name index")?;
        record_count += 1;

        if record_count.is_multiple_of(100_000) {
            info!(processed = record_count, "name indexing progress");
        }
    }

    info!(processed = record_count, "committing name index");
    writer.commit().context("committing name index")?;
    Ok(())
}

fn load_ratings_map(path: &Path) -> Result<HashMap<String, (f64, i64)>> {
    let mut map = HashMap::new();
    let mut reader = ReaderBuilder::new()
        .delimiter(b'\t')
        .has_headers(true)
        .flexible(true)
        .from_path(path)
        .with_context(|| format!("opening {}", path.display()))?;

    for result in reader.records() {
        let record = result.with_context(|| format!("reading {}", path.display()))?;
        if record.len() < 3 {
            continue;
        }
        let tconst = record[0].to_string();
        if tconst.is_empty() || tconst == "\\N" {
            continue;
        }
        let rating = parse_f64(record.get(1));
        let votes = parse_i64(record.get(2));
        if let (Some(rating), Some(votes)) = (rating, votes) {
            map.insert(tconst, (rating, votes));
        }
    }

    Ok(map)
}

fn load_aka_map(path: &Path) -> Result<HashMap<String, Vec<String>>> {
    let mut map: HashMap<String, Vec<String>> = HashMap::new();
    let mut reader = ReaderBuilder::new()
        .delimiter(b'\t')
        .has_headers(true)
        .flexible(true)
        .from_path(path)
        .with_context(|| format!("opening {}", path.display()))?;

    for result in reader.records() {
        let record = result.with_context(|| format!("reading {}", path.display()))?;
        let Some(title_id) = record.get(0) else {
            continue;
        };
        let Some(title) = record.get(2) else {
            continue;
        };
        if title.is_empty() || title == "\\N" {
            continue;
        }
        map.entry(title_id.to_string())
            .or_default()
            .push(title.to_string());
    }

    Ok(map)
}

fn load_name_map(path: &Path) -> Result<HashMap<String, String>> {
    let mut map = HashMap::new();
    let mut reader = ReaderBuilder::new()
        .delimiter(b'\t')
        .has_headers(true)
        .flexible(true)
        .from_path(path)
        .with_context(|| format!("opening {}", path.display()))?;

    for result in reader.records() {
        let record = result.with_context(|| format!("reading {}", path.display()))?;
        let Some(nconst) = record.get(0) else {
            continue;
        };
        let Some(primary_name) = record.get(1) else {
            continue;
        };
        if nconst.is_empty() || nconst == "\\N" || primary_name.is_empty() {
            continue;
        }
        map.insert(nconst.to_string(), primary_name.to_string());
    }

    Ok(map)
}

fn load_principals_map(
    path: &Path,
    name_lookup: &HashMap<String, String>,
) -> Result<HashMap<String, Vec<String>>> {
    let mut map: HashMap<String, HashSet<String>> = HashMap::new();
    let mut reader = ReaderBuilder::new()
        .delimiter(b'\t')
        .has_headers(true)
        .flexible(true)
        .from_path(path)
        .with_context(|| format!("opening {}", path.display()))?;

    for result in reader.records() {
        let record = result.with_context(|| format!("reading {}", path.display()))?;
        let Some(tconst) = record.get(0) else {
            continue;
        };
        let Some(nconst) = record.get(2) else {
            continue;
        };

        if tconst.is_empty() || tconst == "\\N" || nconst.is_empty() || nconst == "\\N" {
            continue;
        }

        let Some(name) = name_lookup.get(nconst) else {
            continue;
        };

        map.entry(tconst.to_string())
            .or_default()
            .insert(name.clone());
    }

    Ok(map
        .into_iter()
        .map(|(tconst, names)| (tconst, names.into_iter().collect()))
        .collect())
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
