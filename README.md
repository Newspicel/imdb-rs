# imdb-rs

Rust service that downloads selected IMDb non-commercial datasets, builds a search index with [Tantivy](https://github.com/quickwit-oss/tantivy), and exposes a simple HTTP API with [Axum](https://docs.rs/axum).

## Features
- Downloads the official IMDb non-commercial TSV archives for names, titles, crew, principals, episodes, akas, and ratings.
- Stores compressed and decompressed TSV files in a configurable data directory.
- Builds Tantivy indices for titles (primary, original, and international AKA titles) and names, enabling multilingual full-text search.
- Async downloader with resumable streaming and background decompression.
- Filterable JSON API for titles (type, year range, genres, rating, vote counts) with optional ranking by rating or votes; title search also matches crew and cast names from `title.principals.tsv`.
- Dedicated name search API backed by the IMDb `name.basics.tsv` dataset.

## Prerequisites
- Rust 1.75+ (project uses the Rust 2024 edition and async/await).
- Sufficient disk space (the full dataset is tens of gigabytes once decompressed).
- Network access to `https://datasets.imdbws.com`.

> ⚠️ The IMDb datasets are licensed for **non-commercial** use only. Review the [IMDb dataset terms](https://developer.imdb.com/non-commercial-datasets/) before using this project and ensure compliance.

## Configuration
Configuration is supplied via environment variables (an optional `.env` file is loaded on startup):

| Variable | Default | Description |
| --- | --- | --- |
| `IMDB_DATA_DIR` | `./data` | Directory where compressed and decompressed TSV files are stored. |
| `IMDB_INDEX_DIR` | `<IMDB_DATA_DIR>/tantivy_index` | Location of the Tantivy index. |
| `IMDB_BIND_ADDR` | `127.0.0.1:3000` | Address for the Axum HTTP server. |

## Running
```bash
# Download datasets, build the index, and start the API server
cargo run --release
```

The first launch will download and decompress all required archives and build the index. Subsequent runs reuse the existing data and index. Delete the index directory if you need to force a rebuild after updating datasets.

## API
### `GET /healthz`
Simple health check endpoint returning `"ok"`.

### `GET /search` and `GET /titles/search`
Searches titles (movies, TV shows, etc.). Supported query parameters:
- `query` *(optional)* – search expression (multilingual via primary, original, and AKA titles).
- `limit` *(optional)* – max results (1–50, default 10).
- `title_type` – filter by exact title type (e.g. `movie`, `tvSeries`).
- `start_year_min`, `start_year_max` – inclusive production year range filters.
- `min_rating`, `max_rating` – inclusive average rating range (floating-point).
- `min_votes`, `max_votes` – inclusive vote-count range.
- `genres` – repeatable parameter to require specific genres (e.g. `genres=Action&genres=Sci-Fi`).
- `sort` – one of `relevance` (default), `rating_desc`, `rating_asc`, `votes_desc`, `votes_asc`.

Response example:
```json
{
  "results": [
    {
      "tconst": "tt0133093",
      "primary_title": "The Matrix",
      "original_title": "The Matrix",
      "title_type": "movie",
      "start_year": 1999,
      "genres": ["Action", "Sci-Fi"],
      "average_rating": 8.7,
      "num_votes": 1900000,
      "score": 13.24534
    }
  ]
}
```

### `GET /names/search`
Searches people from `name.basics.tsv`.

Parameters:
- `query` *(required)* – text to search across primary names and professions.
- `limit` *(optional)* – max results (1–50, default 10).

Response example:
```json
{
  "results": [
    {
      "nconst": "nm0000206",
      "primary_name": "Keanu Reeves",
      "birth_year": 1964,
      "primary_profession": ["actor", "producer"],
      "known_for_titles": ["tt0121765", "tt0133093", "tt0106519", "tt1375666"],
      "score": 14.87334
    }
  ]
}
```

## Development
- `cargo fmt` and `cargo clippy` keep the codebase consistent.
- `cargo check` ensures the project builds without downloading datasets.
- Integration with observability is via `tracing`; control verbosity using `RUST_LOG`, e.g. `RUST_LOG=debug`.

## Notes
- The current index includes title basics and ratings. Additional datasets are downloaded and available for future enrichment (e.g., principals, crew, episodes).
- IMDb datasets are updated daily; consider scheduling periodic re-download + re-index if you need fresh data.
- Large downloads may take time; the downloader skips files already present on disk.
