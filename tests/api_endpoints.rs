use axum::body::{self, Body};
use axum::http::{Request, StatusCode};
use serde_json::from_slice;
use tantivy::Index;
use tantivy::query::QueryParser;
use tantivy::schema::{NumericOptions, STORED, STRING, Schema, TEXT};
use tower::ServiceExt;

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

fn build_title_schema() -> (Schema, imdb_rs::indexer::TitleFields, Index) {
    let schema = {
        let mut builder = Schema::builder();
        builder.add_text_field("tconst", STRING | STORED);
        builder.add_text_field("titleType", STRING | STORED);
        builder.add_text_field("primaryTitle", TEXT | STORED);
        builder.add_text_field("originalTitle", TEXT | STORED);
        builder.add_text_field("genres", TEXT | STORED);
        builder.add_text_field("searchTitles", TEXT);
        let numeric = NumericOptions::default()
            .set_indexed()
            .set_stored()
            .set_fast();
        builder.add_i64_field("startYear", numeric.clone());
        builder.add_i64_field("endYear", numeric.clone());
        builder.add_f64_field("averageRating", numeric.clone());
        builder.add_i64_field("numVotes", numeric);
        builder.build()
    };

    let index = Index::create_in_ram(schema.clone());
    let schema_from_index = index.schema();
    let fields = imdb_rs::indexer::TitleFields {
        tconst: schema_from_index.get_field("tconst").unwrap(),
        primary_title: schema_from_index.get_field("primaryTitle").unwrap(),
        original_title: schema_from_index.get_field("originalTitle").unwrap(),
        title_type: schema_from_index.get_field("titleType").unwrap(),
        start_year: schema_from_index.get_field("startYear").unwrap(),
        end_year: schema_from_index.get_field("endYear").unwrap(),
        genres: schema_from_index.get_field("genres").unwrap(),
        average_rating: schema_from_index.get_field("averageRating").unwrap(),
        num_votes: schema_from_index.get_field("numVotes").unwrap(),
        search_titles: schema_from_index.get_field("searchTitles").unwrap(),
    };

    (schema, fields, index)
}

fn build_name_schema() -> (Schema, imdb_rs::indexer::NameFields, Index) {
    let schema = {
        let mut builder = Schema::builder();
        builder.add_text_field("nconst", STRING | STORED);
        builder.add_text_field("primaryName", TEXT | STORED);
        builder.add_text_field("primaryNameSearch", TEXT);
        builder.add_text_field("primaryProfession", TEXT | STORED);
        builder.add_text_field("knownForTitles", TEXT | STORED);
        let numeric = NumericOptions::default()
            .set_indexed()
            .set_stored()
            .set_fast();
        builder.add_i64_field("birthYear", numeric.clone());
        builder.add_i64_field("deathYear", numeric);
        builder.build()
    };

    let index = Index::create_in_ram(schema.clone());
    let schema_from_index = index.schema();
    let fields = imdb_rs::indexer::NameFields {
        nconst: schema_from_index.get_field("nconst").unwrap(),
        primary_name: schema_from_index.get_field("primaryName").unwrap(),
        primary_name_search: schema_from_index.get_field("primaryNameSearch").unwrap(),
        birth_year: schema_from_index.get_field("birthYear").unwrap(),
        death_year: schema_from_index.get_field("deathYear").unwrap(),
        primary_profession: schema_from_index.get_field("primaryProfession").unwrap(),
        known_for_titles: schema_from_index.get_field("knownForTitles").unwrap(),
    };

    (schema, fields, index)
}

fn build_test_indexes() -> imdb_rs::indexer::PreparedIndexes {
    let (_schema, fields, index) = build_title_schema();
    let mut writer = index
        .writer::<tantivy::schema::TantivyDocument>(20_000_000)
        .unwrap();
    let mut doc = tantivy::schema::TantivyDocument::default();
    doc.add_text(fields.tconst, "tt0133093");
    doc.add_text(fields.title_type, "movie");
    doc.add_text(fields.primary_title, "The Matrix");
    doc.add_text(fields.original_title, "The Matrix");
    doc.add_text(fields.search_titles, "The Matrix");
    doc.add_text(fields.genres, "Action");
    doc.add_text(fields.genres, "Sci-Fi");
    doc.add_i64(fields.start_year, 1999);
    doc.add_i64(fields.end_year, 1999);
    doc.add_f64(fields.average_rating, 8.7);
    doc.add_i64(fields.num_votes, 1_900_000);
    writer.add_document(doc).unwrap();
    writer.commit().unwrap();
    let reader = index.reader().unwrap();
    reader.reload().unwrap();
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

    let title_index = imdb_rs::indexer::TitleIndex {
        schema: index.schema(),
        fields,
        reader,
        query_parser,
    };

    let (_schema, fields, index) = build_name_schema();
    let mut writer = index
        .writer::<tantivy::schema::TantivyDocument>(20_000_000)
        .unwrap();
    let mut doc = tantivy::schema::TantivyDocument::default();
    doc.add_text(fields.nconst, "nm0000206");
    doc.add_text(fields.primary_name, "Keanu Reeves");
    doc.add_text(fields.primary_name_search, "Keanu Reeves");
    doc.add_text(fields.primary_profession, "actor");
    doc.add_text(fields.primary_name_search, "actor");
    doc.add_text(fields.known_for_titles, "tt0133093");
    doc.add_i64(fields.birth_year, 1964);
    writer.add_document(doc).unwrap();
    writer.commit().unwrap();
    let reader = index.reader().unwrap();
    reader.reload().unwrap();
    let mut query_parser = QueryParser::for_index(
        &index,
        vec![fields.primary_name_search, fields.primary_profession],
    );
    query_parser.set_field_boost(fields.primary_name_search, 1.5);
    query_parser.set_field_fuzzy(fields.primary_name_search, false, 1, true);
    query_parser.set_field_fuzzy(fields.primary_profession, false, 1, true);

    let name_index = imdb_rs::indexer::NameIndex {
        fields,
        reader,
        query_parser,
    };

    imdb_rs::indexer::PreparedIndexes {
        titles: title_index,
        names: name_index,
    }
}

#[tokio::test]
async fn title_search_returns_expected_result() -> TestResult<()> {
    let indexes = build_test_indexes();
    let state = imdb_rs::api::AppState::new(indexes);
    let app = imdb_rs::api::router(state);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/titles/search?query=Matrix")
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let bytes = body::to_bytes(response.into_body(), usize::MAX).await?;
    let parsed: imdb_rs::api::types::TitleSearchResponse = from_slice(&bytes)?;
    assert_eq!(parsed.results.len(), 1);
    assert_eq!(parsed.results[0].tconst, "tt0133093");
    Ok(())
}

#[tokio::test]
async fn title_id_endpoint_returns_document() -> TestResult<()> {
    let indexes = build_test_indexes();
    let state = imdb_rs::api::AppState::new(indexes);
    let app = imdb_rs::api::router(state);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/titles/tt0133093")
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let bytes = body::to_bytes(response.into_body(), usize::MAX).await?;
    let parsed: imdb_rs::api::types::TitleSearchResult = from_slice(&bytes)?;
    assert_eq!(parsed.primary_title, "The Matrix");
    Ok(())
}

#[tokio::test]
async fn name_search_supports_typos_and_filters() -> TestResult<()> {
    let indexes = build_test_indexes();
    let state = imdb_rs::api::AppState::new(indexes);
    let app = imdb_rs::api::router(state);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/names/search?query=Kean&birth_year_min=1900&primary_profession=actor")
                .body(Body::empty())?,
        )
        .await?;

    let status = response.status();
    let bytes = body::to_bytes(response.into_body(), usize::MAX).await?;
    if status != StatusCode::OK {
        panic!(
            "unexpected status: {} body: {}",
            status,
            String::from_utf8_lossy(&bytes)
        );
    }
    let parsed: imdb_rs::api::types::NameSearchResponse = from_slice(&bytes)?;
    assert_eq!(parsed.results.len(), 1);
    assert_eq!(parsed.results[0].nconst, "nm0000206");
    Ok(())
}
