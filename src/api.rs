use std::cmp::Ordering;
use std::ops::Bound;
use std::sync::Arc;

use axum::extract::{Path, Query as AxumQuery, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tantivy::collector::TopDocs;
use tantivy::query::{AllQuery, BooleanQuery, Occur, Query as TantivyQuery, RangeQuery, TermQuery};
use tantivy::schema::{Field, OwnedValue, TantivyDocument};
use tantivy::{DocAddress, Order, Score, Term};
use tracing::instrument;

use crate::indexer::{NameFields, NameIndex, PreparedIndexes, TitleFields, TitleIndex};

#[derive(Clone)]
pub struct AppState {
    title_index: Arc<TitleIndex>,
    name_index: Arc<NameIndex>,
}

impl AppState {
    pub fn new(indexes: PreparedIndexes) -> Self {
        Self {
            title_index: Arc::new(indexes.titles),
            name_index: Arc::new(indexes.names),
        }
    }
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/search", get(search_titles))
        .route("/titles/search", get(search_titles))
        .route("/names/search", get(search_names))
        .route("/titles/:tconst", get(get_title_by_id))
        .route("/names/:nconst", get(get_name_by_id))
        .with_state(state)
}

#[derive(Debug, Deserialize)]
struct TitleSearchParams {
    #[serde(default)]
    query: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    title_type: Option<String>,
    #[serde(default)]
    start_year_min: Option<i64>,
    #[serde(default)]
    start_year_max: Option<i64>,
    #[serde(default)]
    min_rating: Option<f64>,
    #[serde(default)]
    max_rating: Option<f64>,
    #[serde(default)]
    min_votes: Option<i64>,
    #[serde(default)]
    max_votes: Option<i64>,
    #[serde(default)]
    genres: Vec<String>,
    #[serde(default)]
    sort: Option<SortMode>,
}

#[derive(Debug, Clone, Copy, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
enum SortMode {
    #[default]
    Relevance,
    RatingDesc,
    RatingAsc,
    VotesDesc,
    VotesAsc,
}

#[derive(Debug, Serialize)]
struct TitleSearchResponse {
    results: Vec<TitleSearchResult>,
}

#[derive(Debug, Serialize)]
struct TitleSearchResult {
    tconst: String,
    primary_title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    original_title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    title_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    start_year: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    genres: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    average_rating: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    num_votes: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    score: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sort_value: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct NameSearchParams {
    #[serde(default)]
    query: String,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    birth_year_min: Option<i64>,
    #[serde(default)]
    birth_year_max: Option<i64>,
    #[serde(default)]
    primary_profession: Vec<String>,
}

#[derive(Debug, Serialize)]
struct NameSearchResponse {
    results: Vec<NameSearchResult>,
}

#[derive(Debug, Serialize)]
struct NameSearchResult {
    nconst: String,
    primary_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    birth_year: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    death_year: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    primary_profession: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    known_for_titles: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    score: Option<f32>,
}

#[instrument(skip(state))]
async fn search_titles(
    State(state): State<AppState>,
    AxumQuery(params): AxumQuery<TitleSearchParams>,
) -> Result<Json<TitleSearchResponse>, ApiError> {
    let limit = params.limit.unwrap_or(10).clamp(1, 50);
    let sort_mode = params.sort.unwrap_or_default();

    let query_text = params.query.as_deref().unwrap_or("").trim().to_string();

    if query_text.is_empty()
        && params.title_type.is_none()
        && params.start_year_min.is_none()
        && params.start_year_max.is_none()
        && params.min_rating.is_none()
        && params.max_rating.is_none()
        && params.min_votes.is_none()
        && params.max_votes.is_none()
        && params.genres.is_empty()
    {
        return Err(ApiError::bad_request(
            "at least one search term or filter must be provided",
        ));
    }

    let title_index = &state.title_index;
    let searcher = title_index.reader.searcher();

    let mut clauses: Vec<(Occur, Box<dyn TantivyQuery>)> = Vec::new();

    if !query_text.is_empty() {
        let parsed_query = title_index
            .query_parser
            .parse_query(&query_text)
            .map_err(|err| ApiError::bad_request(format!("invalid query: {}", err)))?;
        clauses.push((Occur::Must, parsed_query));
    }

    if let Some(title_type) = params.title_type.as_ref().filter(|value| !value.is_empty()) {
        let term = Term::from_field_text(title_index.fields.title_type, title_type);
        let query = TermQuery::new(term, Default::default());
        clauses.push((Occur::Must, Box::new(query)));
    }

    if params.start_year_min.is_some() || params.start_year_max.is_some() {
        let lower = params
            .start_year_min
            .map(|value| {
                Bound::Included(Term::from_field_i64(title_index.fields.start_year, value))
            })
            .unwrap_or(Bound::Unbounded);
        let upper = params
            .start_year_max
            .map(|value| {
                Bound::Included(Term::from_field_i64(title_index.fields.start_year, value))
            })
            .unwrap_or(Bound::Unbounded);
        let range = RangeQuery::new(lower, upper);
        clauses.push((Occur::Must, Box::new(range)));
    }

    if params.min_rating.is_some() || params.max_rating.is_some() {
        let lower = params
            .min_rating
            .map(|value| {
                Bound::Included(Term::from_field_f64(
                    title_index.fields.average_rating,
                    value,
                ))
            })
            .unwrap_or(Bound::Unbounded);
        let upper = params
            .max_rating
            .map(|value| {
                Bound::Included(Term::from_field_f64(
                    title_index.fields.average_rating,
                    value,
                ))
            })
            .unwrap_or(Bound::Unbounded);
        let range = RangeQuery::new(lower, upper);
        clauses.push((Occur::Must, Box::new(range)));
    }

    if params.min_votes.is_some() || params.max_votes.is_some() {
        let lower = params
            .min_votes
            .map(|value| Bound::Included(Term::from_field_i64(title_index.fields.num_votes, value)))
            .unwrap_or(Bound::Unbounded);
        let upper = params
            .max_votes
            .map(|value| Bound::Included(Term::from_field_i64(title_index.fields.num_votes, value)))
            .unwrap_or(Bound::Unbounded);
        let range = RangeQuery::new(lower, upper);
        clauses.push((Occur::Must, Box::new(range)));
    }

    for genre in params.genres.iter().filter(|genre| !genre.is_empty()) {
        let term = Term::from_field_text(title_index.fields.genres, genre);
        let query = TermQuery::new(term, Default::default());
        clauses.push((Occur::Must, Box::new(query)));
    }

    let combined_query: Box<dyn TantivyQuery> = match clauses.len() {
        0 => Box::new(AllQuery),
        1 => clauses.into_iter().next().unwrap().1,
        _ => Box::new(BooleanQuery::from(clauses)),
    };

    let field_name = |field: Field| title_index.schema.get_field_entry(field).name().to_string();

    enum CollectedDocs {
        Score(Vec<(Score, DocAddress)>),
        F64(Vec<(f64, DocAddress)>),
        I64(Vec<(i64, DocAddress)>),
    }

    let query_lower = if query_text.is_empty() {
        None
    } else {
        Some(query_text.to_lowercase())
    };

    let hits = match sort_mode {
        SortMode::Relevance => CollectedDocs::Score(
            searcher
                .search(&combined_query, &TopDocs::with_limit(limit))
                .map_err(|err| ApiError::internal(err.into()))?,
        ),
        SortMode::RatingDesc => {
            let collector = TopDocs::with_limit(limit).order_by_fast_field::<f64>(
                field_name(title_index.fields.average_rating),
                Order::Desc,
            );
            CollectedDocs::F64(
                searcher
                    .search(&combined_query, &collector)
                    .map_err(|err| ApiError::internal(err.into()))?,
            )
        }
        SortMode::RatingAsc => {
            let collector = TopDocs::with_limit(limit).order_by_fast_field::<f64>(
                field_name(title_index.fields.average_rating),
                Order::Asc,
            );
            CollectedDocs::F64(
                searcher
                    .search(&combined_query, &collector)
                    .map_err(|err| ApiError::internal(err.into()))?,
            )
        }
        SortMode::VotesDesc => {
            let collector = TopDocs::with_limit(limit)
                .order_by_fast_field::<i64>(field_name(title_index.fields.num_votes), Order::Desc);
            CollectedDocs::I64(
                searcher
                    .search(&combined_query, &collector)
                    .map_err(|err| ApiError::internal(err.into()))?,
            )
        }
        SortMode::VotesAsc => {
            let collector = TopDocs::with_limit(limit)
                .order_by_fast_field::<i64>(field_name(title_index.fields.num_votes), Order::Asc);
            CollectedDocs::I64(
                searcher
                    .search(&combined_query, &collector)
                    .map_err(|err| ApiError::internal(err.into()))?,
            )
        }
    };

    let mut results = Vec::new();

    match hits {
        CollectedDocs::Score(docs) => {
            for (base_score, addr) in docs {
                let doc = searcher
                    .doc::<TantivyDocument>(addr)
                    .map_err(|err| ApiError::internal(err.into()))?;
                let mut result = document_to_title_result(&doc, &title_index.fields)?;
                let final_score =
                    compute_title_relevance_score(base_score, &result, query_lower.as_deref());
                result.score = Some(final_score);
                results.push(result);
            }
        }
        CollectedDocs::F64(docs) => {
            for (value, addr) in docs {
                let doc = searcher
                    .doc::<TantivyDocument>(addr)
                    .map_err(|err| ApiError::internal(err.into()))?;
                let mut result = document_to_title_result(&doc, &title_index.fields)?;
                result.sort_value = Some(value);
                results.push(result);
            }
        }
        CollectedDocs::I64(docs) => {
            for (value, addr) in docs {
                let doc = searcher
                    .doc::<TantivyDocument>(addr)
                    .map_err(|err| ApiError::internal(err.into()))?;
                let mut result = document_to_title_result(&doc, &title_index.fields)?;
                result.sort_value = Some(value as f64);
                results.push(result);
            }
        }
    }

    if matches!(sort_mode, SortMode::Relevance) {
        results.sort_by(|a, b| {
            let left = a.score.unwrap_or_default();
            let right = b.score.unwrap_or_default();
            right.partial_cmp(&left).unwrap_or(Ordering::Equal)
        });
        results.truncate(limit);
    }

    Ok(Json(TitleSearchResponse { results }))
}

#[instrument(skip(state))]
async fn search_names(
    State(state): State<AppState>,
    AxumQuery(params): AxumQuery<NameSearchParams>,
) -> Result<Json<NameSearchResponse>, ApiError> {
    let query_text = params.query.trim();
    let has_filters = params.birth_year_min.is_some()
        || params.birth_year_max.is_some()
        || !params.primary_profession.is_empty();

    if query_text.is_empty() && !has_filters {
        return Err(ApiError::bad_request(
            "provide a query or at least one filter",
        ));
    }

    let limit = params.limit.unwrap_or(10).clamp(1, 50);
    let name_index = &state.name_index;
    let searcher = name_index.reader.searcher();

    let mut clauses: Vec<(Occur, Box<dyn TantivyQuery>)> = Vec::new();

    if !query_text.is_empty() {
        let parsed_query = name_index
            .query_parser
            .parse_query(query_text)
            .map_err(|err| ApiError::bad_request(format!("invalid query: {}", err)))?;
        clauses.push((Occur::Must, parsed_query));
    }

    if params.birth_year_min.is_some() || params.birth_year_max.is_some() {
        let lower = params
            .birth_year_min
            .map(|value| Bound::Included(Term::from_field_i64(name_index.fields.birth_year, value)))
            .unwrap_or(Bound::Unbounded);
        let upper = params
            .birth_year_max
            .map(|value| Bound::Included(Term::from_field_i64(name_index.fields.birth_year, value)))
            .unwrap_or(Bound::Unbounded);
        let range = RangeQuery::new(lower, upper);
        clauses.push((Occur::Must, Box::new(range)));
    }

    for profession in params
        .primary_profession
        .iter()
        .filter(|value| !value.is_empty())
    {
        let term = Term::from_field_text(name_index.fields.primary_profession, profession);
        let query = TermQuery::new(term, Default::default());
        clauses.push((Occur::Must, Box::new(query)));
    }

    let combined_query: Box<dyn TantivyQuery> = match clauses.len() {
        0 => Box::new(AllQuery),
        1 => clauses.into_iter().next().unwrap().1,
        _ => Box::new(BooleanQuery::from(clauses)),
    };

    let hits = searcher
        .search(&combined_query, &TopDocs::with_limit(limit))
        .map_err(|err| ApiError::internal(err.into()))?;

    let mut results = Vec::with_capacity(hits.len());
    for (score, addr) in hits {
        let doc = searcher
            .doc::<TantivyDocument>(addr)
            .map_err(|err| ApiError::internal(err.into()))?;
        let mut result = document_to_name_result(&doc, &name_index.fields)?;
        result.score = Some(score);
        results.push(result);
    }

    Ok(Json(NameSearchResponse { results }))
}

#[instrument(skip(state))]
async fn get_title_by_id(
    State(state): State<AppState>,
    Path(tconst): Path<String>,
) -> Result<Json<TitleSearchResult>, ApiError> {
    let title_index = &state.title_index;
    let searcher = title_index.reader.searcher();
    let term = Term::from_field_text(title_index.fields.tconst, &tconst);
    let query = TermQuery::new(term, Default::default());

    let hits = searcher
        .search(&query, &TopDocs::with_limit(1))
        .map_err(|err| ApiError::internal(err.into()))?;

    if let Some((score, addr)) = hits.into_iter().next() {
        let doc = searcher
            .doc::<TantivyDocument>(addr)
            .map_err(|err| ApiError::internal(err.into()))?;
        let mut result = document_to_title_result(&doc, &title_index.fields)?;
        result.score = Some(score);
        return Ok(Json(result));
    }

    Err(ApiError::not_found("title not found"))
}

#[instrument(skip(state))]
async fn get_name_by_id(
    State(state): State<AppState>,
    Path(nconst): Path<String>,
) -> Result<Json<NameSearchResult>, ApiError> {
    let name_index = &state.name_index;
    let searcher = name_index.reader.searcher();
    let term = Term::from_field_text(name_index.fields.nconst, &nconst);
    let query = TermQuery::new(term, Default::default());

    let hits = searcher
        .search(&query, &TopDocs::with_limit(1))
        .map_err(|err| ApiError::internal(err.into()))?;

    if let Some((score, addr)) = hits.into_iter().next() {
        let doc = searcher
            .doc::<TantivyDocument>(addr)
            .map_err(|err| ApiError::internal(err.into()))?;
        let mut result = document_to_name_result(&doc, &name_index.fields)?;
        result.score = Some(score);
        return Ok(Json(result));
    }

    Err(ApiError::not_found("name not found"))
}

async fn healthz() -> &'static str {
    "ok"
}

fn document_to_title_result(
    doc: &TantivyDocument,
    fields: &TitleFields,
) -> Result<TitleSearchResult, ApiError> {
    let primary_title = get_first_text(doc, fields.primary_title)
        .ok_or_else(|| ApiError::internal(anyhow::anyhow!("document missing primaryTitle")))?;

    let result = TitleSearchResult {
        tconst: get_first_text(doc, fields.tconst).unwrap_or_default(),
        primary_title,
        original_title: get_first_text(doc, fields.original_title),
        title_type: get_first_text(doc, fields.title_type),
        start_year: get_first_i64(doc, fields.start_year),
        genres: get_all_text(doc, fields.genres),
        average_rating: get_first_f64(doc, fields.average_rating),
        num_votes: get_first_i64(doc, fields.num_votes),
        score: None,
        sort_value: None,
    };
    Ok(result)
}

fn document_to_name_result(
    doc: &TantivyDocument,
    fields: &NameFields,
) -> Result<NameSearchResult, ApiError> {
    let primary_name = get_first_text(doc, fields.primary_name)
        .ok_or_else(|| ApiError::internal(anyhow::anyhow!("document missing primaryName")))?;

    let professions = get_all_text(doc, fields.primary_profession).map(|values| {
        values
            .into_iter()
            .flat_map(|entry| {
                entry
                    .split(',')
                    .map(|s| s.trim())
                    .filter(|value| !value.is_empty())
                    .map(String::from)
                    .collect::<Vec<String>>()
            })
            .collect::<Vec<String>>()
    });
    let known_for = get_all_text(doc, fields.known_for_titles).map(|values| {
        values
            .into_iter()
            .flat_map(|entry| {
                entry
                    .split(',')
                    .map(|s| s.trim())
                    .filter(|value| !value.is_empty())
                    .map(String::from)
                    .collect::<Vec<String>>()
            })
            .collect::<Vec<String>>()
    });

    Ok(NameSearchResult {
        nconst: get_first_text(doc, fields.nconst).unwrap_or_default(),
        primary_name,
        birth_year: get_first_i64(doc, fields.birth_year),
        death_year: get_first_i64(doc, fields.death_year),
        primary_profession: professions,
        known_for_titles: known_for,
        score: None,
    })
}

fn compute_title_relevance_score(
    base_score: Score,
    result: &TitleSearchResult,
    query_lower: Option<&str>,
) -> f32 {
    let base = base_score.max(0.0001) as f64;

    let rating = result.average_rating.unwrap_or(5.0);
    let votes = result.num_votes.unwrap_or(0) as f64;
    let year_component = result
        .start_year
        .map(|year| ((year as f64 - 1980.0) / 50.0).clamp(0.0, 1.5))
        .unwrap_or(0.0);

    let vote_weight = (1.0 + votes).ln();
    let rating_component = (rating / 10.0) * (1.0 + vote_weight / 5.0);
    let popularity_component = vote_weight / 6.0;

    let primary_bonus = query_lower
        .and_then(|needle| {
            let haystack = result.primary_title.to_lowercase();
            if haystack.contains(needle) {
                Some(0.35)
            } else {
                None
            }
        })
        .unwrap_or(0.0);

    let combined = 1.0 + rating_component + popularity_component + year_component + primary_bonus;
    (base * combined) as f32
}

fn get_first_text(doc: &TantivyDocument, field: tantivy::schema::Field) -> Option<String> {
    doc.get_first(field)
        .and_then(|value| owned_value_to_string(&OwnedValue::from(value)))
}

fn get_all_text(doc: &TantivyDocument, field: tantivy::schema::Field) -> Option<Vec<String>> {
    let values: Vec<String> = doc
        .get_all(field)
        .filter_map(|value| owned_value_to_string(&OwnedValue::from(value)))
        .collect();
    if values.is_empty() {
        None
    } else {
        Some(values)
    }
}

fn get_first_i64(doc: &TantivyDocument, field: tantivy::schema::Field) -> Option<i64> {
    doc.get_first(field)
        .and_then(|value| match OwnedValue::from(value) {
            OwnedValue::I64(v) => Some(v),
            OwnedValue::U64(v) => i64::try_from(v).ok(),
            _ => None,
        })
}

fn get_first_f64(doc: &TantivyDocument, field: tantivy::schema::Field) -> Option<f64> {
    doc.get_first(field)
        .and_then(|value| match OwnedValue::from(value) {
            OwnedValue::F64(v) => Some(v),
            OwnedValue::I64(v) => Some(v as f64),
            OwnedValue::U64(v) => Some(v as f64),
            _ => None,
        })
}

fn owned_value_to_string(value: &OwnedValue) -> Option<String> {
    match value {
        OwnedValue::Str(text) => Some(text.clone()),
        OwnedValue::PreTokStr(pre) => Some(pre.text.clone()),
        OwnedValue::Facet(facet) => Some(facet.to_path_string()),
        _ => None,
    }
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: String,
    detail: Option<anyhow::Error>,
}

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
            detail: None,
        }
    }

    fn internal(err: anyhow::Error) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: "internal server error".to_string(),
            detail: Some(err),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
            detail: None,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        if let Some(detail) = &self.detail {
            tracing::error!(error = %detail);
        }
        let body = Json(ErrorBody {
            message: self.message,
        });
        (self.status, body).into_response()
    }
}

#[derive(Serialize)]
struct ErrorBody {
    message: String,
}

impl From<anyhow::Error> for ApiError {
    fn from(value: anyhow::Error) -> Self {
        ApiError::internal(value)
    }
}
