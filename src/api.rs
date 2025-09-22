use std::convert::TryFrom;
use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tantivy::collector::TopDocs;
use tantivy::schema::{OwnedValue, TantivyDocument};
use tracing::instrument;

use crate::indexer::{IndexFields, PreparedIndex};

#[derive(Clone)]
pub struct AppState {
    index_fields: IndexFields,
    reader: tantivy::IndexReader,
    query_parser: Arc<tantivy::query::QueryParser>,
}

impl AppState {
    pub fn new(prepared: PreparedIndex) -> Self {
        Self {
            index_fields: prepared.fields,
            reader: prepared.reader,
            query_parser: Arc::new(prepared.query_parser),
        }
    }
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/search", get(search))
        .with_state(state)
}

#[derive(Debug, Deserialize)]
struct SearchParams {
    query: String,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Serialize)]
struct SearchResponse {
    results: Vec<SearchResult>,
}

#[derive(Debug, Serialize)]
struct SearchResult {
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
}

#[instrument(skip(state))]
async fn search(
    State(state): State<AppState>,
    Query(params): Query<SearchParams>,
) -> Result<Json<SearchResponse>, ApiError> {
    if params.query.trim().is_empty() {
        return Err(ApiError::bad_request("query parameter cannot be empty"));
    }

    let limit = params.limit.unwrap_or(10).clamp(1, 50);

    let query = state
        .query_parser
        .parse_query(&params.query)
        .map_err(|err| ApiError::bad_request(format!("invalid query: {}", err)))?;

    let searcher = state.reader.searcher();
    let top_docs = searcher
        .search(&query, &TopDocs::with_limit(limit))
        .map_err(|err| ApiError::internal(err.into()))?;

    let mut results = Vec::with_capacity(top_docs.len());
    for (_score, addr) in top_docs {
        let doc = searcher
            .doc::<TantivyDocument>(addr)
            .map_err(|err| ApiError::internal(err.into()))?;
        results.push(document_to_result(&doc, &state.index_fields)?);
    }

    Ok(Json(SearchResponse { results }))
}

async fn healthz() -> &'static str {
    "ok"
}

fn document_to_result(
    doc: &TantivyDocument,
    fields: &IndexFields,
) -> Result<SearchResult, ApiError> {
    let primary_title = get_first_text(doc, fields.primary_title)
        .ok_or_else(|| ApiError::internal(anyhow::anyhow!("document missing primaryTitle")))?;

    let result = SearchResult {
        tconst: get_first_text(doc, fields.tconst).unwrap_or_default(),
        primary_title,
        original_title: get_first_text(doc, fields.original_title),
        title_type: get_first_text(doc, fields.title_type),
        start_year: get_first_i64(doc, fields.start_year),
        genres: get_all_text(doc, fields.genres),
        average_rating: get_first_f64(doc, fields.average_rating),
        num_votes: get_first_i64(doc, fields.num_votes),
    };
    Ok(result)
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
