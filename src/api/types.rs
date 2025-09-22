use axum::{Json, http::StatusCode};
use serde::{Deserialize, Serialize};

use super::utils::deserialize_one_or_many;

#[derive(Debug, Deserialize)]
pub struct TitleSearchParams {
    #[serde(default)]
    pub query: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub title_type: Option<String>,
    #[serde(default)]
    pub start_year_min: Option<i64>,
    #[serde(default)]
    pub start_year_max: Option<i64>,
    #[serde(default)]
    pub end_year_min: Option<i64>,
    #[serde(default)]
    pub end_year_max: Option<i64>,
    #[serde(default)]
    pub min_rating: Option<f64>,
    #[serde(default)]
    pub max_rating: Option<f64>,
    #[serde(default)]
    pub min_votes: Option<i64>,
    #[serde(default)]
    pub max_votes: Option<i64>,
    #[serde(default, deserialize_with = "deserialize_one_or_many")]
    pub genres: Vec<String>,
    #[serde(default)]
    pub sort: Option<SortMode>,
}

#[derive(Debug, Clone, Copy, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SortMode {
    #[default]
    Relevance,
    RatingDesc,
    RatingAsc,
    VotesDesc,
    VotesAsc,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TitleSearchResponse {
    pub results: Vec<TitleSearchResult>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TitleSearchResult {
    pub tconst: String,
    pub primary_title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub original_title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_year: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_year: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub genres: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub average_rating: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_votes: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort_value: Option<f64>,
}

#[derive(Debug, Deserialize)]
pub struct NameSearchParams {
    #[serde(default)]
    pub query: String,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub birth_year_min: Option<i64>,
    #[serde(default)]
    pub birth_year_max: Option<i64>,
    #[serde(default, deserialize_with = "deserialize_one_or_many")]
    pub primary_profession: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NameSearchResponse {
    pub results: Vec<NameSearchResult>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NameSearchResult {
    pub nconst: String,
    pub primary_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub birth_year: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub death_year: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub primary_profession: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub known_for_titles: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score: Option<f32>,
}

#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub message: String,
    pub detail: Option<anyhow::Error>,
}

impl ApiError {
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
            detail: None,
        }
    }

    pub fn internal(err: anyhow::Error) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: "internal server error".to_string(),
            detail: Some(err),
        }
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
            detail: None,
        }
    }
}

#[derive(Serialize)]
pub struct ErrorBody {
    pub message: String,
}

impl axum::response::IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        if let Some(detail) = &self.detail {
            tracing::error!(error = %detail);
        }
        let body = Json(ErrorBody {
            message: self.message,
        });
        (self.status, body).into_response()
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(value: anyhow::Error) -> Self {
        ApiError::internal(value)
    }
}
