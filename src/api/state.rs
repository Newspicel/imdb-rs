use std::sync::Arc;

use axum::Router;
use axum::routing::get;

use crate::indexer::{NameIndex, PreparedIndexes, TitleIndex};

use super::handlers::{get_name_by_id, get_title_by_id, healthz, search_names, search_titles};

#[derive(Clone)]
pub struct AppState {
    pub(crate) title_index: Arc<TitleIndex>,
    pub(crate) name_index: Arc<NameIndex>,
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
        .route("/titles/{tconst}", get(get_title_by_id))
        .route("/names/{nconst}", get(get_name_by_id))
        .with_state(state)
}
