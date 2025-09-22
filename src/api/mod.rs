mod handlers;
mod scoring;
mod state;
pub mod types;
mod utils;

pub use scoring::compute_title_relevance_score;
pub use state::{AppState, router};
