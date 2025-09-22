use super::types::TitleSearchResult;
use tantivy::Score;

pub fn compute_title_relevance_score(
    base_score: Score,
    result: &TitleSearchResult,
    query_lower: Option<&str>,
) -> f32 {
    let mut base = (base_score.max(0.0001) as f64).sqrt();

    let rating = result.average_rating.unwrap_or(5.0);
    let votes = result.num_votes.unwrap_or(0) as f64;

    const GLOBAL_AVG: f64 = 6.5;
    const VOTE_THRESHOLD: f64 = 1_000.0;
    let vote_mix = votes / (votes + VOTE_THRESHOLD);
    let weighted_rating = vote_mix * rating + (1.0 - vote_mix) * GLOBAL_AVG;
    let rating_component = (weighted_rating / 10.0) * 3.5;

    let popularity_component = (votes + 1.0).ln() / 9.0;

    let recency_year = result.end_year.or(result.start_year).unwrap_or_default();
    let year_component = if recency_year == 0 {
        0.0
    } else {
        ((recency_year as f64 - 2010.0) / 80.0).clamp(-0.05, 0.18)
    };

    let running_bonus = if result.end_year.is_none() { 0.08 } else { 0.0 };

    let mut title_bonus = 0.0;

    if let Some(query) = query_lower {
        let needle = query.trim();
        if !needle.is_empty() {
            let haystack = result.primary_title.to_lowercase();
            if haystack == needle {
                base = base.max(4.0);
                title_bonus = 5.5;
            } else if haystack.starts_with(needle) {
                title_bonus = 1.75;
            } else if haystack.contains(needle) {
                title_bonus = 0.4;
            } else if needle.len() <= 3 {
                title_bonus = -0.6;
            } else {
                title_bonus = -0.3;
            }
        }
    }

    let mut combined = 1.0
        + rating_component
        + popularity_component
        + year_component
        + running_bonus
        + title_bonus;

    if votes < 10.0 || result.average_rating.is_none() {
        combined *= 0.25;
    }

    (base * combined) as f32
}
