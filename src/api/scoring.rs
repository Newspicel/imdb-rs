use chrono::{Datelike, Utc};
use tantivy::Score;

use crate::api::types::TitleSearchResult;

pub fn compute_title_relevance_score(
    base_score: Score,
    result: &TitleSearchResult,
    query_lower: Option<&str>,
) -> f32 {
    // ---- 1) Base signal: compress to avoid TF-IDF blowups
    let mut base = ((base_score as f64).max(0.0) + 1.0).ln(); // ~0..~something manageable

    // ---- 2) Title match features (robust for very short queries)
    let mut title_bonus = 0.0f64;

    if let Some(q) = query_lower {
        let needle = q.trim().to_lowercase();
        if !needle.is_empty() {
            let haystack = result.primary_title.to_lowercase();

            // token-aware "word contains": split on non-alphanumeric
            let contains_word = haystack
                .split(|c: char| !c.is_alphanumeric())
                .any(|w| w == needle);

            let is_exact = haystack == needle;
            let is_prefix = haystack.starts_with(&needle);
            let is_substr = haystack.contains(&needle);
            let is_short = needle.chars().count() <= 3;

            if is_exact {
                // exact title match should crush near-matches
                let boost_base = if is_short { 4.5 } else { 3.8 };
                let boost_bonus = if is_short { 7.0 } else { 6.0 };
                base = base.max(boost_base);
                title_bonus += boost_bonus;
            } else if is_short && contains_word {
                // "word match" for short queries like "up", "it", "her"
                title_bonus += 1.2;
            } else if is_prefix {
                title_bonus += 0.9;
            } else if is_substr && !is_short {
                title_bonus += 0.4;
            } else if is_short {
                // substring matches on very short queries are noisy
                title_bonus -= 0.8;
            } else {
                title_bonus -= 0.3;
            }
        }
    }

    // ---- 3) Quality / popularity with proper Bayesian shrinkage
    // Bayesian weighted rating: wr = (v/(v+m))*R + (m/(v+m))*C
    let rating = result.average_rating.unwrap_or(5.0) as f64;
    let votes = result.num_votes.unwrap_or(0) as f64;

    const GLOBAL_AVG: f64 = 6.7; // adjust if your corpus differs
    const M_PRIOR: f64 = 12_000.0; // realistic IMDB-ish prior
    let wr = if votes > 0.0 {
        (votes / (votes + M_PRIOR)) * rating + (M_PRIOR / (votes + M_PRIOR)) * GLOBAL_AVG
    } else {
        GLOBAL_AVG
    };
    // Map to ~[0..3]
    let rating_component = (wr / 10.0) * 3.0;

    // Popularity: log-normalized and softly weighted to avoid swamping
    const VMAX: f64 = 2_000_000.0; // rough upper bound for normalization
    let popularity_component = if votes > 0.0 {
        (votes.ln_1p() / VMAX.ln_1p()) * 2.2 // ~[0..2.2]
    } else {
        0.0
    };

    // ---- 4) Recency (small)
    let current_year = Utc::now().year();
    let recency_year = if matches!(
        result.title_type.as_deref(),
        Some("tvSeries") | Some("tvMiniSeries") | Some("tvEpisode")
    ) && result.end_year.is_none()
    {
        current_year
    } else {
        result
            .end_year
            .or(result.start_year)
            .map(|value| value as i32)
            .unwrap_or(0)
    };
    let year_component = if recency_year == 0 {
        0.0
    } else {
        // gentle tilt: [-0.10 .. +0.15] with center ~2012
        ((recency_year as f64 - 2012.0) / 90.0).clamp(-0.10, 0.15)
    };

    // ---- 5) Combine
    let mut combined = 1.0 + rating_component + popularity_component + year_component + title_bonus;

    // Cold-start dampening: smoothly punish low vote counts
    combined *= if votes < 50.0 {
        0.20
    } else if votes < 500.0 {
        0.50
    } else if votes < 2_000.0 {
        0.80
    } else {
        1.00
    };

    // Keep it positive
    combined = combined.max(0.05);

    (base * combined) as f32
}
