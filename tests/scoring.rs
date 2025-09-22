use imdb_rs::api::compute_title_relevance_score;
use imdb_rs::api::types::TitleSearchResult;

#[test]
fn relevance_score_rewards_rating_votes_and_recency() {
    let base = 1.0;
    let high = TitleSearchResult {
        tconst: "tt1".into(),
        primary_title: "High".into(),
        original_title: None,
        title_type: Some("movie".into()),
        start_year: Some(2020),
        end_year: Some(2020),
        genres: None,
        average_rating: Some(8.5),
        num_votes: Some(50_000),
        score: None,
        sort_value: None,
    };
    let low = TitleSearchResult {
        tconst: "tt2".into(),
        primary_title: "Low".into(),
        original_title: None,
        title_type: Some("movie".into()),
        start_year: Some(1990),
        end_year: Some(1990),
        genres: None,
        average_rating: Some(6.0),
        num_votes: Some(10),
        score: None,
        sort_value: None,
    };

    let high_score = compute_title_relevance_score(base, &high, Some("high"));
    let low_score = compute_title_relevance_score(base, &low, Some("low"));

    assert!(high_score > low_score);
}

#[test]
fn higher_rating_and_votes_outweigh_recency() {
    let base = 2.0;
    let recent = TitleSearchResult {
        tconst: "tt_new".into(),
        primary_title: "One Piece".into(),
        original_title: None,
        title_type: Some("tvSeries".into()),
        start_year: Some(2023),
        end_year: None,
        genres: None,
        average_rating: Some(8.3),
        num_votes: Some(179_650),
        score: None,
        sort_value: None,
    };
    let classic = TitleSearchResult {
        tconst: "tt_classic".into(),
        primary_title: "One Piece".into(),
        original_title: None,
        title_type: Some("tvSeries".into()),
        start_year: Some(1999),
        end_year: Some(1999),
        genres: None,
        average_rating: Some(9.0),
        num_votes: Some(321_631),
        score: None,
        sort_value: None,
    };

    let recent_score = compute_title_relevance_score(base, &recent, Some("one piece"));
    let classic_score = compute_title_relevance_score(base, &classic, Some("one piece"));

    assert!(
        classic_score > recent_score,
        "classic should outrank recent due to higher rating/votes"
    );
}

#[test]
fn exact_title_match_outranks_partial_even_with_lower_base() {
    let exact = TitleSearchResult {
        tconst: "tt_exact".into(),
        primary_title: "Up".into(),
        original_title: None,
        title_type: Some("movie".into()),
        start_year: Some(2009),
        end_year: Some(2009),
        genres: None,
        average_rating: Some(8.3),
        num_votes: Some(1_201_529),
        score: None,
        sort_value: None,
    };

    let partial = TitleSearchResult {
        tconst: "tt_partial".into(),
        primary_title: "No Way Up".into(),
        original_title: None,
        title_type: Some("movie".into()),
        start_year: Some(2024),
        end_year: Some(2024),
        genres: None,
        average_rating: Some(4.6),
        num_votes: Some(11_321),
        score: None,
        sort_value: None,
    };

    let exact_score = compute_title_relevance_score(0.75, &exact, Some("up"));
    let partial_score = compute_title_relevance_score(5.0, &partial, Some("up"));

    assert!(
        exact_score > partial_score,
        "exact title match with better rating should outrank partial match"
    );
}
