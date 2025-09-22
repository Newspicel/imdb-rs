use std::cmp::Ordering;
use std::ops::Bound;

use axum::Json;
use axum::extract::{Path, Query as AxumQuery, State};
use tantivy::collector::TopDocs;
use tantivy::query::{AllQuery, BooleanQuery, Occur, Query as TantivyQuery, RangeQuery, TermQuery};
use tantivy::schema::{Field, TantivyDocument};
use tantivy::{DocAddress, Order, Score, Term};
use tracing::{debug, instrument};

use super::scoring::compute_title_relevance_score;
use super::state::AppState;
use super::types::{
    ApiError, NameSearchParams, NameSearchResponse, NameSearchResult, SortMode, TitleSearchParams,
    TitleSearchResponse, TitleSearchResult,
};
use super::utils::{document_to_name_result, document_to_title_result};

pub async fn healthz() -> &'static str {
    "ok"
}

#[instrument(skip_all)]
pub async fn search_titles(
    State(state): State<AppState>,
    AxumQuery(params): AxumQuery<TitleSearchParams>,
) -> Result<Json<TitleSearchResponse>, ApiError> {
    let limit = params.limit.unwrap_or(10).clamp(1, 50);
    let sort_mode = params.sort.unwrap_or_default();

    let query_text = params.query.as_deref().unwrap_or("").trim().to_string();
    let default_title_types = vec!["movie".to_string(), "tvSeries".to_string()];
    let title_types: Vec<String> = match params.title_type.as_ref() {
        Some(value) if !value.is_empty() => vec![value.clone()],
        _ => default_title_types,
    };

    if query_text.is_empty()
        && params.title_type.is_none()
        && params.start_year_min.is_none()
        && params.min_rating.is_none()
        && params.max_rating.is_none()
        && params.min_votes.is_none()
        && params.max_votes.is_none()
        && params.genres.is_empty()
    {
        debug!("applying default title filters: titleType in [movie,tvSeries], start_year>=1980");
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

    if title_types.len() == 1 {
        let term = Term::from_field_text(title_index.fields.title_type, &title_types[0]);
        let query = TermQuery::new(term, Default::default());
        clauses.push((Occur::Must, Box::new(query)));
    } else {
        let shoulds: Vec<(Occur, Box<dyn TantivyQuery>)> = title_types
            .into_iter()
            .map(|value| {
                let term = Term::from_field_text(title_index.fields.title_type, &value);
                (
                    Occur::Should,
                    Box::new(TermQuery::new(term, Default::default())) as Box<dyn TantivyQuery>,
                )
            })
            .collect();
        if !shoulds.is_empty() {
            clauses.push((Occur::Must, Box::new(BooleanQuery::from(shoulds))));
        }
    }

    let mut year_min = params.start_year_min.unwrap_or(1980);
    let mut year_max = params.start_year_max;
    if let Some(explicit_min) = params.start_year_min {
        year_min = explicit_min;
    }
    if let Some(explicit_max) = params.start_year_max {
        year_max = Some(explicit_max);
    }

    if year_min != 0 || year_max.is_some() {
        let lower = Bound::Included(Term::from_field_i64(
            title_index.fields.start_year,
            year_min,
        ));
        let upper = year_max
            .map(|value| {
                Bound::Included(Term::from_field_i64(title_index.fields.start_year, value))
            })
            .unwrap_or(Bound::Unbounded);
        let range = RangeQuery::new(lower, upper);
        clauses.push((Occur::Must, Box::new(range)));
    }

    if params.end_year_min.is_some() || params.end_year_max.is_some() {
        let lower = params
            .end_year_min
            .map(|value| Bound::Included(Term::from_field_i64(title_index.fields.end_year, value)))
            .unwrap_or(Bound::Unbounded);
        let upper = params
            .end_year_max
            .map(|value| Bound::Included(Term::from_field_i64(title_index.fields.end_year, value)))
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

#[instrument(skip_all)]
pub async fn search_names(
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

#[instrument(skip_all)]
pub async fn get_title_by_id(
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

#[instrument(skip_all)]
pub async fn get_name_by_id(
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
