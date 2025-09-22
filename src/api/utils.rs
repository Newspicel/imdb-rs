use serde::Deserializer;
use tantivy::schema::{Field, OwnedValue, TantivyDocument};

use crate::indexer::{NameFields, TitleFields};

use super::types::{NameSearchResult, TitleSearchResult};

pub fn deserialize_one_or_many<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    struct Visitor;

    impl<'de> serde::de::Visitor<'de> for Visitor {
        type Value = Vec<String>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a string or list of strings")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(vec![value.to_string()])
        }

        fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(vec![value])
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            let mut values = Vec::new();
            while let Some(value) = seq.next_element::<String>()? {
                values.push(value);
            }
            Ok(values)
        }
    }

    deserializer.deserialize_any(Visitor)
}

pub fn document_to_title_result(
    doc: &TantivyDocument,
    fields: &TitleFields,
) -> Result<TitleSearchResult, anyhow::Error> {
    let primary_title = get_first_text(doc, fields.primary_title)
        .ok_or_else(|| anyhow::anyhow!("document missing primaryTitle"))?;

    Ok(TitleSearchResult {
        tconst: get_first_text(doc, fields.tconst).unwrap_or_default(),
        primary_title,
        original_title: get_first_text(doc, fields.original_title),
        title_type: get_first_text(doc, fields.title_type),
        start_year: get_first_i64(doc, fields.start_year),
        end_year: get_first_i64(doc, fields.end_year),
        genres: get_all_text(doc, fields.genres),
        average_rating: get_first_f64(doc, fields.average_rating),
        num_votes: get_first_i64(doc, fields.num_votes),
        score: None,
        sort_value: None,
    })
}

pub fn document_to_name_result(
    doc: &TantivyDocument,
    fields: &NameFields,
) -> Result<NameSearchResult, anyhow::Error> {
    let primary_name = get_first_text(doc, fields.primary_name)
        .ok_or_else(|| anyhow::anyhow!("document missing primaryName"))?;

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

pub fn get_first_text(doc: &TantivyDocument, field: Field) -> Option<String> {
    doc.get_first(field)
        .and_then(|value| match OwnedValue::from(value) {
            OwnedValue::Str(text) => Some(text),
            OwnedValue::PreTokStr(pre) => Some(pre.text),
            OwnedValue::Facet(facet) => Some(facet.to_path_string()),
            _ => None,
        })
}

pub fn get_all_text(doc: &TantivyDocument, field: Field) -> Option<Vec<String>> {
    let values: Vec<String> = doc
        .get_all(field)
        .filter_map(|value| match OwnedValue::from(value) {
            OwnedValue::Str(text) => Some(text),
            OwnedValue::PreTokStr(pre) => Some(pre.text),
            OwnedValue::Facet(facet) => Some(facet.to_path_string()),
            _ => None,
        })
        .collect();
    if values.is_empty() {
        None
    } else {
        Some(values)
    }
}

pub fn get_first_i64(doc: &TantivyDocument, field: Field) -> Option<i64> {
    doc.get_first(field)
        .and_then(|value| match OwnedValue::from(value) {
            OwnedValue::I64(v) => Some(v),
            OwnedValue::U64(v) => i64::try_from(v).ok(),
            _ => None,
        })
}

pub fn get_first_f64(doc: &TantivyDocument, field: Field) -> Option<f64> {
    doc.get_first(field)
        .and_then(|value| match OwnedValue::from(value) {
            OwnedValue::F64(v) => Some(v),
            OwnedValue::I64(v) => Some(v as f64),
            OwnedValue::U64(v) => Some(v as f64),
            _ => None,
        })
}
