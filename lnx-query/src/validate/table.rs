use std::collections::BTreeMap;

use lnx_common::table::LnxTable;

use crate::validate::{ErrorCode, ValidationError, ValidatorContext};

/// Validates the table name provided by the query syntax.
///
/// An error is returned if the table does not exist.
pub fn validate_and_get_table<'a>(
    context: &mut ValidatorContext,
    tables: &'a BTreeMap<String, LnxTable>,
    table_name: &str,
) -> Result<&'a LnxTable, ValidationError> {
    if let Some(table) = tables.get(table_name) {
        return Ok(table);
    }

    let help = tables
        .keys()
        .filter(|name| crate::utils::damerau_levenshtein(table_name, name) < 3)
        .min_by_key(|name| crate::utils::damerau_levenshtein(table_name, name))
        .map(|closest| format!("did you mean {closest:?}?"));

    // TODO: LNX-171: We should reference a link to the docs when the table is empty
    let message = if tables.is_empty() {
        format!("the table {table_name:?} does not exist, there are currently no tables in the lnx catalog")
    } else {
        format!("the table {table_name:?} does not exist")
    };

    let error = context.build_error(ErrorCode::UnknownTable, message, help);
    Err(error)
}

#[cfg(test)]
mod tests {
    use tantivy::schema::STORED;

    use super::*;

    #[test]
    fn test_validate_and_get_table_rejects_unknown_table() {
        let mut schema_builder = tantivy::schema::Schema::builder();
        schema_builder.add_text_field("example", STORED);
        let schema = schema_builder.build();

        let mut tables = BTreeMap::new();
        tables.insert("example".to_string(), LnxTable::new(schema));

        let mut context = ValidatorContext::default();
        let error = validate_and_get_table(&mut context, &tables, "exampl")
            .expect_err("Validator should reject unknown tables");
        assert_eq!(error.code, ErrorCode::UnknownTable);
        assert_eq!(error.help.as_deref(), Some("did you mean \"example\"?"),);
        assert_eq!(error.message, "the table \"exampl\" does not exist");
        assert_eq!(error.location, "");
    }

    #[test]
    fn test_validate_and_get_table_accepts_known_table() {
        let mut schema_builder = tantivy::schema::Schema::builder();
        schema_builder.add_text_field("example", STORED);
        let schema = schema_builder.build();

        let mut tables = BTreeMap::new();
        tables.insert("example".to_string(), LnxTable::new(schema));

        let mut context = ValidatorContext::default();
        validate_and_get_table(&mut context, &tables, "example")
            .expect("Validator should allow known table");
    }
}
