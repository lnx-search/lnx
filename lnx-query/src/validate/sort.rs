use std::collections::BTreeSet;

use crate::syntax::{OneOrManySortBy, SortBy};
use crate::validate::{ErrorCode, ValidationError, ValidatorContext};

/// Validates that the sort configuration is valid for the current schema.
pub fn validate_sort_fields(
    context: &mut ValidatorContext,
    table_name: &str,
    schema: &tantivy::schema::Schema,
    sort: &OneOrManySortBy,
) -> Result<(), ValidationError> {
    match sort {
        OneOrManySortBy::One(sort_by) => {
            context.push_location("$by");
            validate_sort_by(context, table_name, schema, sort_by)?;
            context.pop_location();
        },
        OneOrManySortBy::Many(many_sort_by) => {
            if many_sort_by.is_empty() {
                let message =
                    "at least one sort by rule must be provided when explicitly setting `$sort`"
                        .to_string();
                let help =
                    "by default, lnx will sort results by `$score` in descending order"
                        .to_string();
                let error = context.build_error(
                    ErrorCode::MissingSortFields,
                    message,
                    Some(help),
                );
                return Err(error);
            }

            let mut seen = BTreeSet::new();
            for (idx, sort_by) in many_sort_by.iter().enumerate() {
                context.push_location(idx);
                context.push_location("$by");

                validate_sort_by(context, table_name, schema, sort_by)?;

                let did_insert = seen.insert(&sort_by.by);
                if !did_insert {
                    return Err(make_duplicate_field_error(
                        context,
                        table_name,
                        &sort_by.by,
                    ));
                }

                context.pop_location();
                context.pop_location();
            }
        },
    }
    Ok(())
}

fn validate_sort_by(
    context: &mut ValidatorContext,
    table_name: &str,
    schema: &tantivy::schema::Schema,
    sort_by: &SortBy,
) -> Result<(), ValidationError> {
    if sort_by.by == "$score" {
        return Ok(());
    }

    let field = schema.get_field(&sort_by.by).map_err(|_| {
        let mut fields = schema
            .fields()
            .map(|(_, entry)| entry.name())
            .collect::<Vec<_>>();
        fields.push("$score");
        make_field_does_not_exist_error(context, table_name, &sort_by.by, &fields)
    })?;

    let entry = schema.get_field_entry(field);
    if !entry.is_fast() {
        let help =
            "documents can only be sorted by columns that have a columnar index, \
            all fields are `columnar: true` by default"
                .to_string();
        let message = format!("field {:?} has no columnar index", entry.name());
        let error = context.build_error(ErrorCode::FieldIsNotFast, message, Some(help));
        return Err(error);
    }

    Ok(())
}

fn make_field_does_not_exist_error(
    context: &ValidatorContext,
    table_name: &str,
    field_name: &str,
    fields: &[&str],
) -> ValidationError {
    let help = fields
        .iter()
        .filter(|name| crate::utils::damerau_levenshtein(field_name, name) <= 3)
        .min_by_key(|name| crate::utils::damerau_levenshtein(field_name, name))
        .map(|closest| format!("did you mean {closest:?}?"))
        .unwrap_or_else(|| {
            "by default, lnx will sort by `$score` in descending order".to_string()
        });

    let message =
        format!("the field {field_name:?} does not exist within table {table_name:?}");
    context.build_error(ErrorCode::UnknownField, message, Some(help))
}

fn make_duplicate_field_error(
    context: &ValidatorContext,
    table_name: &str,
    field_name: &str,
) -> ValidationError {
    let help =
        "remove one of the duplicate fields from the sort by clause so a field is \
    only mentioned at most once in the sort by clause"
            .to_string();
    let message = format!(
        "the field {field_name:?} within table {table_name:?} is already used to sort"
    );
    context.build_error(ErrorCode::DuplicateField, message, Some(help))
}

#[cfg(test)]
mod tests {
    use tantivy::schema::{FAST, STORED};

    use super::*;
    use crate::syntax::Order;

    #[test]
    fn test_validate_sort_by_single_rule() {
        let mut schema_builder = tantivy::schema::Schema::builder();
        schema_builder.add_text_field("example", FAST);
        let schema = schema_builder.build();

        let mut context = ValidatorContext::default();
        validate_sort_fields(
            &mut context,
            "test",
            &schema,
            &OneOrManySortBy::One(SortBy {
                by: "example".to_string(),
                order: Order::Desc,
            }),
        )
        .expect("Validator should allow valid fields");
    }

    #[test]
    fn test_validate_sort_by_many_rule() {
        let mut schema_builder = tantivy::schema::Schema::builder();
        schema_builder.add_text_field("example", FAST);
        schema_builder.add_u64_field("age", FAST);
        let schema = schema_builder.build();

        let mut context = ValidatorContext::default();
        validate_sort_fields(
            &mut context,
            "test",
            &schema,
            &OneOrManySortBy::Many(vec![
                SortBy {
                    by: "age".to_string(),
                    order: Order::Desc,
                },
                SortBy {
                    by: "example".to_string(),
                    order: Order::Asc,
                },
            ]),
        )
        .expect("Validator should allow valid fields");

        let mut context = ValidatorContext::default();
        validate_sort_fields(
            &mut context,
            "test",
            &schema,
            &OneOrManySortBy::Many(vec![
                SortBy {
                    by: "$score".to_string(),
                    order: Order::Asc,
                },
                SortBy {
                    by: "age".to_string(),
                    order: Order::Desc,
                },
            ]),
        )
        .expect("Validator should allow valid fields");
    }

    #[test]
    fn test_validate_sort_by_rejects_empty_fields() {
        let mut schema_builder = tantivy::schema::Schema::builder();
        schema_builder.add_text_field("example", FAST);
        let schema = schema_builder.build();

        let mut context = ValidatorContext::default();
        let error = validate_sort_fields(
            &mut context,
            "test",
            &schema,
            &OneOrManySortBy::Many(vec![]),
        )
        .expect_err("Validator should reject empty fields");
        assert_eq!(error.code, ErrorCode::MissingSortFields);
        assert_eq!(
            error.help.as_deref(),
            Some("by default, lnx will sort results by `$score` in descending order"),
        );
        assert_eq!(
            error.message,
            "at least one sort by rule must be provided when explicitly setting `$sort`"
        );
        assert_eq!(error.location, "");
    }

    #[test]
    fn test_validate_sort_by_rejects_unknown_field() {
        let mut schema_builder = tantivy::schema::Schema::builder();
        schema_builder.add_text_field("example", FAST);
        schema_builder.add_text_field("age", FAST);
        let schema = schema_builder.build();

        let mut context = ValidatorContext::default();
        let error = validate_sort_fields(
            &mut context,
            "test",
            &schema,
            &OneOrManySortBy::Many(vec![
                SortBy {
                    by: "age".to_string(),
                    order: Order::Desc,
                },
                SortBy {
                    by: "exampl".to_string(),
                    order: Order::Asc,
                },
            ]),
        )
        .expect_err("Validator should reject unknown fields");
        assert_eq!(error.code, ErrorCode::UnknownField);
        assert_eq!(error.help.as_deref(), Some("did you mean \"example\"?"),);
        assert_eq!(
            error.message,
            "the field \"exampl\" does not exist within table \"test\""
        );
        assert_eq!(error.location, "[1].$by");

        let mut context = ValidatorContext::default();
        let error = validate_sort_fields(
            &mut context,
            "test",
            &schema,
            &OneOrManySortBy::One(SortBy {
                by: "lettuce".to_string(),
                order: Order::Desc,
            }),
        )
        .expect_err("Validator should reject unknown fields");
        assert_eq!(error.code, ErrorCode::UnknownField);
        assert_eq!(
            error.help.as_deref(),
            Some("by default, lnx will sort by `$score` in descending order"),
        );
        assert_eq!(
            error.message,
            "the field \"lettuce\" does not exist within table \"test\""
        );
        assert_eq!(error.location, "$by");
    }

    #[test]
    fn test_validate_sort_by_rejects_not_fast() {
        let mut schema_builder = tantivy::schema::Schema::builder();
        schema_builder.add_text_field("example", STORED);
        let schema = schema_builder.build();

        let mut context = ValidatorContext::default();
        let error = validate_sort_fields(
            &mut context,
            "test",
            &schema,
            &OneOrManySortBy::One(SortBy {
                by: "example".to_string(),
                order: Order::Desc,
            }),
        )
        .expect_err("Validator should reject field which isn't fast");
        assert_eq!(error.code, ErrorCode::FieldIsNotFast);
        assert_eq!(
            error.help.as_deref(),
            Some(
                "documents can only be sorted by columns that have a columnar index, \
            all fields are `columnar: true` by default"
            ),
        );
        assert_eq!(error.message, "field \"example\" has no columnar index");
        assert_eq!(error.location, "$by");
    }

    #[test]
    fn test_validate_sort_by_rejects_duplicates() {
        let mut schema_builder = tantivy::schema::Schema::builder();
        schema_builder.add_text_field("example", FAST);
        let schema = schema_builder.build();

        let mut context = ValidatorContext::default();
        let error = validate_sort_fields(
            &mut context,
            "test",
            &schema,
            &OneOrManySortBy::Many(vec![
                SortBy {
                    by: "example".to_string(),
                    order: Order::Asc,
                },
                SortBy {
                    by: "example".to_string(),
                    order: Order::Desc,
                },
            ]),
        )
        .expect_err("Validator should reject duplicate");
        assert_eq!(error.code, ErrorCode::DuplicateField);
        assert_eq!(
            error.help.as_deref(),
            Some("remove one of the duplicate fields from the sort by clause so a field is \
            only mentioned at most once in the sort by clause"),
        );
        assert_eq!(
            error.message,
            "the field \"example\" within table \"test\" is already used to sort"
        );
        assert_eq!(error.location, "[1].$by");
    }
}
