use lnx_common::table::LnxTable;

use crate::validate::{ErrorCode, ValidationError, ValidatorContext};

/// Validates the fields that are attempting to be selected by the query.
pub fn validate_select_fields(
    context: &mut ValidatorContext,
    table_name: &str,
    table: &LnxTable,
    fields: &[String],
) -> Result<(), ValidationError> {
    if fields.is_empty() {
        let message =
            "at least one field or wildcard must be provided to the `$select` field"
                .to_string();
        let help = "you can use a wildcard to select all fields: `$select: [\"*\"]` "
            .to_string();
        let error =
            context.build_error(ErrorCode::MissingSelectFields, message, Some(help));
        return Err(error);
    }

    let schema = table.tantivy_schema();
    for (idx, field_name) in fields.iter().enumerate() {
        context.push_location(idx);

        let field = schema.get_field(field_name).map_err(|_| {
            make_field_does_not_exist_error(context, table_name, field_name, fields)
        })?;

        let entry = schema.get_field_entry(field);
        if !entry.is_stored() {
            return Err(make_field_not_stored_error(context, table_name, field_name));
        }

        context.pop_location();
    }

    Ok(())
}

fn make_field_does_not_exist_error(
    context: &ValidatorContext,
    table_name: &str,
    field_name: &str,
    fields: &[String],
) -> ValidationError {
    let help = fields
        .iter()
        .min_by_key(|name| crate::utils::damerau_levenshtein(field_name, name))
        .map(|closest| format!("did you mean {closest:?}?"))
        .unwrap_or_else(|| "you can use a wildcard `*` to select all fields and see what fields are available".to_string());
    let message =
        format!("the field {field_name:?} does not exist within table {table_name:?}");
    context.build_error(ErrorCode::UnknownField, message, Some(help))
}

fn make_field_not_stored_error(
    context: &ValidatorContext,
    table_name: &str,
    field_name: &str,
) -> ValidationError {
    let help =
        "fields are always stored by default unless explicitly disabled, to re-enable \
     storing after it has been disabled requires deleting and re-creating the table"
            .to_string();
    let message =
        format!("the field {field_name:?} within table {table_name:?} is not stored");
    context.build_error(ErrorCode::FieldIsNotStored, message, Some(help))
}
