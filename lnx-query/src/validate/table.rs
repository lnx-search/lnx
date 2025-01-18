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
