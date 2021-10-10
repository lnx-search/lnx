use std::collections::BTreeMap;
use serde::Deserialize;
use anyhow::{Error, Result};
use engine::{QueryPayload, DocumentId};
use engine::structures::{DocumentOptions, DocumentValueOptions, IndexDeclaration};

use crate::{check_error, INDEX_KEYSPACE};
use crate::responders::json_response;


#[derive(Deserialize)]
struct IndexCreationPayload {
    #[serde(default)]
    override_if_exists: bool,
    index: IndexDeclaration,
}

