use std::env::temp_dir;

use tantivy::schema::{Field, Schema, STORED, TEXT};

use crate::{AppContext, AppContextInner, IndexContext, IndexContextInner};

pub fn test_index_ctx() -> (IndexContext, Field, Field) {
    let mut schema_builder = Schema::builder();

    let title = schema_builder.add_text_field("title", TEXT | STORED);
    let body = schema_builder.add_text_field("body", TEXT);

    let schema = schema_builder.build();

    (
        IndexContext::from(IndexContextInner {
            name: "text-index".to_string(),
            schema,
        }),
        title,
        body,
    )
}

pub fn test_app_ctx() -> AppContext {
    AppContext::from(AppContextInner {
        tmp_path: temp_dir(),
    })
}
