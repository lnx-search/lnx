use std::time::Instant;

use anyhow::{anyhow, Result};
use lnx_common::schema::{FieldName, INDEX_PK, SEGMENT_KEY};
use lnx_common::types::document::{DocField, DocId, Document, TypeSafeDocument};
use lnx_common::types::Value;
use lnx_storage::types::SegmentId;
use tantivy::schema::{Field, FieldEntry, FieldType, Schema};
use tantivy::{IndexWriter, Term};
use tokio::sync::mpsc;

/// A index task.
pub(crate) enum Task {
    /// Adds a set of documents to the index.
    ///
    /// Only fields which are actually indexed are added.
    AddDocuments(Vec<(DocId, SegmentId, TypeSafeDocument)>),

    /// Removes a set of documents from the index.
    RemoveSegment(SegmentId),

    /// Clears all documents from the index.
    ClearAllDocuments,
}

#[instrument(name = "indexer", skip_all)]
/// Starts listening for tasks to perform operations with the given index writer.
///
/// This only commits when all tasks are complete and is designed to be non-continuous.
pub(crate) fn start_indexing(
    schema: Schema,
    mut writer: IndexWriter,
    mut tasks: mpsc::Receiver<Task>,
) -> Result<()> {
    let pk_field = schema.get_field(INDEX_PK).ok_or_else(|| {
        anyhow!(
            "expected index primary key to exist, index is corrupted. (This is a bug.)"
        )
    })?;

    let segment_id_field = schema.get_field(SEGMENT_KEY).ok_or_else(|| {
        anyhow!(
            "expected index segment key to exist, index is corrupted. (This is a bug.)"
        )
    })?;

    while let Some(task) = tasks.blocking_recv() {
        handle_task(pk_field, segment_id_field, &schema, &mut writer, task)?;
    }

    info!("Indexer has finished current tasks, preparing commit...");
    let start = Instant::now();
    writer.commit()?;
    info!("Commit complete took {:?}", start.elapsed());

    info!("Waiting for merge threads to finish...");
    let start = Instant::now();
    writer.wait_merging_threads()?;
    info!("Merge threads took {:?}", start.elapsed());

    info!("Indexing complete! We are now up to date.");

    Ok(())
}

fn handle_task(
    pk_field: Field,
    segment_id_field: Field,
    schema: &Schema,
    writer: &mut IndexWriter,
    task: Task,
) -> Result<()> {
    match task {
        Task::ClearAllDocuments => {
            writer.delete_all_documents()?;
        },
        Task::RemoveSegment(segment) => {
            let term = Term::from_field_i64(segment_id_field, segment);
            writer.delete_term(term);
        },
        Task::AddDocuments(docs) => {
            for (doc_id, segment, doc) in docs {
                let doc = process_document(
                    pk_field,
                    segment_id_field,
                    schema,
                    doc_id,
                    segment,
                    doc,
                )?;
                writer.add_document(doc)?;
            }
        },
    }

    Ok(())
}

fn process_document(
    pk_field: Field,
    segment_id_field: Field,
    schema: &Schema,
    doc_id: DocId,
    segment_id: SegmentId,
    doc: TypeSafeDocument,
) -> Result<tantivy::Document> {
    trace!("Adding document {:?}", &doc);

    let mut document = tantivy::Document::new();

    for (field_name, value) in doc.0 {
        let field = match schema.get_field(&field_name) {
            None => continue,
            Some(f) => f,
        };
        let entry = schema.get_field_entry(field);

        if !entry.is_indexed() {
            continue;
        }

        match value {
            DocField::Empty => continue,
            DocField::Single(value) => add_field(&mut document, entry, field, value)?,
            DocField::Multi(values) => {
                for value in values {
                    add_field(&mut document, entry, field, value)?;
                }
            },
        }
    }

    document.add_bytes(pk_field, doc_id.as_bytes().as_slice());
    document.add_i64(segment_id_field, segment_id);

    Ok(document)
}

fn add_field(
    doc: &mut tantivy::Document,
    entry: &FieldEntry,
    field: Field,
    value: Value,
) -> Result<()> {
    use tantivy::schema;

    let val = match entry.field_type() {
        FieldType::Str(_) => schema::Value::Str(value.try_into()?),
        FieldType::U64(_) => schema::Value::U64(value.try_into()?),
        FieldType::I64(_) => schema::Value::I64(value.try_into()?),
        FieldType::F64(_) => schema::Value::F64(value.try_into()?),
        FieldType::Date(_) => schema::Value::Date(value.try_into()?),
        FieldType::Facet(_) => schema::Value::Facet(value.try_into()?),
        FieldType::Bytes(_) => schema::Value::Bytes(value.try_into()?),
        FieldType::JsonObject(_) => schema::Value::JsonObject(value.try_into()?),
    };

    doc.add_field_value(field, val);

    Ok(())
}
