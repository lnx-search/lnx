use anyhow::{anyhow, Result};
use lnx_common::schema::{FieldName, INDEX_PK};
use lnx_common::types::document::{DocField, Document};
use lnx_common::types::Value;
use lnx_storage::DocId;
use tantivy::schema::{Field, FieldEntry, FieldType, FieldValue, Schema};
use tantivy::{IndexWriter, Term};
use tokio::sync::mpsc;

/// A index task.
pub(crate) enum Task {
    /// Adds a set of documents to the index.
    ///
    /// Only fields which are actually indexed are added.
    AddDocuments(Vec<(DocId, Document)>),

    /// Removes a set of documents from the index.
    RemoveDocs(Vec<DocId>),

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

    while let Some(task) = tasks.blocking_recv() {
        handle_task(pk_field, &schema, &mut writer, task)?;
    }

    info!("Indexer has finished current tasks, preparing commit...");
    writer.commit()?;

    info!("Commit complete, waiting for merge threads to finish...");
    writer.wait_merging_threads()?;

    info!("Indexing complete! We are now up to date.");

    Ok(())
}

fn handle_task(
    pk_field: Field,
    schema: &Schema,
    writer: &mut IndexWriter,
    task: Task,
) -> Result<()> {
    match task {
        Task::ClearAllDocuments => {
            writer.delete_all_documents()?;
        },
        Task::RemoveDocs(doc_ids) => {
            for doc_id in doc_ids {
                let term =
                    Term::from_field_bytes(pk_field, doc_id.as_bytes().as_slice());
                writer.delete_term(term);
            }
        },
        Task::AddDocuments(docs) => {
            let fields: Vec<(Field, FieldName, &FieldEntry)> = schema
                .fields()
                .map(|(f, e)| (f, FieldName(e.name().to_string()), e))
                .collect();

            for (doc_id, doc) in docs {
                let doc = process_document(pk_field, &fields, doc_id, doc)?;
                writer.add_document(doc)?;
            }
        },
    }

    Ok(())
}

fn process_document(
    pk_field: Field,
    fields: &[(Field, FieldName, &FieldEntry)],
    doc_id: DocId,
    mut doc: Document,
) -> Result<tantivy::Document> {
    trace!("Adding document {:?}", &doc);

    let mut document = tantivy::Document::new();

    for (field, name, entry) in fields.iter() {
        let doc_field = match doc.0.remove(name) {
            Some(v) => v,
            None => continue,
        };

        if !entry.is_indexed() {
            continue;
        }

        match doc_field {
            DocField::Empty => continue,
            DocField::Single(value) => add_field(&mut document, entry, *field, value)?,
            DocField::Multi(values) => {
                for value in values {
                    add_field(&mut document, entry, *field, value)?;
                }
            },
        }
    }

    document.add_bytes(pk_field, doc_id.as_bytes().as_slice());

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
    };

    let fv = FieldValue::new(field, val);
    doc.add(fv);

    Ok(())
}
