use std::borrow::Cow;
use hashbrown::HashSet;
use std::path::Path;
use std::time::Instant;

use futures_util::StreamExt;
use anyhow::Result;
use itertools::Itertools;
use scylla::frame::value::ValueList;
use scylla::IntoTypedRows;
use scylla::routing::murmur3_token;
use lnx_common::index::context::IndexContext;
use lnx_common::types::document::{DocId, TypeSafeDocument};
use lnx_storage::async_trait;
use lnx_storage::templates::change_log::{
    ChangeLogEntry,
    ChangeLogIterator,
    ChangeLogStore,
};
use tokio::sync::mpsc;
use lnx_common::configuration::NUM_SEGMENTS;
use lnx_common::schema::{INDEX_PK, SEGMENT_KEY};
use lnx_storage::templates::doc_store::{DocStore, DocumentIterator, DocumentUpdate};
use lnx_storage::templates::meta_store::{MetaStore, Synonyms};
use lnx_storage::types::{SegmentId, Timestamp};
use crate::connection::session;
use crate::helpers::doc::ScyllaSafeDocument;
use crate::helpers::format_column;

use crate::ReplicationInfo;

pub static DOCUMENT_TABLE: &str = "documents";
pub static CHANGE_LOG_TABLE: &str = "index_changelog";
pub static SYNONYMS_TABLE: &str = "synonyms";
pub static STOPWORDS_TABLE: &str = "stop_words";
pub static NODES_INFO_TABLE: &str = "stop_words";

macro_rules! send_chunk {
    ($chunk:expr, $tx:expr) => {
        if $tx.send($chunk.clone()).await.is_err() {
            trace!("chunk send failed, cleaning up and shutting down task");
            break;
        }

        $chunk.clear();
    };
}

pub struct ScyllaIndexStore {
    ctx: IndexContext,
    fields: Cow<'static, Vec<String>>,
    insert_columns: String,
    placeholders_columns: String,
}

impl ScyllaIndexStore {
    pub async fn setup(
        ctx: IndexContext,
        default_replication: ReplicationInfo,
    ) -> Result<Self> {
        let replication_info: ReplicationInfo = match ctx.storage_config().cloned() {
            None => default_replication,
            Some(cfg) => serde_json::from_value(cfg).unwrap_or(default_replication),
        };

        replication_info.build_index_keyspace(&ctx).await?;

        let raw_fields = ctx.schema()
            .fields()
            .keys()
            .map(|v| v.to_string())
            .collect::<Vec<String>>();

        let insert_columns = raw_fields.iter().map(format_column).join(", ");
        let placeholders_columns = raw_fields.iter().map(|_| "?").join(", ");
        let fields = Cow::Owned(raw_fields.clone());

        super::tables::create_doc_tables(ctx.keyspace(), raw_fields).await?;
        super::tables::create_meta_tables(ctx.keyspace()).await?;

        Ok(Self { ctx, fields, insert_columns, placeholders_columns })
    }
}

#[async_trait]
impl DocStore for ScyllaIndexStore {
    async fn add_documents(
        &self,
        docs: &[(DocId, TypeSafeDocument)],
    ) -> Result<HashSet<SegmentId>> {
        let query = format!(
            "INSERT INTO {ks}.{table} ({pk}, {columns}) VALUES (?, {placeholders})",
            ks = self.ctx.keyspace(),
            pk = INDEX_PK,
            table = DOCUMENT_TABLE,
            columns = self.insert_columns,
            placeholders = self.placeholders_columns,
        );

        let prepared = session().prepare(&query).await?;
        let session = session().as_ref();

        let mut tokens = HashSet::new();
        for (doc_id, doc) in docs {
            let doc = ScyllaSafeDocument(*doc_id, doc);
            session.execute(&prepared, &doc).await?;

            let serialized_pk = (doc_id,).serialized()?.into_owned(); // TODO does this work? Or do we need the entire `doc`
            let token =
                murmur3_token(prepared.compute_partition_key(&serialized_pk)?).value;
            tokens.insert(token % NUM_SEGMENTS);
        }

        Ok(tokens)
    }

    async fn update_documents(&self, docs: &[DocumentUpdate]) -> Result<HashSet<SegmentId>> {
        todo!()
    }

    async fn remove_documents(&self, docs: &[DocId]) -> Result<HashSet<SegmentId>> {
        let query = format!(
            "DELETE FROM {ks}.{table} WHERE {pk} = ?",
            ks = self.ctx.keyspace(),
            table = DOCUMENT_TABLE,
            pk = INDEX_PK,
        );

        let prepared = session().prepare(&query).await?;
        let session = session().as_ref();

        let mut tokens = HashSet::new();
        for doc_id in docs {
            session.execute(&prepared, (doc_id,)).await?;

            let serialized_pk = (doc_id,).serialized()?.into_owned();
            let token =
                murmur3_token(prepared.compute_partition_key(&serialized_pk)?).value;
            tokens.insert(token % NUM_SEGMENTS);
        }

        Ok(tokens)
    }

    async fn clear_documents(&self) -> Result<()> {
        let query = format!(
            "DELETE FROM {ks}.{table};",
            ks = self.ctx.keyspace(),
            table = DOCUMENT_TABLE,
        );

        session().query_prepared(&query, &[]).await?;

        // TODO: mark in log

        Ok(())
    }

    async fn fetch_document(
        &self,
        fields: Option<Vec<String>>,
        docs: DocId,
    ) -> Result<Option<(DocId, SegmentId, TypeSafeDocument)>> {
        let columns = fields
            .clone()
            .map(|v| v.join(", "))
            .unwrap_or_else(|| self.insert_columns.clone());

        let query = format!(
            "SELECT {pk}, {segment}, {columns} FROM {ks}.{table} WHERE {pk} = ?;",
            pk = INDEX_PK,
            segment = SEGMENT_KEY,
            columns = columns,
            ks = self.ctx.keyspace(),
            table = DOCUMENT_TABLE,
        );

        let results = session()
            .query_prepared(query.as_str(), (docs,)) // TODO: Can we optimise this?
            .await?;

        let rows = results
            .rows
            .unwrap_or_default()
            .into_iter()
            .next()
            .map(|r| ScyllaSafeDocument::from_row_and_layout(r, fields.as_ref().unwrap_or(&self.fields)))
            .transpose()?;

        Ok(rows)
    }

    async fn iter_documents(
        &self,
        fields: Option<Vec<String>>,
        chunk_size: usize,
        segment_id: Option<SegmentId>,
    ) -> Result<DocumentIterator> {
        let columns = fields
            .clone()
            .map(|v| v.join(", "))
            .unwrap_or_else(|| self.insert_columns.clone());

        let query = format!(
            "SELECT {pk}, token({pk}), {columns} FROM {ks}.{table} {segment_filter};",
            pk = INDEX_PK,
            columns = columns,
            table = DOCUMENT_TABLE,
            ks = self.ctx.keyspace(),
            segment_filter = if segment_id.is_some() {
                format!("WHERE token({pk}) >= ? AND token({pk}) <= ?", pk = INDEX_PK)
            } else {
                "".to_string()
            }
        );

        let mut iter = if let Some(segment) = segment_id {
            let params = super::helpers::segments::get_range(segment);
            session().query_iter(query.as_str(), params).await?
        } else {
            session().query_iter(query.as_str(), &[]).await?
        };

        let default_fields = self.fields.clone();
        let (tx, rx) = mpsc::channel(1);
        let handle = tokio::spawn(async move {
            let mut chunk = Vec::with_capacity(chunk_size);

            while let Some(Ok(row)) = iter.next().await {
                let layout = fields.as_ref().unwrap_or(&default_fields);
                let res = ScyllaSafeDocument::from_row_and_layout(row, layout);

                let row = match res {
                    Err(e) => {
                        error!("failed to handle chunk due to error {:?}", e);
                        return;
                    },
                    Ok(row) => row,
                };

                chunk.push(row);
                if chunk.len() >= chunk_size {
                    send_chunk!(chunk, tx);
                }
            }

            if !chunk.is_empty() && tx.send(chunk).await.is_err() {
                trace!("chunk send failed, cleaning up and shutting down task");
            }
        });

        let iterator = DocumentIterator::from_rx_and_handle(rx, handle);

        Ok(iterator)
    }
}

#[async_trait]
impl ChangeLogStore for ScyllaIndexStore {
    async fn append_changes(&self, logs: ChangeLogEntry) -> Result<()> {
        let insert_query = format!(
            "INSERT INTO {ks}.{table} (segment, at) VALUES (?, ?);",
            ks = self.ctx.keyspace(),
            table = CHANGE_LOG_TABLE,
        );
        let delete_query = format!(
            "DELETE FROM {ks}.{table} WHERE segment = ?;",
            ks = self.ctx.keyspace(),
            table = CHANGE_LOG_TABLE,
        );

        session()
            .query_prepared(&delete_query, (logs.affected_segment,))
            .await?;

        session()
            .query_prepared(
                &insert_query,
                (logs.affected_segment, logs.timestamp),
            )
            .await?;

        trace!("Update registered with change log.");

        Ok(())
    }

    async fn get_pending_changes(&self, from: Timestamp) -> Result<ChangeLogIterator> {
        let query = format!(
            "SELECT segment, at FROM {ks}.{table} WHERE at >= ?;",
            ks = self.ctx.keyspace(),
            table = CHANGE_LOG_TABLE,
        );

        let start = Instant::now();
        let iter = session()
            .query_iter(query.as_str(), (from,))
            .await?;
        info!("Preparing iterator took {:?} to execute.", start.elapsed());

        type ChangedRow = (SegmentId, Timestamp);
        let (tx, rx) = mpsc::channel(2);
        let handle = tokio::spawn(async move {
            let mut typed_iter = iter.into_typed::<ChangedRow>();
            while let Some(Ok(row)) = typed_iter.next().await {
                let change = ChangeLogEntry {
                    affected_segment: row.0,
                    timestamp: row.1,
                };

                if tx.send(change).await.is_err() {
                    trace!("chunk send failed, cleaning up and shutting down task");
                    break;
                }
            }
        });

        let iterator = ChangeLogIterator::from_rx_and_handle(rx, handle);

        Ok(iterator)
    }

    async fn count_pending_changes(&self, from: Timestamp) -> Result<usize> {
        let query = format!(
            "SELECT COUNT (segment) FROM {ks}.{table} WHERE at >= ?;",
            ks = self.ctx.keyspace(),
            table = CHANGE_LOG_TABLE,
        );

        let row = session()
            .query_prepared(&query, (from,))
            .await?
            .rows
            .unwrap_or_default()
            .into_typed::<(i64,)>()
            .next();

        if let Some(Ok((count,))) = row {
            Ok(count as usize)
        } else {
            Ok(0)
        }
    }
}

#[async_trait]
impl MetaStore for ScyllaIndexStore {
    async fn add_stopwords(&self, words: Vec<String>) -> Result<()> {
        todo!()
    }

    async fn remove_stopwords(&self, words: Vec<String>) -> Result<()> {
        todo!()
    }

    async fn fetch_stopwords(&self) -> Result<Vec<String>> {
        todo!()
    }

    async fn add_synonyms(&self, words: Vec<Synonyms>) -> Result<()> {
        todo!()
    }

    async fn remove_synonyms(&self, words: Vec<String>) -> Result<()> {
        todo!()
    }

    async fn fetch_synonyms(&self) -> Result<Vec<Synonyms>> {
        todo!()
    }

    async fn set_update_timestamp(&self, timestamp: Timestamp) -> Result<()> {
        todo!()
    }

    async fn get_last_update_timestamp(&self) -> Result<Option<Timestamp>> {
        todo!()
    }

    async fn load_index_from_peer(&self, out_dir: &Path) -> Result<()> {
        todo!()
    }

    async fn update_settings(&self, key: &str, data: Vec<u8>) -> Result<()> {
        todo!()
    }

    async fn remove_settings(&self, key: &str) -> Result<()> {
        todo!()
    }

    async fn load_settings(&self, key: &str) -> Result<Option<Vec<u8>>> {
        todo!()
    }
}
