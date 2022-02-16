use itertools::Itertools;
use scylla::transport::errors::QueryError;

use super::connection::session;
use super::engine_store::{INDEXES_TABLE, self};
use super::meta_store::{STOPWORDS_TABLE, SYNONYMS_TABLE, NODES_INFO_TABLE};
use super::primary_store::{DOCUMENT_TABLE, CHANGE_LOG_TABLE};
use super::doc_wrapper::DOCUMENT_PRIMARY_KEY;

pub fn format_column(v: impl AsRef<str>) -> String {
    format!("field_{}", v.as_ref())
}

pub async fn create_indexes_table() -> Result<(), QueryError> {
    let query = format!(
        r#"CREATE TABLE IF NOT EXISTS {ks}.{table} (
            name text,
            index_schema blob,
            polling_mode blob,
            replication blob,
            settings map<text, blob>,
            PRIMARY KEY ( name )
        );
        "#,
        ks = engine_store::KEYSPACE,
        table = INDEXES_TABLE,
    );

    session()
        .query_prepared(&query, &[])
        .await?;

    Ok(())
}

pub async fn create_meta_tables(ks: &str) -> Result<(), QueryError> {
    let queries = vec![format!(
            r#"CREATE TABLE IF NOT EXISTS {ks}.{table} (
                word text,
                PRIMARY KEY ( word )
            );
            "#,
            ks = ks,
            table = STOPWORDS_TABLE,
        ),
        format!(
            r#"CREATE TABLE IF NOT EXISTS {ks}.{table} (
                word text,
                synonyms set<text>,
                PRIMARY KEY ( word )
            );
            "#,
            ks = ks,
            table = SYNONYMS_TABLE,
        ),
        format!(
            r#"CREATE TABLE IF NOT EXISTS {ks}.{table} (
                node_id uuid,
                last_updated timestamp,
                last_heartbeat timestamp,
                PRIMARY KEY ( node_id )
            );
            "#,
            ks = ks,
            table = NODES_INFO_TABLE,
        ),
    ];

    for query in queries {
        session()
            .query_prepared(&query, &[])
            .await?;
    }

    Ok(())
}

pub async fn create_doc_tables(ks: &str, doc_fields: Vec<String>) -> Result<(), QueryError> {
    let fields = doc_fields
        .into_iter()
        .map(|v| format!("{} blob", format_column(v)))
        .join(", ");

    let queries = vec![
        format!(
            r#"CREATE TABLE IF NOT EXISTS {ks}.{table} (
                id uuid,
                kind tinyint,
                affected_docs set<uuid>,
                at timestamp,
                PRIMARY KEY ( id, at )
            );
            "#,
            ks = ks,
            table = CHANGE_LOG_TABLE,
        ),
        format!(
            r#"CREATE TABLE IF NOT EXISTS {ks}.{table} (
                {pk} uuid,
                {fields},
                PRIMARY KEY ( {pk} )
            );
            "#,
            ks = ks,
            table = DOCUMENT_TABLE,
            pk = DOCUMENT_PRIMARY_KEY,
            fields = fields,
        ),
    ];

    for query in queries {
        session()
            .query_prepared(&query, &[])
            .await?;
    }

    Ok(())
}
