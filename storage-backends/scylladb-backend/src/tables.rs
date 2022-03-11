use itertools::Itertools;
use lnx_common::schema::INDEX_PK;
use scylla::transport::errors::QueryError;
use crate::helpers::format_column;

use super::connection::session;
use crate::index_store::{CHANGE_LOG_TABLE, DOCUMENT_TABLE, NODES_INFO_TABLE, SYNONYMS_TABLE, STOPWORDS_TABLE};

pub static INDEXES_TABLE: &str = "indexes";


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
        ks = lnx_common::configuration::SEARCH_ENGINE_CONFIGURATION_KEYSPACE,
        table = INDEXES_TABLE,
    );

    session().query_prepared(&query, &[]).await?;

    Ok(())
}

pub async fn create_meta_tables(ks: &str) -> Result<(), QueryError> {
    let queries = vec![
        format!(
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
        session().query_prepared(&query, &[]).await?;
    }

    Ok(())
}

pub async fn create_doc_tables(
    ks: &str,
    doc_fields: Vec<String>,
) -> Result<(), QueryError> {
    let fields = doc_fields
        .into_iter()
        .map(|v| format!("{} blob", format_column(v)))
        .join(", ");

    let queries = vec![
        format!(
            r#"CREATE TABLE IF NOT EXISTS {ks}.{table} (
                segment bigint,
                at timestamp,
                PRIMARY KEY ( segment, at )
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
            pk = INDEX_PK,
            fields = fields,
        ),
    ];

    for query in queries {
        session().query_prepared(&query, &[]).await?;
    }

    Ok(())
}