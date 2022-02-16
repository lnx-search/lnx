use std::fmt::Debug;

use scylla::frame::value::ValueList;
use scylla::query::Query;
use scylla::transport::errors::{DbError, QueryError};
use scylla::transport::iterator::RowIterator;
use scylla::QueryResult;

pub struct Session(scylla::CachingSession);

impl From<scylla::Session> for Session {
    fn from(s: scylla::Session) -> Self {
        Self(scylla::CachingSession::from(s, 100))
    }
}

impl Session {
    #[instrument(skip(self, query), level = "debug")]
    pub async fn query(
        &self,
        query: &str,
        values: impl ValueList + Debug,
    ) -> Result<QueryResult, QueryError> {
        debug!("executing query {}", query);
        let result = self.0.execute(query, &values).await;

        if let Err(ref e) = result {
            consider_logging_error(query, e);
        }

        result
    }

    #[instrument(skip(self, query), level = "debug")]
    pub async fn query_iter(
        &self,
        query: &str,
        values: impl ValueList + Debug,
    ) -> Result<RowIterator, QueryError> {
        debug!("preparing and paging new statement: {}", query);
        let result = self.0.execute_iter(Query::from(query), &values).await;

        if let Err(e) = result {
            consider_logging_error(query, &e);
            return Err(e);
        }

        result
    }

    #[instrument(skip(self, query), level = "debug")]
    pub async fn query_prepared(
        &self,
        query: &str,
        values: impl ValueList + Debug,
    ) -> Result<QueryResult, QueryError> {
        debug!("preparing new statement: {}", query);
        let result = self.0.execute(Query::from(query), &values).await;

        if let Err(e) = result {
            consider_logging_error(query, &e);
            return Err(e);
        }

        result
    }
}

fn consider_logging_error(query: &str, e: &QueryError) {
    match e {
        QueryError::DbError(DbError::AlreadyExists { .. }, ..) => {
            info!("Keyspace already exists, skipping...");
        },
        other => {
            error!(
                "failed to execute statement: {} due to error: {:?}",
                query, other
            );
        },
    }
}
