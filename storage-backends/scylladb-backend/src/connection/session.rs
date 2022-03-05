use std::fmt::{Debug, Display};

use scylla::frame::value::ValueList;
use scylla::prepared_statement::PreparedStatement;
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

impl AsRef<scylla::Session> for Session {
    fn as_ref(&self) -> &scylla::Session {
        &self.0.session
    }
}

impl Session {
    #[instrument(skip(self, query), level = "debug")]
    pub async fn prepare(&self, query: &str) -> Result<PreparedStatement, QueryError> {
        let query = Query::from(query);
        self.0.add_prepared_statement(&query).await
    }

    #[instrument(skip(self, query), level = "debug")]
    pub async fn query(
        &self,
        query: &str,
        values: impl ValueList + Debug,
    ) -> Result<QueryResult, QueryError> {
        debug!("executing query {}", query);
        let result = self.0.execute(query, &values).await;

        if let Err(ref e) = result {
            consider_logging_error(e);
        }

        result
    }

    #[instrument(skip(self, query), level = "debug")]
    pub async fn query_iter(
        &self,
        query: impl Into<Query> + Display,
        values: impl ValueList + Debug,
    ) -> Result<RowIterator, QueryError> {
        debug!("preparing new iterator statement: {}", query);
        let mut query: Query = query.into();
        query.set_page_size(2500);

        let result = self.0.execute_iter(query, &values).await;

        if let Err(e) = result {
            consider_logging_error(&e);
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

        match result {
            Ok(res) => Ok(res),
            Err(e) => {
                consider_logging_error(&e);
                Err(e)
            },
        }
    }
}

fn consider_logging_error(e: &QueryError) {
    if let QueryError::DbError(DbError::AlreadyExists { .. }, ..) = e {
        info!("Keyspace already exists, skipping...");
    }
}
