use std::sync::Arc;

use peer_cursor::{QueryExecutor, QueryOutput, Schema, SchemaRef};
use pgerror::PgError;
use pgwire::{
    api::results::{FieldFormat, FieldInfo},
    error::{PgWireError, PgWireResult},
};
use pt::peers::PostgresConfig;
use sqlparser::ast::Statement;
use tokio_postgres::Client;

mod stream;

// PostgresQueryExecutor is a QueryExecutor that uses a Postgres database as its
// backing store.
pub struct PostgresQueryExecutor {
    config: PostgresConfig,
    client: Box<Client>,
}

fn get_connection_string(config: &PostgresConfig) -> String {
    let mut connection_string = String::new();
    connection_string.push_str("host=");
    connection_string.push_str(&config.host);
    connection_string.push_str(" port=");
    connection_string.push_str(&config.port.to_string());
    connection_string.push_str(" user=");
    connection_string.push_str(&config.user);
    connection_string.push_str(" password=");
    connection_string.push_str(&config.password);
    connection_string.push_str(" dbname=");
    connection_string.push_str(&config.database);
    connection_string
}

impl PostgresQueryExecutor {
    pub async fn new(config: &PostgresConfig) -> anyhow::Result<Self> {
        let connection_string = get_connection_string(config);

        let (client, connection) =
            tokio_postgres::connect(&connection_string, tokio_postgres::NoTls)
                .await
                .map_err(|e| {
                    anyhow::anyhow!("error encountered while connecting to postgres {:?}", e)
                })?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                println!("connection error: {}", e)
            }
        });

        Ok(Self {
            config: config.clone(),
            client: Box::new(client),
        })
    }

    pub async fn schema_from_query(&self, query: &str) -> SchemaRef {
        let prepared = self
            .client
            .prepare_typed(query, &[])
            .await
            .expect("failed to prepare query");

        let fields: Vec<FieldInfo> = prepared
            .columns()
            .iter()
            .map(|c| {
                let name = c.name().to_string();
                FieldInfo::new(name, None, None, c.type_().clone(), FieldFormat::Text)
            })
            .collect();

        Arc::new(Schema { fields })
    }
}

#[async_trait::async_trait]
impl QueryExecutor for PostgresQueryExecutor {
    async fn execute(&self, stmt: &Statement) -> PgWireResult<QueryOutput> {
        let query_str = stmt.to_string();

        // if the query is a select statement, we need to fetch the rows
        // and return them as a QueryOutput::Stream, else we return the
        // number of affected rows.
        match stmt {
            Statement::Query(_query) => {
                // given that there could be a lot of rows returned, we
                // need to use a cursor to stream the rows back to the
                // client.
                let stream = self
                    .client
                    .query_raw(&query_str, std::iter::empty::<&str>())
                    .await
                    .map_err(|e| {
                        PgWireError::ApiError(Box::new(PgError::Internal {
                            err_msg: format!("error executing query: {}", e),
                        }))
                    })?;

                let schema = self.schema_from_query(&query_str).await;
                let cursor = stream::PgRecordStream::new(stream, schema);
                Ok(QueryOutput::Stream(Box::pin(cursor)))
            }
            _ => {
                let rows_affected = self.client.execute(&query_str, &[]).await.map_err(|e| {
                    PgWireError::ApiError(Box::new(PgError::Internal {
                        err_msg: format!("error executing query: {}", e),
                    }))
                })?;
                Ok(QueryOutput::AffectedRows(rows_affected as usize))
            }
        }
    }
}
