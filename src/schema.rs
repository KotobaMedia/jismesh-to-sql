use anyhow::{Result, anyhow};

const INIT_SQL: &str = include_str!("./schema.sql");

pub async fn init_schema(client: &tokio_postgres::Client) -> Result<()> {
    client
        .batch_execute(INIT_SQL)
        .await
        .map_err(|e| anyhow!("Failed to create tables: {}", e))?;

    Ok(())
}
