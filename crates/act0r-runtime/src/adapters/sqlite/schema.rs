//! Initialize the single supported schema. Database version changes require explicit migrations.
use super::{Error, SqliteConnection, storage};

const CURRENT_VERSION: i64 = 62;
const BASELINE: &str = include_str!("../../../schema/baseline.sql");

pub(super) async fn initialize(conn: &mut SqliteConnection) -> Result<(), Error> {
    // The caller holds BEGIN IMMEDIATE, including schema creation and the version marker.
    let version: i64 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(&mut *conn)
        .await
        .map_err(storage)?;
    match version {
        0 => {
            sqlx::raw_sql(BASELINE)
                .execute(conn)
                .await
                .map_err(storage)?;
        }
        CURRENT_VERSION => {}
        _ => {
            return Err(Error::Storage(format!(
                "unsupported database schema {version}; expected {CURRENT_VERSION}"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::sqlite::SqliteStore;
    use sqlx::Connection;

    #[tokio::test]
    async fn baseline_initializes_current_version_and_preserves_history_identity() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("baseline.db");
        let first = SqliteStore::open(&path).await.unwrap();
        let version: i64 = sqlx::query_scalar("PRAGMA user_version")
            .fetch_one(&first.pool)
            .await
            .unwrap();
        assert_eq!(version, CURRENT_VERSION);
        assert!(
            sqlx::query("PRAGMA foreign_key_check")
                .fetch_all(&first.pool)
                .await
                .unwrap()
                .is_empty()
        );
        let identity: String =
            sqlx::query_scalar("SELECT id FROM history_stream WHERE singleton=1")
                .fetch_one(&first.pool)
                .await
                .unwrap();
        first.close().await;
        let reopened = SqliteStore::open(&path).await.unwrap();
        let saved: String = sqlx::query_scalar("SELECT id FROM history_stream WHERE singleton=1")
            .fetch_one(&reopened.pool)
            .await
            .unwrap();
        assert_eq!(identity, saved);
        let other = SqliteStore::open(directory.path().join("other.db"))
            .await
            .unwrap();
        let different: String =
            sqlx::query_scalar("SELECT id FROM history_stream WHERE singleton=1")
                .fetch_one(&other.pool)
                .await
                .unwrap();
        assert_ne!(identity, different);
        reopened.close().await;
        other.close().await;
    }

    #[tokio::test]
    async fn unsupported_versions_are_rejected_without_rewriting_schema_or_data() {
        let directory = tempfile::tempdir().unwrap();
        for version in [1, CURRENT_VERSION - 1, CURRENT_VERSION + 1] {
            let path = directory.path().join(format!("version-{version}.db"));
            let options = sqlx::sqlite::SqliteConnectOptions::new()
                .filename(&path)
                .create_if_missing(true);
            let mut conn = SqliteConnection::connect_with(&options).await.unwrap();
            sqlx::raw_sql(
                "CREATE TABLE retained(value TEXT); INSERT INTO retained VALUES('keep this');",
            )
            .execute(&mut conn)
            .await
            .unwrap();
            // PRAGMA assignments cannot bind values; the version is a test-controlled integer.
            let pragma = format!("PRAGMA user_version={version}");
            sqlx::query(sqlx::AssertSqlSafe(pragma.as_str()))
                .execute(&mut conn)
                .await
                .unwrap();
            conn.close().await.unwrap();
            let result = SqliteStore::open(&path).await;
            assert!(
                matches!(result, Err(Error::Storage(message)) if message == format!("unsupported database schema {version}; expected {CURRENT_VERSION}"))
            );
            let mut conn = SqliteConnection::connect_with(&options).await.unwrap();
            let tables: Vec<String> = sqlx::query_scalar(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name",
            )
            .fetch_all(&mut conn)
            .await
            .unwrap();
            assert_eq!(tables, ["retained"]);
            let value: String = sqlx::query_scalar("SELECT value FROM retained")
                .fetch_one(&mut conn)
                .await
                .unwrap();
            assert_eq!(value, "keep this");
            let saved: i64 = sqlx::query_scalar("PRAGMA user_version")
                .fetch_one(&mut conn)
                .await
                .unwrap();
            assert_eq!(saved, version);
            conn.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn failed_initialization_rolls_back_new_tables_and_the_version_marker() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("conflict.db");
        let options = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(true);
        let mut conn = SqliteConnection::connect_with(&options).await.unwrap();
        sqlx::query("CREATE TABLE runs(sentinel TEXT)")
            .execute(&mut conn)
            .await
            .unwrap();
        conn.close().await.unwrap();
        assert!(SqliteStore::open(&path).await.is_err());
        let mut conn = SqliteConnection::connect_with(&options).await.unwrap();
        let tables: Vec<String> =
            sqlx::query_scalar("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
                .fetch_all(&mut conn)
                .await
                .unwrap();
        assert_eq!(tables, ["runs"]);
        let version: i64 = sqlx::query_scalar("PRAGMA user_version")
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_eq!(version, 0);
        conn.close().await.unwrap();
    }
}
