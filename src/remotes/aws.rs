use aws_config::{BehaviorVersion, Region};
use aws_credential_types::Credentials;
use aws_sdk_s3::{Client, Error as S3Error};
use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension, Result, params};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use zeroize::{Zeroize, ZeroizeOnDrop};

#[derive(Clone, Debug, PartialEq)]
pub struct S3Object {
    pub key: String,
    pub etag: String,
    pub size: i64,
    pub last_modified: DateTime<Utc>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct CompactS3Object {
    etag: String,
    size: u64,
    last_modified: i64,
}

#[derive(Debug, PartialEq)]
pub enum Change {
    Added(S3Object),
    Modified { old: S3Object, new: S3Object },
    Deleted(S3Object),
}

#[derive(Debug, Default, PartialEq)]
pub struct DiffStats {
    pub added: u64,
    pub modified: u64,
    pub deleted: u64,
    pub unchanged: u64,
}

#[derive(Zeroize, ZeroizeOnDrop)]
pub struct S3Conf {
    #[zeroize(skip)]
    pub bucket: String,
    #[zeroize(skip)]
    pub region: String,
    pub access_key_id: SecretString,
    pub secret_access_key: SecretString,
    #[zeroize(skip)]
    pub endpoint_url: Option<String>,
}

impl S3Conf {
    pub fn new(
        bucket: String,
        access_key_id: String,
        secret_access_key: String,
        region: String,
    ) -> Self {
        S3Conf {
            bucket,
            access_key_id: SecretString::new(access_key_id.into()),
            secret_access_key: SecretString::new(secret_access_key.into()),
            region,
            endpoint_url: None,
        }
    }

    pub fn new_with_endpoint(
        bucket: String,
        access_key_id: String,
        secret_access_key: String,
        region: String,
        endpoint_url: String,
    ) -> Self {
        S3Conf {
            bucket,
            access_key_id: SecretString::new(access_key_id.into()),
            secret_access_key: SecretString::new(secret_access_key.into()),
            region,
            endpoint_url: Some(endpoint_url),
        }
    }

    fn get_access_key_id(&self) -> &str {
        self.access_key_id.expose_secret()
    }

    fn get_secret_access_key(&self) -> &str {
        self.secret_access_key.expose_secret()
    }
}

pub struct S3 {
    client: Client,
    bucket: String,
}

impl S3 {
    pub async fn new(conf: &S3Conf) -> Self {
        Self::create_client(conf).await
    }

    pub fn from_client(client: Client, bucket: String) -> Self {
        Self { client, bucket }
    }

    async fn create_client(conf: &S3Conf) -> Self {
        let credentials = Credentials::new(
            conf.get_access_key_id(),
            conf.get_secret_access_key(),
            None,
            None,
            "resy",
        );

        let region = Region::new(conf.region.clone());

        let mut config_builder = aws_config::defaults(BehaviorVersion::latest())
            .region(region)
            .credentials_provider(credentials);

        // ideally we should only use endpoint_url for local testing. We may want to add a flag to disable it in prod.
        if let Some(ref endpoint) = conf.endpoint_url {
            config_builder = config_builder.endpoint_url(endpoint);
        }

        let config = config_builder.load().await;
        let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&config);

        // only for local testing
        if conf.endpoint_url.is_some() {
            s3_config_builder = s3_config_builder.force_path_style(true);
        }

        let s3_config = s3_config_builder.build();

        Self {
            client: Client::from_conf(s3_config),
            bucket: conf.bucket.clone(),
        }
    }

    async fn stream_objects<F>(&self, mut processor: F) -> Result<(), S3Error>
    where
        F: FnMut(S3Object) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
    {
        let mut continuation_token: Option<String> = None;
        let mut total_processed = 0;

        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .max_keys(1000);

            if let Some(token) = continuation_token.take() {
                request = request.continuation_token(token);
            }

            let response = request.send().await?;

            let contents = response.contents();
            if !contents.is_empty() {
                for obj in contents {
                    if let (Some(key), Some(etag), Some(size), Some(last_modified)) =
                        (obj.key(), obj.e_tag(), obj.size(), obj.last_modified())
                    {
                        let s3_object = S3Object {
                            key: key.to_string(),
                            etag: etag.to_string(),
                            size,
                            last_modified: DateTime::from_timestamp(
                                last_modified.secs(),
                                last_modified.subsec_nanos(),
                            )
                            .unwrap_or_default()
                            .with_timezone(&Utc),
                        };

                        if let Err(e) = processor(s3_object) {
                            eprintln!("Error processing object: {}", e);
                        }

                        total_processed += 1;
                    }
                }
            }

            if total_processed % 10_000 == 0 {
                println!("Processed {} objects", total_processed);
            }

            if response.is_truncated().unwrap_or(false) {
                continuation_token = response.next_continuation_token().map(|s| s.to_string());
            } else {
                break;
            }
        }

        print!("Total processed {} objects", total_processed);
        Ok(())
    }

    pub async fn create_state_db(db_path: &str) -> Result<Connection, rusqlite::Error> {
        let conn = Connection::open(db_path)?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS object_state (
            key TEXT PRIMARY KEY,
            etag TEXT NOT NULL,
            size INTEGER NOT NULL,
            last_modified INTEGER NOT NULL
        )",
            [],
        )?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_etag ON object_state(etag)",
            [],
        )?;

        Ok(conn)
    }

    pub async fn stream_diff_and_update<F>(
        &mut self,
        db_path: &str,
        mut change_handler: F,
    ) -> Result<DiffStats, Box<dyn std::error::Error + Send + Sync>>
    where
        F: FnMut(Change) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
    {
        let mut conn = Self::create_state_db(db_path).await?;
        let mut stats = DiffStats {
            added: 0,
            modified: 0,
            deleted: 0,
            unchanged: 0,
        };

        let tx = conn.transaction()?;

        tx.execute(
            "ALTER TABLE object_state ADD COLUMN temp_seen INTEGER DEFAULT 0",
            [],
        )
        .ok();

        tx.execute("UPDATE object_state SET temp_seen = 0", [])?;

        let mut select_stmt =
            tx.prepare("SELECT etag, size, last_modified FROM object_state WHERE key = ?1")?;

        let mut update_stmt = tx.prepare(
            "INSERT OR REPLACE INTO object_state (key, etag, size, last_modified, temp_seen) VALUES (?1, ?2, ?3, ?4, 1)",
        )?;

        let mut mark_seen_stmt =
            tx.prepare("UPDATE object_state SET temp_seen = 1 WHERE key = ?1")?;

        self.stream_objects(|current_obj| {
            let previous_state = select_stmt
                .query_row([&current_obj.key], |row| {
                    Ok(CompactS3Object {
                        etag: row.get(0)?,
                        size: row.get(1)?,
                        last_modified: row.get(2)?,
                    })
                })
                .optional()?;

            let change = match previous_state {
                None => {
                    stats.added += 1;
                    update_stmt.execute(params![
                        current_obj.key,
                        current_obj.etag,
                        current_obj.size,
                        current_obj.last_modified.timestamp()
                    ])?;
                    Some(Change::Added(current_obj.clone()))
                }
                Some(prev_obj) => {
                    mark_seen_stmt.execute([&current_obj.key])?;

                    if prev_obj.etag != current_obj.etag {
                        // Object modified - update it
                        stats.modified += 1;
                        update_stmt.execute(params![
                            current_obj.key,
                            current_obj.etag,
                            current_obj.size,
                            current_obj.last_modified.timestamp()
                        ])?;
                        Some(Change::Modified {
                            old: S3Object {
                                key: current_obj.key.clone(),
                                etag: prev_obj.etag,
                                size: prev_obj.size as i64,
                                last_modified: DateTime::from_timestamp(prev_obj.last_modified, 0)
                                    .unwrap_or_default()
                                    .with_timezone(&Utc),
                            },
                            new: current_obj.clone(),
                        })
                    } else {
                        stats.unchanged += 1;
                        None
                    }
                }
            };

            if let Some(change) = change {
                change_handler(change)?;
            }

            let total_processed = stats.added + stats.modified + stats.unchanged;
            if total_processed.is_multiple_of(10_000) {
                println!(
                    "Processed {}: {} added, {} modified, {} unchanged",
                    total_processed, stats.added, stats.modified, stats.unchanged
                );
            }

            Ok(())
        })
        .await?;

        let mut deleted_stmt = tx.prepare(
            "SELECT key, etag, size, last_modified FROM object_state WHERE temp_seen = 0",
        )?;

        let deleted_rows: Vec<_> = deleted_stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    CompactS3Object {
                        etag: row.get(1)?,
                        size: row.get(2)?,
                        last_modified: row.get(3)?,
                    },
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;

        let mut delete_obj_stmt = tx.prepare("DELETE FROM object_state WHERE key = ?1")?;
        for deleted_row in deleted_rows {
            let (key, prev_obj) = deleted_row;
            stats.deleted += 1;

            let deleted_obj = S3Object {
                key: key.clone(),
                etag: prev_obj.etag,
                size: prev_obj.size as i64,
                last_modified: DateTime::from_timestamp(prev_obj.last_modified, 0)
                    .unwrap_or_default()
                    .with_timezone(&Utc),
            };

            change_handler(Change::Deleted(deleted_obj))?;
            delete_obj_stmt.execute([&key])?;
        }

        // Clean up the temporary column (for next run)
        // SQLite doesn't support DROP COLUMN before version 3.35.0
        // so let's make sure we use version >= 3.35.0 in production
        tx.execute(
            "ALTER TABLE object_state DROP COLUMN IF EXISTS temp_seen",
            [],
        )
        .or_else(|_| {
            println!("Warning: Could not drop temp_seen column (older SQLite version)");
            Ok::<_, rusqlite::Error>(0) // @todo: is it ok to return a success here?
        })?;

        // drop statements to release borrows on tx.
        // It took hours to figure this out so please Tommaso let me know if there's a better way :D
        drop(select_stmt);
        drop(update_stmt);
        drop(mark_seen_stmt);
        drop(deleted_stmt);
        drop(delete_obj_stmt);

        tx.execute(
            "INSERT OR REPLACE INTO metadata (key, value) VALUES ('last_updated', ?1)",
            params![Utc::now().timestamp()],
        )?;

        tx.commit()?;

        println!(
            "Diff completed: {} added, {} modified, {} deleted, {} unchanged",
            stats.added, stats.modified, stats.deleted, stats.unchanged
        );

        Ok(stats)
    }

    pub fn compact_to_s3_object(key: &str, compact: &CompactS3Object) -> S3Object {
        S3Object {
            key: key.to_string(),
            etag: compact.etag.clone(),
            size: compact.size as i64,
            last_modified: DateTime::from_timestamp(compact.last_modified, 0)
                .unwrap_or_default()
                .with_timezone(&Utc),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::remotes::aws;

    use super::*;
    use chrono::TimeZone;
    use tempfile::NamedTempFile;

    fn create_test_s3_object(key: &str, etag: &str, size: i64, timestamp: i64) -> S3Object {
        S3Object {
            key: key.to_string(),
            etag: etag.to_string(),
            size,
            last_modified: Utc.timestamp_opt(timestamp, 0).unwrap(),
        }
    }

    fn create_test_s3_conf() -> S3Conf {
        S3Conf::new(
            "test-bucket".to_string(),
            "test-key".to_string(),
            "test-secret".to_string(),
            "us-west-2".to_string(),
        )
    }

    #[test]
    fn test_s3_object_creation() {
        let obj = create_test_s3_object("test/file.txt", "etag123", 1024, 1609459200);

        assert_eq!(obj.key, "test/file.txt");
        assert_eq!(obj.etag, "etag123");
        assert_eq!(obj.size, 1024);
        assert_eq!(obj.last_modified, Utc.timestamp_opt(1609459200, 0).unwrap());
    }

    #[test]
    fn test_s3_struct_creation() {
        let s3 = create_test_s3_conf();

        assert_eq!(s3.bucket, "test-bucket");
        assert_eq!(s3.region, "us-west-2");
        assert_eq!(s3.get_access_key_id(), "test-key");
        assert_eq!(s3.get_secret_access_key(), "test-secret");
    }

    #[test]
    fn test_diff_stats_default() {
        let stats = DiffStats::default();

        assert_eq!(stats.added, 0);
        assert_eq!(stats.modified, 0);
        assert_eq!(stats.deleted, 0);
        assert_eq!(stats.unchanged, 0);
    }

    #[test]
    fn test_change_enum_variants() {
        let obj1 = create_test_s3_object("test1", "etag1", 100, 1609459200);
        let obj2 = create_test_s3_object("test2", "etag2", 200, 1609459300);
        let obj3 = create_test_s3_object("test3", "etag3", 300, 1609459400);

        let added = Change::Added(obj1.clone());
        let modified = Change::Modified {
            old: obj1.clone(),
            new: obj2.clone(),
        };
        let deleted = Change::Deleted(obj3.clone());

        match added {
            Change::Added(ref obj) => assert_eq!(obj.key, "test1"),
            _ => panic!("Expected Added variant"),
        }

        match modified {
            Change::Modified { ref old, ref new } => {
                assert_eq!(old.key, "test1");
                assert_eq!(new.key, "test2");
            }
            _ => panic!("Expected Modified variant"),
        }

        match deleted {
            Change::Deleted(ref obj) => assert_eq!(obj.key, "test3"),
            _ => panic!("Expected Deleted variant"),
        }
    }

    #[tokio::test]
    async fn test_create_state_db() {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path().to_str().unwrap();

        let conn = aws::S3::create_state_db(db_path).await.unwrap();

        let mut stmt = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table'")
            .unwrap();
        let tables: Vec<String> = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert!(tables.contains(&"object_state".to_string()));
        assert!(tables.contains(&"metadata".to_string()));

        let mut index_stmt = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_etag'")
            .unwrap();
        let index_count: i32 = index_stmt.query_row([], |_| Ok(1)).unwrap_or(0);
        assert_eq!(index_count, 1);
    }

    #[test]
    fn test_compact_to_s3_object() {
        let compact = CompactS3Object {
            etag: "etag123".to_string(),
            size: 1024,
            last_modified: 1609459200,
        };

        let s3_obj = aws::S3::compact_to_s3_object("test/file.txt", &compact);

        assert_eq!(s3_obj.key, "test/file.txt");
        assert_eq!(s3_obj.etag, "etag123");
        assert_eq!(s3_obj.size, 1024);
        assert_eq!(
            s3_obj.last_modified,
            Utc.timestamp_opt(1609459200, 0).unwrap()
        );
    }

    #[tokio::test]
    async fn test_database_operations() {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path().to_str().unwrap();

        let conn = aws::S3::create_state_db(db_path).await.unwrap();

        conn.execute(
            "INSERT INTO object_state (key, etag, size, last_modified) VALUES (?1, ?2, ?3, ?4)",
            params!["test/file.txt", "etag123", 1024, 1609459200],
        )
        .unwrap();

        let mut stmt = conn
            .prepare("SELECT key, etag, size, last_modified FROM object_state WHERE key = ?1")
            .unwrap();
        let (key, etag, size, last_modified): (String, String, i64, i64) = stmt
            .query_row(["test/file.txt"], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })
            .unwrap();

        assert_eq!(key, "test/file.txt");
        assert_eq!(etag, "etag123");
        assert_eq!(size, 1024);
        assert_eq!(last_modified, 1609459200);
    }

    #[tokio::test]
    async fn test_database_with_temp_seen_column() {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path().to_str().unwrap();

        let conn = aws::S3::create_state_db(db_path).await.unwrap();

        conn.execute(
            "INSERT INTO object_state (key, etag, size, last_modified) VALUES (?1, ?2, ?3, ?4)",
            params!["test/file.txt", "etag123", 1024, 1609459200],
        )
        .unwrap();

        let tx = conn.unchecked_transaction().unwrap();

        tx.execute(
            "ALTER TABLE object_state ADD COLUMN temp_seen INTEGER DEFAULT 0",
            [],
        )
        .unwrap();

        tx.execute(
            "UPDATE object_state SET temp_seen = 1 WHERE key = ?1",
            ["test/file.txt"],
        )
        .unwrap();

        let temp_seen: i32 = tx
            .query_row(
                "SELECT temp_seen FROM object_state WHERE key = ?1",
                ["test/file.txt"],
                |row| row.get(0),
            )
            .unwrap();

        assert_eq!(temp_seen, 1);

        tx.commit().unwrap();
    }

    #[test]
    fn test_s3_object_equality() {
        let obj1 = create_test_s3_object("test", "etag1", 100, 1609459200);
        let obj2 = create_test_s3_object("test", "etag1", 100, 1609459200);
        let obj3 = create_test_s3_object("test", "etag2", 100, 1609459200);

        assert_eq!(obj1, obj2);
        assert_ne!(obj1, obj3);
    }

    #[test]
    fn test_change_equality() {
        let obj1 = create_test_s3_object("test1", "etag1", 100, 1609459200);
        let obj2 = create_test_s3_object("test2", "etag2", 200, 1609459300);

        let change1 = Change::Added(obj1.clone());
        let change2 = Change::Added(obj1.clone());
        let change3 = Change::Added(obj2.clone());

        assert_eq!(change1, change2);
        assert_ne!(change1, change3);
    }

    #[test]
    fn test_s3_with_endpoint() {
        let s3 = S3Conf::new_with_endpoint(
            "test-bucket".to_string(),
            "test-key".to_string(),
            "test-secret".to_string(),
            "us-west-2".to_string(),
            "http://localhost:4566".to_string(),
        );

        assert_eq!(s3.bucket, "test-bucket");
        assert_eq!(s3.region, "us-west-2");
        assert_eq!(s3.endpoint_url, Some("http://localhost:4566".to_string()));
        assert_eq!(s3.get_access_key_id(), "test-key");
        assert_eq!(s3.get_secret_access_key(), "test-secret");
    }

    #[test]
    fn test_s3_without_endpoint() {
        let s3 = S3Conf::new(
            "test-bucket".to_string(),
            "test-key".to_string(),
            "test-secret".to_string(),
            "us-west-2".to_string(),
        );

        assert_eq!(s3.bucket, "test-bucket");
        assert_eq!(s3.region, "us-west-2");
        assert_eq!(s3.endpoint_url, None);
        assert_eq!(s3.get_access_key_id(), "test-key");
        assert_eq!(s3.get_secret_access_key(), "test-secret");
    }
}
