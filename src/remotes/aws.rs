use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::{Client, Error as S3Error, config::Credentials};
use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension, Result, params};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use zeroize::{Zeroize, ZeroizeOnDrop};

#[derive(Clone, Debug)]
struct S3Object {
    key: String,
    etag: String,
    size: i64,
    last_modified: DateTime<Utc>,
}

#[derive(Serialize, Deserialize, Clone)]
struct CompactS3Object {
    etag: String,
    size: u64,
    last_modified: i64,
}

#[derive(Serialize, Deserialize)]
struct CompactBucketState {
    objects: HashMap<String, CompactS3Object>, // HashMap key here is the S3 object key
    timestamp: i64,
}

struct BucketState {
    objects: HashMap<String, S3Object>,
    timestamp: DateTime<Utc>,
}

enum Change {
    Added(S3Object),
    Modified { old: S3Object, new: S3Object },
    Deleted(S3Object),
}

struct DiffStats {
    added: u64,
    modified: u64,
    deleted: u64,
    unchanged: u64,
}

#[derive(Zeroize, ZeroizeOnDrop)]
struct S3 {
    #[zeroize(skip)]
    bucket: String,
    #[zeroize(skip)]
    region: String,
    access_key_id: SecretString,
    secret_access_key: SecretString,
}

impl S3 {
    pub fn new(
        bucket: String,
        access_key_id: String,
        secret_access_key: String,
        region: String,
    ) -> Self {
        S3 {
            bucket,
            access_key_id: SecretString::new(access_key_id.into()),
            secret_access_key: SecretString::new(secret_access_key.into()),
            region,
        }
    }

    fn get_access_key_id(&self) -> &str {
        self.access_key_id.expose_secret()
    }

    fn get_secret_access_key(&self) -> &str {
        self.secret_access_key.expose_secret()
    }

    pub async fn create_client(&self) -> Result<Client, S3Error> {
        let credentials = Credentials::new(
            self.get_access_key_id(),
            self.get_secret_access_key(),
            None,
            None,
            "resy",
        );

        let region = Region::new(self.region.clone());

        let config = aws_config::defaults(BehaviorVersion::latest())
            .region(region)
            .credentials_provider(credentials)
            .load()
            .await;

        Ok(Client::new(&config))
    }

    async fn stream_objects<F>(&self, mut processor: F) -> Result<(), S3Error>
    where
        F: FnMut(S3Object) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
    {
        let client = self.create_client().await?;
        let mut continuation_token: Option<String> = None;
        let mut total_processed = 0;

        loop {
            let mut request = client.list_objects_v2().bucket(&self.bucket).max_keys(1000);

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

    async fn create_state_db(&self, db_path: &str) -> Result<Connection, rusqlite::Error> {
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
        &self,
        db_path: &str,
        mut change_handler: F,
    ) -> Result<DiffStats, Box<dyn std::error::Error + Send + Sync>>
    where
        F: FnMut(Change) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
    {
        let conn = self.create_state_db(db_path).await?;
        let mut stats = DiffStats {
            added: 0,
            modified: 0,
            deleted: 0,
            unchanged: 0,
        };

        let tx = conn.unchecked_transaction()?;

        tx.execute(
            "ALTER TABLE object_state ADD COLUMN temp_seen INTEGER DEFAULT 0",
            [],
        )
        .ok();

        tx.execute("UPDATE object_state SET temp_seen = 0", [])?;

        let mut select_stmt =
            tx.prepare("SELECT etag, size, last_modified FROM object_state WHERE key = ?1")?;

        let mut update_stmt = tx.prepare(
        "INSERT OR REPLACE INTO object_state (key, etag, size, last_modified, temp_seen) VALUES (?1, ?2, ?3, ?4, 1)"
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
            if total_processed % 10_000 == 0 {
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
        // So let's make sure we use version >= 3.35.0 in production
        tx.execute("ALTER TABLE object_state DROP COLUMN temp_seen", [])?;

        // Drop statements to release borrows on tx. It took hours to figure this out so please Tommaso let me know if there's a better way :D
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

    async fn save_state_to_db(
        &self,
        conn: &Connection,
        objects: &HashMap<String, S3Object>,
    ) -> Result<(), rusqlite::Error> {
        let tx = conn.unchecked_transaction()?;

        tx.execute("DELETE FROM object_state", [])?;

        {
            let mut stmt = tx.prepare(
                "INSERT INTO object_state (key, etag, size, last_modified) VALUES (?1, ?2, ?3, ?4)",
            )?;

            for (key, obj) in objects {
                stmt.execute(params![
                    key,
                    obj.etag,
                    obj.size,
                    obj.last_modified.timestamp()
                ])?;
            }
        }

        tx.execute(
            "INSERT OR REPLACE INTO metadata (key, value) VALUES ('last_updated', ?1)",
            params![Utc::now().timestamp()],
        )?;

        tx.commit()?;

        Ok(())
    }

    async fn compute_diff_from_db(
        &self,
        conn: &Connection,
    ) -> Result<Vec<Change>, Box<dyn std::error::Error>> {
        let mut changes: Vec<Change> = Vec::new();
        let mut current_objects: HashMap<String, S3Object> = HashMap::new();

        self.stream_objects(|obj| {
            current_objects.insert(obj.key.clone(), obj);
            Ok(())
        })
        .await?;

        let mut stmt = conn.prepare("SELECT key, etag, size, last_modified FROM object_state")?;

        let previous_objects: HashMap<String, CompactS3Object> = stmt
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
            .collect::<Result<HashMap<_, _>, _>>()?;

        for (key, current_obj) in &current_objects {
            match previous_objects.get(key) {
                None => changes.push(Change::Added(current_obj.clone())),
                Some(prev_obj) => {
                    if prev_obj.etag != current_obj.etag {
                        changes.push(Change::Modified {
                            old: self.compact_to_s3_object(key, prev_obj),
                            new: current_obj.clone(),
                        })
                    }
                }
            }
        }

        for (key, prev_obj) in &previous_objects {
            if !current_objects.contains_key(key) {
                changes.push(Change::Deleted(self.compact_to_s3_object(key, prev_obj)))
            }
        }

        println!("Computed {} changes", changes.len());

        Ok(changes)
    }

    fn compact_to_s3_object(&self, key: &str, compact: &CompactS3Object) -> S3Object {
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
