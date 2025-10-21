# Resy

**Re**mote **Sy**nc change detection lib. Currently supporting AWS S3 but possibly open to more sources.

## Usage

```rust
#[tokio::main]
  async fn main() -> Result<(), Box<dyn std::error::Error>> {
      let s3_conf = S3Conf::new(
          "my-bucket".to_string(),
          "AKIA...".to_string(),      // access_key_id
          "secret...".to_string(),    // secret_access_key
          "us-east-1".to_string(),    // region
      )
      // Initialize S3 client
      let s3 = S3::new(s3_conf).await;

      // Path to store state database
      let db_path = "./bucket_state.db";

      // Track changes and handle each one
      let stats = s3.stream_diff_and_update(db_path, |change| {
          match change {
              Change::Added(obj) => {
                  println!("➕ Added: {} ({} bytes)", obj.key, obj.size);
              }
              Change::Modified { old, new } => {
                  println!("🔄 Modified: {} ({} → {} bytes)",
                      new.key, old.size, new.size);
              }
              Change::Deleted(obj) => {
                  println!("❌ Deleted: {} ({} bytes)", obj.key, obj.size);
              }
          }
          Ok(())
      }).await?;

      // Print summary
      println!("\nSummary:");
      println!("  Added: {}", stats.added);
      println!("  Modified: {}", stats.modified);
      println!("  Deleted: {}", stats.deleted);
      println!("  Unchanged: {}", stats.unchanged);

      Ok(())
  }
```

## Security

AWS credentials are wrapped in `SecretString` with automatic memory zeroing via the `Zeroize` and `ZeroizeOnDrop` traits, ensuring secrets are cleared from memory when no longer needed.

Access to credentials is controlled through private getter methods that only expose secrets when explicitly needed for AWS API calls. The system uses read-only S3 operations (`list_objects_v2`) without requiring write permissions, following the principle of least privilege.

State persistence uses local SQLite databases with no network exposure, and the database schema stores only metadata (keys, ETags, sizes, timestamps) without any file contents or sensitive data, minimizing the attack surface while maintaining functionality.

## Scalability

Resy is designed for scalability across large buckets and frequent operations.

It uses AWS S3's paginated `list_objects_v2` API with 1000-object batches and **continuation tokens** to handle buckets with millions of objects without memory exhaustion.

The streaming architecture processes objects one-by-one rather than loading everything into memory, with progress reporting every 10,000 objects to provide visibility during long operations.

SQLite provides efficient local state persistence with indexed lookups on ETags for fast comparisons, while the temporary `temp_seen` column approach enables single-pass change detection without requiring multiple database scans.

Database transactions ensure consistency during concurrent operations, and the compact storage format (storing only metadata, not file contents) keeps the state database lightweight even for buckets with terabytes of data, making the system suitable for enterprise-scale S3 monitoring and synchronization tasks.

## How it works

```mermaid
flowchart TD
      A[Start: stream_diff_and_update] --> B[Create/Connect to SQLite DB]
      B --> C[Setup temporary tracking column temp_seen]
      C --> D[Reset all temp_seen = 0]
      D --> E[Start streaming S3 objects via list_objects_v2]

      E --> F{For each S3 object}
      F --> G[Query local DB for existing object by key]

      G --> H{Object exists in DB?}

      H -->|No| I[NEW OBJECT]
      I --> J[Insert into DB with temp_seen=1]
      J --> K[stats.added++]
      K --> L[Emit Change::Added]

      H -->|Yes| M[Mark temp_seen=1 for this key]
      M --> N{ETags match?}

      N -->|No| O[MODIFIED OBJECT]
      O --> P[Update DB with new data]
      P --> Q[stats.modified++]
      Q --> R[Emit Change::Modified with old/new]

      N -->|Yes| S[UNCHANGED OBJECT]
      S --> T[stats.unchanged++]
      T --> U[No change event emitted]

      L --> V[Process next object]
      R --> V
      U --> V
      V --> W{More objects?}

      W -->|Yes| F
      W -->|No| X[Find deleted objects: temp_seen=0]

      X --> Y{Any deleted objects?}
      Y -->|Yes| Z[For each deleted object]
      Z --> AA[stats.deleted++]
      AA --> BB[Emit Change::Deleted]
      BB --> CC[Delete from DB]
      CC --> DD{More deleted?}
      DD -->|Yes| Z
      DD -->|No| EE

      Y -->|No| EE[Cleanup: DROP temp_seen column]
      EE --> FF[Update last_updated timestamp]
      FF --> GG[Commit transaction]
      GG --> HH[Return DiffStats]

      subgraph DB["Database Schema"]
          I1["object_state table:<br/>key PRIMARY KEY<br/>etag<br/>size<br/>last_modified<br/>temp_seen temporary"]
          I2["metadata table:<br/>key<br/>value"]
      end

      subgraph CHANGES["Change Types"]
          J1["Change::Added<br/>New S3Object"]
          J2["Change::Modified<br/>old and new S3Object"]
          J3["Change::Deleted<br/>S3Object"]
      end

      subgraph PROGRESS["Progress Tracking"]
          K1["Every 10,000 objects<br/>Print progress stats"]
      end
```
