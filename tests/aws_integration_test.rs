use aws_config::SdkConfig;
use aws_credential_types::Credentials;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_sdk_s3::{
    Client,
    config::{self, BehaviorVersion, Region},
};
use testcontainers::{GenericImage, core::WaitFor, runners::AsyncRunner};
use testcontainers::{ImageExt, core::ContainerPort};

#[tokio::test]
async fn test_stream_diff_and_update() {
    let localstack_port = ContainerPort::Tcp(4566);
    let container = GenericImage::new("localstack/localstack", "s3-latest")
        .with_exposed_port(localstack_port)
        .with_wait_for(WaitFor::message_on_stdout("Ready."))
        .with_env_var("SERVICES", "s3")
        .start()
        .await
        .unwrap();

    let host = container.get_host().await.unwrap();
    let host_port = container
        .get_host_port_ipv4(localstack_port.as_u16())
        .await
        .unwrap();
    let endpoint_url = format!("http://{}:{}", host, host_port);

    let credentials = Credentials::new("test", "test", None, None, "test");
    let config = SdkConfig::builder()
        .credentials_provider(SharedCredentialsProvider::new(credentials))
        .endpoint_url(endpoint_url)
        .region(Region::new("us-east-1"))
        .behavior_version(BehaviorVersion::latest())
        .build();

    let s3_config = config::Builder::from(&config)
        .force_path_style(true)
        .build();

    let s3_client = Client::from_conf(s3_config);

    let bucket_name = "my-test-bucket";

    s3_client
        .create_bucket()
        .bucket(bucket_name)
        .send()
        .await
        .expect("Failed to create S3 bucket");

    let list_buckets_output = s3_client
        .list_buckets()
        .send()
        .await
        .expect("Failed to list S3 buckets");

    let buckets = list_buckets_output.buckets();
    let found = buckets.iter().any(|b| b.name() == Some(bucket_name));
    assert!(found, "Bucket '{}' was not found in the list.", bucket_name);

    let db_file = tempfile::NamedTempFile::new().unwrap();
    let db_path = db_file.path().to_str().unwrap();

    let mut s3 = resy::remotes::aws::S3::from_client(s3_client.clone(), bucket_name.to_string());

    // 1. Initial check: No changes
    let stats = s3
        .stream_diff_and_update(db_path, |_| Ok(()))
        .await
        .unwrap();
    assert_eq!(stats, resy::remotes::aws::DiffStats::default());

    // 2. Add a file
    let key = "test-file.txt";
    let content = "Hello, Resy!";
    s3_client
        .put_object()
        .bucket(bucket_name)
        .key(key)
        .body(aws_sdk_s3::primitives::ByteStream::from(
            content.as_bytes().to_vec(),
        ))
        .send()
        .await
        .unwrap();

    let mut changes = Vec::new();
    let stats = s3
        .stream_diff_and_update(db_path, |change| {
            changes.push(change);
            Ok(())
        })
        .await
        .unwrap();

    assert_eq!(stats.added, 1);
    assert_eq!(stats.modified, 0);
    assert_eq!(stats.deleted, 0);
    assert_eq!(changes.len(), 1);
    match &changes[0] {
        resy::remotes::aws::Change::Added(obj) => {
            assert_eq!(obj.key, key);
            assert_eq!(obj.size, content.len() as i64);
        }
        _ => panic!("Expected Added change"),
    }

    // 3. Modify the file
    let updated_content = "Hello, Resy! Updated.";
    s3_client
        .put_object()
        .bucket(bucket_name)
        .key(key)
        .body(aws_sdk_s3::primitives::ByteStream::from(
            updated_content.as_bytes().to_vec(),
        ))
        .send()
        .await
        .unwrap();

    let mut changes = Vec::new();
    let stats = s3
        .stream_diff_and_update(db_path, |change| {
            changes.push(change);
            Ok(())
        })
        .await
        .unwrap();

    assert_eq!(stats.added, 0);
    assert_eq!(stats.modified, 1);
    assert_eq!(stats.deleted, 0);
    assert_eq!(changes.len(), 1);
    match &changes[0] {
        resy::remotes::aws::Change::Modified { old, new } => {
            assert_eq!(new.key, key);
            assert_eq!(old.size, content.len() as i64);
            assert_eq!(new.size, updated_content.len() as i64);
        }
        _ => panic!("Expected Modified change"),
    }

    // 4. Delete the file
    s3_client
        .delete_object()
        .bucket(bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();

    let mut changes = Vec::new();
    let stats = s3
        .stream_diff_and_update(db_path, |change| {
            changes.push(change);
            Ok(())
        })
        .await
        .unwrap();

    assert_eq!(stats.added, 0);
    assert_eq!(stats.modified, 0);
    assert_eq!(stats.deleted, 1);
    assert_eq!(changes.len(), 1);
    match &changes[0] {
        resy::remotes::aws::Change::Deleted(obj) => {
            assert_eq!(obj.key, key);
            assert_eq!(obj.size, updated_content.len() as i64);
        }
        _ => panic!("Expected Deleted change"),
    }
}
