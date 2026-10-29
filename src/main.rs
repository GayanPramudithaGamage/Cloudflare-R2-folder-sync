use anyhow::{anyhow, Context, Result};
use aws_config::BehaviorVersion;
use aws_sdk_s3::{
    config::Credentials,
    primitives::ByteStream,
    Client,
};
use notify::{
    Config as NotifyConfig, Event, RecommendedWatcher, RecursiveMode,
    Watcher,
};
use std::{
    env,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{
    sync::mpsc,
    time::sleep,
};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Clone)]
struct AppConfig {
    watch_dir: PathBuf,
    bucket: String,
    prefix: String,
    account_id: String,
    access_key: String,
    secret_key: String,
}

impl AppConfig {
    fn from_env() -> Result<Self> {
        let watch_dir = PathBuf::from(required_env("WATCH_DIR")?);

        let metadata = std::fs::metadata(&watch_dir)
            .with_context(|| {
                format!(
                    "Cannot access watched directory: {}",
                    watch_dir.display()
                )
            })?;

        if !metadata.is_dir() {
            return Err(anyhow!(
                "WATCH_DIR is not a directory: {}",
                watch_dir.display()
            ));
        }

        let prefix = env::var("R2_PREFIX")
            .unwrap_or_default()
            .trim_matches('/')
            .to_string();

        Ok(Self {
            watch_dir: std::fs::canonicalize(&watch_dir)?,
            bucket: required_env("R2_BUCKET")?,
            prefix,
            account_id: required_env("R2_ACCOUNT_ID")?,
            access_key: required_env("R2_ACCESS_KEY_ID")?,
            secret_key: required_env("R2_SECRET_ACCESS_KEY")?,
        })
    }
}

fn required_env(name: &str) -> Result<String> {
    env::var(name)
        .with_context(|| format!("Missing environment variable: {name}"))
}

fn build_s3_client(config: &AppConfig) -> Result<Client> {
    let credentials = Credentials::new(
        config.access_key.clone(),
        config.secret_key.clone(),
        None,
        None,
        "cloudflare-r2",
    );

    let region = aws_sdk_s3::config::Region::new("auto");

    // R2 implements the S3 API but uses a Cloudflare endpoint.
    let shared_config = aws_config::defaults(BehaviorVersion::latest())
        .region(region)
        .credentials_provider(credentials)
        .endpoint_url(format!(
            "https://{}.r2.cloudflarestorage.com",
            config.account_id
        ));

    // This is an S3-compatible service, not an AWS S3 bucket.
    // Use the explicit endpoint and avoid AWS-specific assumptions.
    let client_config = tokio::runtime::Handle::current();

    let _ = client_config;

    // Load the shared configuration asynchronously in main instead.
    Err(anyhow!(
        "Internal initialization placeholder: use build_s3_client_async"
    ))
}

async fn build_s3_client_async(config: &AppConfig) -> Result<Client> {
    let credentials = Credentials::new(
        config.access_key.clone(),
        config.secret_key.clone(),
        None,
        None,
        "cloudflare-r2",
    );

    let region = aws_sdk_s3::config::Region::new("auto");

    let shared_config = aws_config::defaults(BehaviorVersion::latest())
        .region(region)
        .credentials_provider(credentials)
        .endpoint_url(format!(
            "https://{}.r2.cloudflarestorage.com",
            config.account_id
        ))
        .load()
        .await;

    let s3_config = aws_sdk_s3::config::Builder::from(&shared_config)
        .force_path_style(true)
        .build();

    Ok(Client::from_conf(s3_config))
}

fn s3_key(config: &AppConfig, path: &Path) -> Result<String> {
    let relative_path = path
        .strip_prefix(&config.watch_dir)
        .with_context(|| {
            format!(
                "File is outside watched directory: {}",
                path.display()
            )
        })?;

    let relative_key = relative_path
        .components()
        .map(|component| {
            component
                .as_os_str()
                .to_str()
                .ok_or_else(|| anyhow!("File path is not valid UTF-8"))
        })
        .collect::<Result<Vec<_>>>()?
        .join("/");

    if relative_key.is_empty() {
        return Err(anyhow!("Cannot upload the watched directory itself"));
    }

    if config.prefix.is_empty() {
        Ok(relative_key)
    } else {
        Ok(format!("{}/{}", config.prefix, relative_key))
    }
}

async fn wait_until_stable(path: &Path) -> Result<()> {
    const REQUIRED_STABLE_CHECKS: usize = 3;
    const CHECK_INTERVAL: Duration = Duration::from_secs(1);

    let mut previous_size: Option<u64> = None;
    let mut stable_checks = 0;

    loop {
        let metadata = match tokio::fs::metadata(path).await {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(anyhow!("File disappeared: {}", path.display()));
            }
            Err(error) => return Err(error.into()),
        };

        if !metadata.is_file() {
            return Err(anyhow!(
                "Path is not a regular file: {}",
                path.display()
            ));
        }

        let current_size = metadata.len();

        if previous_size == Some(current_size) {
            stable_checks += 1;
        } else {
            stable_checks = 0;
        }

        if stable_checks >= REQUIRED_STABLE_CHECKS {
            return Ok(());
        }

        previous_size = Some(current_size);
        sleep(CHECK_INTERVAL).await;
    }
}

async fn upload_file(
    client: &Client,
    config: &AppConfig,
    path: &Path,
) -> Result<()> {
    wait_until_stable(path).await?;

    let key = s3_key(config, path)?;

    info!(
        file = %path.display(),
        bucket = %config.bucket,
        key = %key,
        "Uploading file to Cloudflare R2"
    );

    // Open the file as a streaming body rather than reading it
    // entirely into a Vec<u8>.
    let body = ByteStream::from_path(path)
        .await
        .with_context(|| format!("Cannot open file: {}", path.display()))?;

    client
        .put_object()
        .bucket(&config.bucket)
        .key(&key)
        .body(body)
        .send()
        .await
        .with_context(|| {
            format!("R2 upload failed for {}", path.display())
        })?;

    info!(
        file = %path.display(),
        key = %key,
        "R2 upload succeeded"
    );

    // Delete only after the S3-compatible API confirms success.
    tokio::fs::remove_file(path)
        .await
        .with_context(|| {
            format!(
                "Upload succeeded, but local deletion failed: {}",
                path.display()
            )
        })?;

    info!(
        file = %path.display(),
        "Local file deleted after successful upload"
    );

    Ok(())
}

async fn process_file(
    client: &Client,
    config: &AppConfig,
    path: PathBuf,
) {
    if !path.starts_with(&config.watch_dir) {
        return;
    }

    // Ignore temporary files. Producers should rename completed
    // files to their final names before the watcher processes them.
    if path.extension().is_some_and(|ext| {
        ext.eq_ignore_ascii_case("tmp")
            || ext.eq_ignore_ascii_case("part")
    }) {
        return;
    }

    if !path.is_file() {
        return;
    }

    let mut attempt: u32 = 0;

    loop {
        attempt += 1;

        match upload_file(client, config, &path).await {
            Ok(()) => return,

            Err(error) => {
                // A failed upload must not delete the source.
                warn!(
                    file = %path.display(),
                    attempt,
                    error = %error,
                    "Processing failed; retaining file"
                );

                let delay_secs = 2_u64
                    .saturating_pow(attempt.min(6))
                    .min(60);

                sleep(Duration::from_secs(delay_secs)).await;

                // The file may have been removed externally.
                if !path.exists() {
                    warn!(
                        file = %path.display(),
                        "File no longer exists; stopping retries"
                    );
                    return;
                }
            }
        }
    }
}

fn collect_files(dir: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    for entry in std::fs::read_dir(dir)
        .with_context(|| format!("Cannot scan {}", dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        let metadata = entry.metadata()?;

        if metadata.is_dir() {
            collect_files(&path, files)?;
        } else if metadata.is_file() {
            files.push(path);
        }
    }

    Ok(())
}

fn process_event(event: Event, tx: &mpsc::UnboundedSender<PathBuf>) {
    for path in event.paths {
        // Notify can emit multiple events for a single file.
        // Processing is serialized in this basic implementation.
        if let Err(error) = tx.send(path) {
            error!(%error, "Could not enqueue filesystem event");
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config = AppConfig::from_env()?;

    info!(
        directory = %config.watch_dir.display(),
        bucket = %config.bucket,
        prefix = %config.prefix,
        "Starting R2 folder watcher"
    );

    let client = build_s3_client_async(&config).await?;

    // Check credentials, bucket access, and connectivity before
    // starting the watcher.
    client
        .head_bucket()
        .bucket(&config.bucket)
        .send()
        .await
        .context("Cannot access the R2 bucket; check credentials and permissions")?;

    info!("R2 bucket access verified");

    let (tx, mut rx) = mpsc::unbounded_channel::<PathBuf>();

    // Keep the watcher alive for the lifetime of the application.
    let event_tx = tx.clone();

    let mut watcher = RecommendedWatcher::new(
        move |result: notify::Result<Event>| match result {
            Ok(event) => process_event(event, &event_tx),
            Err(error) => error!(%error, "Filesystem watcher error"),
        },
        NotifyConfig::default(),
    )?;

    watcher.watch(&config.watch_dir, RecursiveMode::Recursive)?;

    // Scan existing files on startup so files left behind after a
    // restart or failed upload are processed again.
    let mut existing_files = Vec::new();
    collect_files(&config.watch_dir, &mut existing_files)?;

    for path in existing_files {
        tx.send(path)
            .context("Could not enqueue existing file")?;
    }

    info!("Watching for new files; press Ctrl+C to stop");

    // This initial implementation uses one worker, so only one
    // file is processed at a time.
    while let Some(path) = rx.recv().await {
        process_file(&client, &config, path).await;
    }

    Ok(())
}
