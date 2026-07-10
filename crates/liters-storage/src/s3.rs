//! S3-compatible replica client using litestream's S3 bucket layout:
//! `{prefix}/{level:04x}/{min:016x}-{max:016x}.ltx` (note: no `ltx/` path
//! segment, 4-digit zero-padded lowercase-hex level — s3/replica_client.go:655).
//!
//! The LTX header timestamp is stored as object metadata under
//! `litestream-timestamp` (RFC3339Nano), exactly like Go (s3/replica_client.go:55,
//! 697-707), so timestamp-accurate listings interoperate both ways.
//!
//! Blocking facade over `object_store`: the client owns a current-thread
//! tokio runtime on which every call runs to completion. This keeps the core
//! crates sync and mobile-friendly (no executor leaks across the FFI).

use std::io::Read;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::StreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::{
    Attribute, AttributeValue, Attributes, GetOptions, GetRange, ObjectStore, PutOptions,
};
use ltx::{format_filename, parse_filename, FileInfo, Header, Txid, HEADER_SIZE};

use crate::{ReplicaClient, Result, StorageError};

/// Object metadata key carrying the LTX header timestamp. (s3/replica_client.go:55)
const METADATA_KEY_TIMESTAMP: &str = "litestream-timestamp";

/// Configuration for an S3-compatible endpoint (AWS, Tigris, MinIO, R2, …).
#[derive(Debug, Clone, Default)]
pub struct S3Config {
    pub bucket: String,
    /// Key prefix for this database ("path" in litestream URLs).
    pub prefix: String,
    /// Custom endpoint URL, e.g. `https://fly.storage.tigris.dev` or
    /// `http://localhost:9000`. Empty = AWS.
    pub endpoint: Option<String>,
    pub region: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    /// Path-style addressing; litestream defaults to true for custom
    /// endpoints. (s3/replica_client.go:449)
    pub force_path_style: bool,
    /// Permit plain-HTTP endpoints (local MinIO).
    pub allow_http: bool,
}

/// Litestream-S3-layout replica client.
pub struct S3ReplicaClient {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    rt: tokio::runtime::Runtime,
}

impl S3ReplicaClient {
    pub fn new(config: S3Config) -> Result<S3ReplicaClient> {
        let mut builder = AmazonS3Builder::from_env().with_bucket_name(&config.bucket);
        if let Some(endpoint) = &config.endpoint {
            builder = builder.with_endpoint(endpoint);
        }
        if let Some(region) = &config.region {
            builder = builder.with_region(region);
        }
        if let Some(key) = &config.access_key_id {
            builder = builder.with_access_key_id(key);
        }
        if let Some(secret) = &config.secret_access_key {
            builder = builder.with_secret_access_key(secret);
        }
        if config.force_path_style {
            builder = builder.with_virtual_hosted_style_request(false);
        }
        if config.allow_http {
            builder = builder.with_allow_http(true);
        }
        let store = builder.build().map_err(|e| StorageError::Other(e.to_string()))?;

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()?;

        Ok(S3ReplicaClient {
            store: Arc::new(store),
            prefix: config.prefix.trim_matches('/').to_string(),
            rt,
        })
    }

    /// `{prefix}/{level:04x}` — the level "directory". (s3/replica_client.go:1392)
    fn level_prefix(&self, level: u8) -> object_store::path::Path {
        if self.prefix.is_empty() {
            object_store::path::Path::from(format!("{level:04x}"))
        } else {
            object_store::path::Path::from(format!("{}/{level:04x}", self.prefix))
        }
    }

    fn ltx_key(&self, level: u8, min: Txid, max: Txid) -> object_store::path::Path {
        let name = format_filename(min, max);
        if self.prefix.is_empty() {
            object_store::path::Path::from(format!("{level:04x}/{name}"))
        } else {
            object_store::path::Path::from(format!("{}/{level:04x}/{name}", self.prefix))
        }
    }
}

fn map_err(e: object_store::Error, ctx: (u8, Txid, Txid)) -> StorageError {
    match e {
        object_store::Error::NotFound { .. } => StorageError::NotFound {
            level: ctx.0,
            min_txid: ctx.1,
            max_txid: ctx.2,
        },
        e => StorageError::Other(e.to_string()),
    }
}

impl ReplicaClient for S3ReplicaClient {
    fn client_type(&self) -> &'static str {
        "s3"
    }

    fn ltx_files(&self, level: u8, seek: Txid, use_metadata: bool) -> Result<Vec<FileInfo>> {
        let prefix = self.level_prefix(level);
        let store = self.store.clone();

        let mut infos: Vec<FileInfo> = self.rt.block_on(async {
            let mut out = Vec::new();
            let mut stream = store.list(Some(&prefix));
            while let Some(meta) = stream.next().await {
                let meta = meta.map_err(|e| StorageError::Other(e.to_string()))?;
                let name = meta.location.filename().unwrap_or_default().to_string();
                // Non-LTX keys are skipped, as in Go. (s3/replica_client.go:1546)
                let Some((min_txid, max_txid)) = parse_filename(&name) else { continue };
                if min_txid < seek {
                    continue;
                }
                let created_at: SystemTime = meta.last_modified.into();
                out.push(FileInfo {
                    level,
                    min_txid,
                    max_txid,
                    size: meta.size,
                    created_at: Some(created_at),
                    ..Default::default()
                });
            }
            Ok::<_, StorageError>(out)
        })?;

        // Accurate creation timestamps from object metadata when requested.
        // (s3/replica_client.go:1413-1484)
        if use_metadata {
            for info in &mut infos {
                let key = self.ltx_key(level, info.min_txid, info.max_txid);
                let store = self.store.clone();
                let attrs: Option<Attributes> = self.rt.block_on(async {
                    store
                        .get_opts(&key, GetOptions { head: true, ..Default::default() })
                        .await
                        .ok()
                        .map(|r| r.attributes)
                });
                if let Some(attrs) = attrs {
                    if let Some(v) =
                        attrs.get(&Attribute::Metadata(METADATA_KEY_TIMESTAMP.into()))
                    {
                        if let Ok(t) = chrono::DateTime::parse_from_rfc3339(v.as_ref()) {
                            info.created_at = Some(t.into());
                        }
                        // Parse failures silently fall back to LastModified,
                        // as in Go.
                    }
                }
            }
        }

        infos.sort_by_key(|f| (f.min_txid, f.max_txid));
        Ok(infos)
    }

    fn open_ltx_file(
        &self,
        level: u8,
        min_txid: Txid,
        max_txid: Txid,
        offset: u64,
        size: u64,
    ) -> Result<Box<dyn Read + Send>> {
        let key = self.ltx_key(level, min_txid, max_txid);
        let range = if offset == 0 && size == 0 {
            None
        } else if size == 0 {
            Some(GetRange::Offset(offset))
        } else {
            Some(GetRange::Bounded(offset..offset + size))
        };

        let store = self.store.clone();
        let bytes = self.rt.block_on(async {
            let result = store
                .get_opts(&key, GetOptions { range, ..Default::default() })
                .await
                .map_err(|e| map_err(e, (level, min_txid, max_txid)))?;
            result
                .bytes()
                .await
                .map_err(|e| map_err(e, (level, min_txid, max_txid)))
        })?;

        Ok(Box::new(std::io::Cursor::new(bytes.to_vec())))
    }

    fn write_ltx_file(
        &self,
        level: u8,
        min_txid: Txid,
        max_txid: Txid,
        rd: &mut dyn Read,
    ) -> Result<FileInfo> {
        // Buffer the file (bounded: L0 deltas are small; snapshots at most
        // the database size) and peek the header for the timestamp.
        let mut body = Vec::new();
        rd.read_to_end(&mut body)?;
        if body.len() < HEADER_SIZE {
            return Err(StorageError::Other("ltx file shorter than header".into()));
        }
        let hdr = Header::decode(&body[..HEADER_SIZE])?;
        let timestamp = UNIX_EPOCH + Duration::from_millis(hdr.timestamp.max(0) as u64);

        // RFC3339Nano-style timestamp, parseable by Go's ParseTimestamp
        // fallback. (s3/replica_client.go:697-707)
        let ts: chrono::DateTime<chrono::Utc> = timestamp.into();
        let ts_str = ts.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true);

        let mut attributes = Attributes::new();
        attributes.insert(
            Attribute::Metadata(METADATA_KEY_TIMESTAMP.into()),
            AttributeValue::from(ts_str),
        );

        let key = self.ltx_key(level, min_txid, max_txid);
        let store = self.store.clone();
        let size = body.len() as u64;
        self.rt.block_on(async {
            store
                .put_opts(
                    &key,
                    body.into(),
                    PutOptions { attributes, ..Default::default() },
                )
                .await
                .map_err(|e| StorageError::Other(e.to_string()))
        })?;

        Ok(FileInfo {
            level,
            min_txid,
            max_txid,
            size,
            created_at: Some(timestamp),
            ..Default::default()
        })
    }

    fn delete_ltx_files(&self, infos: &[FileInfo]) -> Result<()> {
        let store = self.store.clone();
        for info in infos {
            let key = self.ltx_key(info.level, info.min_txid, info.max_txid);
            let result = self.rt.block_on(async { store.delete(&key).await });
            match result {
                Ok(()) => {}
                Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(StorageError::Other(e.to_string())),
            }
        }
        Ok(())
    }

    fn delete_all(&self) -> Result<()> {
        let prefix = if self.prefix.is_empty() {
            None
        } else {
            Some(object_store::path::Path::from(self.prefix.clone()))
        };
        let store = self.store.clone();
        self.rt.block_on(async {
            let mut stream = store.list(prefix.as_ref());
            while let Some(meta) = stream.next().await {
                let meta = meta.map_err(|e| StorageError::Other(e.to_string()))?;
                match store.delete(&meta.location).await {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                    Err(e) => return Err(StorageError::Other(e.to_string())),
                }
            }
            Ok(())
        })
    }
}
