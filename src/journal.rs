use std::{
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::{Value, json};
use tokio::{
    fs::{self, OpenOptions},
    io::AsyncWriteExt,
    sync::Mutex,
};

use crate::protocol::LabConfig;

const WRITEUP_CONTEXT_LIMIT: u64 = 2 * 1024 * 1024;

#[derive(Clone)]
pub struct Journal {
    dir: PathBuf,
    write_lock: Arc<Mutex<()>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Manifest<'a> {
    version: u8,
    lab_id: &'a str,
    created_at_ms: u128,
    config: &'a LabConfig,
}

impl Journal {
    pub async fn open(lab_id: &str, config: &LabConfig) -> Result<Self> {
        let base = dirs::data_dir()
            .or_else(|| dirs::home_dir().map(|path| path.join(".local/share")))
            .context("cannot determine user data directory")?;
        let dir = base.join("kuebiko/labs").join(lab_id);
        fs::create_dir_all(&dir).await?;
        fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).await?;
        let manifest_path = dir.join("manifest.json");
        if fs::metadata(&manifest_path).await.is_err() {
            let manifest = Manifest {
                version: 1,
                lab_id,
                created_at_ms: now_ms(),
                config,
            };
            write_atomic(&manifest_path, &serde_json::to_vec_pretty(&manifest)?).await?;
        }
        Ok(Self {
            dir,
            write_lock: Arc::new(Mutex::new(())),
        })
    }

    pub async fn record<T: Serialize>(&self, source: &str, kind: &str, data: &T) {
        let value = json!({
            "timestampMs": now_ms(),
            "source": source,
            "kind": kind,
            "data": data,
        });
        if let Err(error) = self.append(value).await {
            tracing::warn!(%error, "write lab journal");
        }
    }

    async fn append(&self, value: Value) -> Result<()> {
        let _guard = self.write_lock.lock().await;
        let mut bytes = serde_json::to_vec(&value)?;
        bytes.push(b'\n');
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o600)
            .open(self.dir.join("events.jsonl"))
            .await?;
        file.write_all(&bytes).await?;
        file.flush().await?;
        Ok(())
    }

    pub async fn writeup_context(&self) -> Result<String> {
        let path = self.dir.join("events.jsonl");
        let file = fs::File::open(&path).await?;
        let length = file.metadata().await?.len();
        let start = length.saturating_sub(WRITEUP_CONTEXT_LIMIT);
        use tokio::io::{AsyncReadExt, AsyncSeekExt};
        let mut file = file;
        file.seek(std::io::SeekFrom::Start(start)).await?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).await?;
        if start > 0
            && let Some(newline) = bytes.iter().position(|byte| *byte == b'\n')
        {
            bytes.drain(..=newline);
        }
        let prefix = if start > 0 {
            "[Earlier journal events omitted due to the 2 MiB context limit.]\n"
        } else {
            ""
        };
        Ok(format!("{prefix}{}", String::from_utf8_lossy(&bytes)))
    }

    pub async fn save_writeup(&self, markdown: &str) -> Result<PathBuf> {
        let path = self.dir.join("writeup.md");
        write_atomic(&path, markdown.as_bytes()).await?;
        Ok(path)
    }
}

async fn write_atomic(path: &PathBuf, bytes: &[u8]) -> Result<()> {
    let temporary = path.with_extension("tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .mode(0o600)
        .open(&temporary)
        .await?;
    file.write_all(bytes).await?;
    file.flush().await?;
    drop(file);
    fs::rename(temporary, path).await?;
    Ok(())
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}
