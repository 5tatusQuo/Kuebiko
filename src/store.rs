use std::{
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::fs;

use crate::protocol::{LabConfig, LabRecord};

#[derive(Debug, Default, Serialize, Deserialize)]
struct StoredState {
    #[serde(default)]
    labs: Vec<LabRecord>,
}

#[derive(Clone)]
pub struct Store {
    path: PathBuf,
}

impl Store {
    pub fn new() -> Result<Self> {
        let base = dirs::state_dir()
            .or_else(|| dirs::home_dir().map(|p| p.join(".local/state")))
            .context("cannot determine user state directory")?;
        Ok(Self {
            path: base.join("kuebiko/state.json"),
        })
    }

    pub async fn list(&self) -> Vec<LabRecord> {
        self.read().await.unwrap_or_default().labs
    }

    pub async fn get(&self, id: &str) -> Option<LabRecord> {
        self.list().await.into_iter().find(|lab| lab.id == id)
    }

    pub async fn put(
        &self,
        id: String,
        config: LabConfig,
        thread_id: Option<String>,
    ) -> Result<()> {
        let mut state = self.read().await.unwrap_or_default();
        state.labs.retain(|lab| lab.id != id);
        state.labs.insert(
            0,
            LabRecord {
                id,
                config,
                thread_id,
                updated_at: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
            },
        );
        state.labs.truncate(20);
        self.write(&state).await
    }

    async fn read(&self) -> Result<StoredState> {
        let content = fs::read(&self.path).await.context("read state file")?;
        serde_json::from_slice(&content).context("parse state file")
    }

    async fn write(&self, state: &StoredState) -> Result<()> {
        let parent = self.path.parent().context("state path has no parent")?;
        fs::create_dir_all(parent).await?;
        let temporary = self.path.with_extension("json.tmp");
        fs::write(&temporary, serde_json::to_vec_pretty(state)?).await?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&temporary, std::fs::Permissions::from_mode(0o600)).await?;
        }
        fs::rename(temporary, &self.path).await?;
        Ok(())
    }
}
