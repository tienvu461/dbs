use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use ulid::Ulid;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BackupStatus {
    Processing,
    Complete,
    Failed,
}

impl BackupStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Processing => "processing",
            Self::Complete => "complete",
            Self::Failed => "failed",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BackupRecord {
    pub id: String,
    pub name: String,
    pub status: BackupStatus,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub original_size: u64,
    pub stored_size: u64,
    pub checksum: String,
    pub tags: BTreeMap<String, String>,
    #[serde(default)]
    pub source_path: Option<String>,
    #[serde(default)]
    pub filename: String,
}

impl BackupRecord {
    pub fn new(name: String, tags: BTreeMap<String, String>) -> Self {
        Self {
            id: Ulid::new().to_string(),
            name,
            status: BackupStatus::Processing,
            created_at: Utc::now(),
            completed_at: None,
            original_size: 0,
            stored_size: 0,
            checksum: String::new(),
            tags,
            source_path: None,
            filename: String::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChunkManifest {
    pub sequence: u64,
    pub blob_id: String,
    pub plaintext_hash: String,
    pub stored_hash: String,
    pub plaintext_size: u64,
    pub stored_size: u64,
    pub compression: String,
    pub encryption: String,
    #[serde(default)]
    pub envelope: Option<String>,
    #[serde(default)]
    pub cdn_url: Option<String>,
    #[serde(default)]
    pub cdn_encrypted: Option<bool>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ArtifactManifest {
    pub filename: String,
    pub size: u64,
    pub chunks: Vec<ChunkManifest>,
    #[serde(default)]
    pub content: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BackupManifest {
    pub format: String,
    pub backup: BackupRecord,
    pub artifacts: Vec<ArtifactManifest>,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn backup_ids_are_ulids_and_records_start_processing() {
        let backup = BackupRecord::new("example".into(), BTreeMap::new());
        assert_eq!(backup.id.len(), 26);
        assert_eq!(backup.status, BackupStatus::Processing);
    }
}
