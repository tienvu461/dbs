//! R2 object-create -> Queue consumer -> stateless backup -> R2 manifest.
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadEvent {
    pub action: String,
    pub bucket: String,
    pub object: EventObject,
    pub event_time: String,
}
#[derive(Debug, Deserialize, Serialize)]
pub struct EventObject {
    pub key: String,
    pub size: Option<u64>,
    #[serde(rename = "eTag")]
    pub etag: Option<String>,
}
impl UploadEvent {
    pub fn is_input(&self, bucket: &str, prefix: &str) -> bool {
        matches!(
            self.action.as_str(),
            "PutObject" | "CopyObject" | "CompleteMultipartUpload"
        ) && self.bucket == bucket
            && !prefix.is_empty()
            && self.object.key.starts_with(prefix)
    }
    pub fn manifest_key(&self, key_id: &str) -> String {
        // Length-prefixed JSON avoids ambiguity and never embeds a signed URL.
        let identity = serde_json::to_vec(&(self, key_id)).expect("event fields serialize");
        format!("manifests/{}.json", blake3::hash(&identity).to_hex())
    }
}
#[cfg(target_arch = "wasm32")]
mod runtime;
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parse_real_r2_shape_and_filter_output_events() {
        let event: UploadEvent = serde_json::from_str(r#"{"account":"test","action":"PutObject","bucket":"input","object":{"key":"incoming/backup.bin","size":10,"eTag":"abc"},"eventTime":"2026-10-11T00:00:00Z"}"#).unwrap();
        assert!(event.is_input("input", "incoming/"));
        assert!(!event.is_input("output", "incoming/"));
        assert!(!event.is_input("input", "manifests/"));
        let key = event.manifest_key("v1");
        assert!(key.starts_with("manifests/"));
        let mut changed = event;
        changed.object.etag = Some("new".into());
        assert_ne!(key, changed.manifest_key("v1"));
    }
}
