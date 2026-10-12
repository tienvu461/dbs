use super::*;
use std::{cell::RefCell, collections::BTreeMap};
#[derive(Default)]
struct Memory(RefCell<BTreeMap<String, Vec<u8>>>);
#[async_trait(?Send)]
impl BlobStore for Memory {
    async fn put(&self, key: &str, bytes: &[u8]) -> Result<String> {
        self.0.borrow_mut().insert(key.into(), bytes.to_vec());
        Ok(key.into())
    }
    async fn get(&self, key: &str, max: usize) -> Result<Vec<u8>> {
        let data = self.0.borrow().get(key).cloned().ok_or(Error::Transport)?;
        if data.len() > max {
            return Err(Error::Limit);
        }
        Ok(data)
    }
}
#[test]
fn restore_without_catalog_and_reject_modified_metadata() {
    futures::executor::block_on(async {
        let store = Memory::default();
        let engine = Uploader::new([7; 32], "test-key".into(), 4, 1000).unwrap();
        let data = b"stateless backups do not need sqlite";
        let manifest = engine
            .upload(&data[..], "backup.bin".into(), &store)
            .await
            .unwrap();
        let portable = Manifest::from_json(&manifest.to_json().unwrap()).unwrap();
        let fresh = Uploader::new([7; 32], "test-key".into(), 8, 1000).unwrap();
        let mut output = Vec::new();
        fresh.restore(&portable, &store, &mut output).await.unwrap();
        assert_eq!(output, data);
        let mut modified = portable.clone();
        modified.filename = "changed".into();
        assert!(fresh.verify_manifest(&modified).is_err());
        assert!(Uploader::new([8; 32], "test-key".into(), 4, 1000)
            .unwrap()
            .verify_manifest(&portable)
            .is_err());
        store
            .0
            .borrow_mut()
            .get_mut(&portable.chunks[0].locator)
            .unwrap()[25] ^= 1;
        assert!(fresh
            .restore(&portable, &store, &mut Vec::new())
            .await
            .is_err());
    });
}
#[test]
fn empty_input_and_limit_handling() {
    futures::executor::block_on(async {
        let store = Memory::default();
        let engine = Uploader::new([7; 32], "test-key".into(), 4, 5).unwrap();
        let manifest = engine
            .upload(&b""[..], "empty".into(), &store)
            .await
            .unwrap();
        engine
            .restore(&manifest, &store, &mut Vec::new())
            .await
            .unwrap();
        assert!(matches!(
            engine.upload(&b"123456"[..], "large".into(), &store).await,
            Err(Error::Limit)
        ));
    });
}

#[test]
fn corrupt_upload_readback_never_returns_successful_manifest() {
    struct Corrupt;
    #[async_trait(?Send)]
    impl BlobStore for Corrupt {
        async fn put(&self, _: &str, _: &[u8]) -> Result<String> {
            Ok("fixture".into())
        }
        async fn get(&self, _: &str, _: usize) -> Result<Vec<u8>> {
            Ok(vec![0; 40])
        }
    }
    futures::executor::block_on(async {
        let engine = Uploader::new([7; 32], "test-key".into(), 4, 1000).unwrap();
        assert!(matches!(
            engine
                .upload(&b"test"[..], "backup.bin".into(), &Corrupt)
                .await,
            Err(Error::Integrity)
        ));
    });
}
