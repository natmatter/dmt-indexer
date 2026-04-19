//! Smoke test: open a fresh store, seed cursor, reopen, read it back.

use dmt_indexer::store::tables::{cursor_get, cursor_set, Cursor};
use dmt_indexer::store::Store;

#[test]
fn cursor_round_trip() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("index.redb");
    {
        let store = Store::open(&path).unwrap();
        let wtx = store.write().unwrap();
        cursor_set(
            &wtx,
            &Cursor {
                height: 900_000,
                block_hash: "abc".into(),
                updated_at: None,
            },
        )
        .unwrap();
        wtx.commit().unwrap();
    }
    let store = Store::open(&path).unwrap();
    let rtx = store.read().unwrap();
    let c = cursor_get(&rtx).unwrap().unwrap();
    assert_eq!(c.height, 900_000);
    assert_eq!(c.block_hash, "abc");
}
