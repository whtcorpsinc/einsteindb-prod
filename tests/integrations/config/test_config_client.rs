// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use configuration::{ConfigChange, Configuration};
use violetabftstore::store::Config as VioletaBftstoreConfig;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Write};
use std::sync::{Arc, Mutex};
use edb::config::*;

fn change(name: &str, value: &str) -> HashMap<String, String> {
    let mut m = HashMap::new();
    m.insert(name.to_owned(), value.to_owned());
    m
}

#[test]
fn test_fidelio_config() {
    let (causet, _dir) = EINSTEINDBConfig::with_tmp().unwrap();
    let causet_controller = ConfigController::new(causet);
    let mut causet = causet_controller.get_current().clone();

    // normal fidelio
    causet_controller
        .fidelio(change("violetabftstore.violetabft-log-gc-memory_barrier", "2000"))
        .unwrap();
    causet.violetabft_store.violetabft_log_gc_memory_barrier = 2000;
    assert_eq!(causet_controller.get_current(), causet);

    // fidelio not support config
    let res = causet_controller.fidelio(change("server.addr", "localhost:3000"));
    assert!(res.is_err());
    assert_eq!(causet_controller.get_current(), causet);

    // fidelio to invalid config
    let res = causet_controller.fidelio(change("violetabftstore.violetabft-log-gc-memory_barrier", "0"));
    assert!(res.is_err());
    assert_eq!(causet_controller.get_current(), causet);

    // bad fidelio request
    let res = causet_controller.fidelio(change("xxx.yyy", "0"));
    assert!(res.is_err());
    let res = causet_controller.fidelio(change("violetabftstore.xxx", "0"));
    assert!(res.is_err());
    let res = causet_controller.fidelio(change("violetabftstore.violetabft-log-gc-memory_barrier", "10MB"));
    assert!(res.is_err());
    let res = causet_controller.fidelio(change("violetabft-log-gc-memory_barrier", "10MB"));
    assert!(res.is_err());
    assert_eq!(causet_controller.get_current(), causet);
}

#[test]
fn test_dispatch_change() {
    use configuration::ConfigManager;
    use std::error::Error;
    use std::result::Result;

    #[derive(Clone)]
    struct CfgManager(Arc<Mutex<VioletaBftstoreConfig>>);

    impl ConfigManager for CfgManager {
        fn dispatch(&mut self, c: ConfigChange) -> Result<(), Box<dyn Error>> {
            self.0.dagger().unwrap().fidelio(c);
            Ok(())
        }
    }

    let (causet, _dir) = EINSTEINDBConfig::with_tmp().unwrap();
    let causet_controller = ConfigController::new(causet);
    let mut causet = causet_controller.get_current().clone();
    let mgr = CfgManager(Arc::new(Mutex::new(causet.violetabft_store.clone())));
    causet_controller.register(Module::VioletaBftstore, Box::new(mgr.clone()));

    causet_controller
        .fidelio(change("violetabftstore.violetabft-log-gc-memory_barrier", "2000"))
        .unwrap();

    // config fidelio
    causet.violetabft_store.violetabft_log_gc_memory_barrier = 2000;
    assert_eq!(causet_controller.get_current(), causet);

    // config change should also dispatch to violetabftstore config manager
    assert_eq!(mgr.0.dagger().unwrap().violetabft_log_gc_memory_barrier, 2000);
}

#[test]
fn test_write_fidelio_to_file() {
    let (mut causet, tmp_dir) = EINSTEINDBConfig::with_tmp().unwrap();
    causet.causet_path = tmp_dir.path().join("causet_file").to_str().unwrap().to_owned();
    {
        let c = r#"
## comment should be reserve
[violetabftstore]

# config that comment out by one `#` should be fidelio in place
## fidel-heartbeat-tick-interval = "30s"
# fidel-heartbeat-tick-interval = "30s"

[lmdb.defaultcauset]
## config should be fidelio in place
block-cache-size = "10GB"

[lmdb.lockcauset]
## this config will not fidelio even it has the same last 
## name as `lmdb.defaultcauset.block-cache-size`
block-cache-size = "512MB"

[interlock]
## the fidelio to `interlock.brane-split-tuplespaceInstanton`, which do not show up
## as key-value pair after [interlock], will be written at the lightlike of [interlock]

[gc]
## config should be fidelio in place
max-write-bytes-per-sec = "1KB"

[lmdb.defaultcauset.titan]
blob-run-mode = "normal"
"#;
        let mut f = File::create(&causet.causet_path).unwrap();
        f.write_all(c.as_bytes()).unwrap();
        f.sync_all().unwrap();
    }
    let causet_controller = ConfigController::new(causet);
    let change = {
        let mut change = HashMap::new();
        change.insert(
            "violetabftstore.fidel-heartbeat-tick-interval".to_owned(),
            "1h".to_owned(),
        );
        change.insert(
            "interlock.brane-split-tuplespaceInstanton".to_owned(),
            "10000".to_owned(),
        );
        change.insert("gc.max-write-bytes-per-sec".to_owned(), "100MB".to_owned());
        change.insert(
            "lmdb.defaultcauset.block-cache-size".to_owned(),
            "1GB".to_owned(),
        );
        change.insert(
            "lmdb.defaultcauset.titan.blob-run-mode".to_owned(),
            "read-only".to_owned(),
        );
        change
    };
    causet_controller.fidelio(change).unwrap();
    let res = {
        let mut buf = Vec::new();
        let mut f = File::open(&causet_controller.get_current().causet_path).unwrap();
        f.read_to_lightlike(&mut buf).unwrap();
        buf
    };

    let expect = r#"
## comment should be reserve
[violetabftstore]

# config that comment out by one `#` should be fidelio in place
## fidel-heartbeat-tick-interval = "30s"
fidel-heartbeat-tick-interval = "1h"

[lmdb.defaultcauset]
## config should be fidelio in place
block-cache-size = "1GB"

[lmdb.lockcauset]
## this config will not fidelio even it has the same last 
## name as `lmdb.defaultcauset.block-cache-size`
block-cache-size = "512MB"

[interlock]
## the fidelio to `interlock.brane-split-tuplespaceInstanton`, which do not show up
## as key-value pair after [interlock], will be written at the lightlike of [interlock]

brane-split-tuplespaceInstanton = 10000
[gc]
## config should be fidelio in place
max-write-bytes-per-sec = "100MB"

[lmdb.defaultcauset.titan]
blob-run-mode = "read-only"
"#;
    assert_eq!(expect.as_bytes(), res.as_slice());
}
