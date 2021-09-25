// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

use super::peer_causet_storage::{
    write_initial_apply_state, write_initial_violetabft_state, INIT_EPOCH_CONF_VER, INIT_EPOCH_VER,
};
use super::util::new_peer;
use crate::Result;
use edb::{Engines, CausetEngine, MuBlock, VioletaBftEngine};
use edb::{Causet_DEFAULT, Causet_VIOLETABFT};

use ekvproto::meta_timeshare;
use ekvproto::violetabft_server_timeshare::{VioletaBftLocalState, BraneLocalState, StoreIdent};

pub fn initial_brane(store_id: u64, brane_id: u64, peer_id: u64) -> meta_timeshare::Brane {
    let mut brane = meta_timeshare::Brane::default();
    brane.set_id(brane_id);
    brane.set_spacelike_key(tuplespaceInstanton::EMPTY_KEY.to_vec());
    brane.set_lightlike_key(tuplespaceInstanton::EMPTY_KEY.to_vec());
    brane.mut_brane_epoch().set_version(INIT_EPOCH_VER);
    brane.mut_brane_epoch().set_conf_ver(INIT_EPOCH_CONF_VER);
    brane.mut_peers().push(new_peer(store_id, peer_id));
    brane
}

// check no any data in cone [spacelike_key, lightlike_key)
fn is_cone_empty(
    engine: &impl CausetEngine,
    causet: &str,
    spacelike_key: &[u8],
    lightlike_key: &[u8],
) -> Result<bool> {
    let mut count: u32 = 0;
    engine.scan_causet(causet, spacelike_key, lightlike_key, false, |_, _| {
        count += 1;
        Ok(false)
    })?;

    Ok(count == 0)
}

// Bootstrap the store, the DB for this store must be empty and has no data.
//
// FIXME: ER typaram should just be impl CausetEngine, but VioletaBftEngine doesn't support
// the `is_cone_empty` query yet.
pub fn bootstrap_store<ER>(
    engines: &Engines<impl CausetEngine, ER>,
    cluster_id: u64,
    store_id: u64,
) -> Result<()>
where
    ER: VioletaBftEngine,
{
    let mut ident = StoreIdent::default();

    if !is_cone_empty(&engines.kv, Causet_DEFAULT, tuplespaceInstanton::MIN_KEY, tuplespaceInstanton::MAX_KEY)? {
        return Err(box_err!("kv store is not empty and has already had data."));
    }

    ident.set_cluster_id(cluster_id);
    ident.set_store_id(store_id);

    engines.kv.put_msg(tuplespaceInstanton::STORE_IDENT_KEY, &ident)?;
    engines.sync_kv()?;
    Ok(())
}

/// The first phase of bootstrap cluster
///
/// Write the first brane meta and prepare state.
pub fn prepare_bootstrap_cluster(
    engines: &Engines<impl CausetEngine, impl VioletaBftEngine>,
    brane: &meta_timeshare::Brane,
) -> Result<()> {
    let mut state = BraneLocalState::default();
    state.set_brane(brane.clone());

    let mut wb = engines.kv.write_batch();
    box_try!(wb.put_msg(tuplespaceInstanton::PREPARE_BOOTSTRAP_KEY, brane));
    box_try!(wb.put_msg_causet(Causet_VIOLETABFT, &tuplespaceInstanton::brane_state_key(brane.get_id()), &state));
    write_initial_apply_state(&mut wb, brane.get_id())?;
    engines.kv.write(&wb)?;
    engines.sync_kv()?;

    let mut violetabft_wb = engines.violetabft.log_batch(1024);
    write_initial_violetabft_state(&mut violetabft_wb, brane.get_id())?;
    box_try!(engines.violetabft.consume(&mut violetabft_wb, true));
    Ok(())
}

// Clear first brane meta and prepare key.
pub fn clear_prepare_bootstrap_cluster(
    engines: &Engines<impl CausetEngine, impl VioletaBftEngine>,
    brane_id: u64,
) -> Result<()> {
    let mut wb = engines.violetabft.log_batch(1024);
    box_try!(engines
        .violetabft
        .clean(brane_id, &VioletaBftLocalState::default(), &mut wb));
    box_try!(engines.violetabft.consume(&mut wb, true));

    let mut wb = engines.kv.write_batch();
    box_try!(wb.delete(tuplespaceInstanton::PREPARE_BOOTSTRAP_KEY));
    // should clear violetabft initial state too.
    box_try!(wb.delete_causet(Causet_VIOLETABFT, &tuplespaceInstanton::brane_state_key(brane_id)));
    box_try!(wb.delete_causet(Causet_VIOLETABFT, &tuplespaceInstanton::apply_state_key(brane_id)));
    engines.kv.write(&wb)?;
    engines.sync_kv()?;
    Ok(())
}

// Clear prepare key
pub fn clear_prepare_bootstrap_key(
    engines: &Engines<impl CausetEngine, impl VioletaBftEngine>,
) -> Result<()> {
    box_try!(engines.kv.delete(tuplespaceInstanton::PREPARE_BOOTSTRAP_KEY));
    engines.sync_kv()?;
    Ok(())
}

#[causet(test)]
mod tests {
    use tempfile::Builder;

    use super::*;
    use edb::Engines;
    use edb::{Peekable, Causet_DEFAULT};

    #[test]
    fn test_bootstrap() {
        let path = Builder::new().prefix("var").temfidelir().unwrap();
        let violetabft_path = path.path().join("violetabft");
        let kv_engine = engine_lmdb::util::new_engine(
            path.path().to_str().unwrap(),
            None,
            &[Causet_DEFAULT, Causet_VIOLETABFT],
            None,
        )
        .unwrap();
        let violetabft_engine =
            engine_lmdb::util::new_engine(violetabft_path.to_str().unwrap(), None, &[Causet_DEFAULT], None)
                .unwrap();
        let engines = Engines::new(kv_engine.clone(), violetabft_engine.clone());
        let brane = initial_brane(1, 1, 1);

        assert!(bootstrap_store(&engines, 1, 1).is_ok());
        assert!(bootstrap_store(&engines, 1, 1).is_err());

        assert!(prepare_bootstrap_cluster(&engines, &brane).is_ok());
        assert!(kv_engine
            .get_value(tuplespaceInstanton::PREPARE_BOOTSTRAP_KEY)
            .unwrap()
            .is_some());
        assert!(kv_engine
            .get_value_causet(Causet_VIOLETABFT, &tuplespaceInstanton::brane_state_key(1))
            .unwrap()
            .is_some());
        assert!(kv_engine
            .get_value_causet(Causet_VIOLETABFT, &tuplespaceInstanton::apply_state_key(1))
            .unwrap()
            .is_some());
        assert!(violetabft_engine
            .get_value(&tuplespaceInstanton::violetabft_state_key(1))
            .unwrap()
            .is_some());

        assert!(clear_prepare_bootstrap_key(&engines).is_ok());
        assert!(clear_prepare_bootstrap_cluster(&engines, 1).is_ok());
        assert!(is_cone_empty(
            &kv_engine,
            Causet_VIOLETABFT,
            &tuplespaceInstanton::brane_meta_prefix(1),
            &tuplespaceInstanton::brane_meta_prefix(2)
        )
        .unwrap());
        assert!(is_cone_empty(
            &violetabft_engine,
            Causet_DEFAULT,
            &tuplespaceInstanton::brane_violetabft_prefix(1),
            &tuplespaceInstanton::brane_violetabft_prefix(2)
        )
        .unwrap());
    }
}
