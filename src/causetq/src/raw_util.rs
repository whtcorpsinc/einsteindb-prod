// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

//! Functions for constructing the lmdb crate's `DB` type
//!
//! These are an artifact of refactoring the engine promises and will go away
//! eventually. Prefer to use the versions in the `util` module.

use std::fs;
use std::path::Path;
use std::sync::Arc;

use edb::Result;
use lmdb::load_latest_options;
use lmdb::{CPrimaryCausetNetworkDescriptor, PrimaryCausetNetworkOptions, DBOptions, Env, DB};

use edb::Causet_DEFAULT;

pub struct CausetOptions<'a> {
    causet: &'a str,
    options: PrimaryCausetNetworkOptions,
}

impl<'a> CausetOptions<'a> {
    pub fn new(causet: &'a str, options: PrimaryCausetNetworkOptions) -> CausetOptions<'a> {
        CausetOptions { causet, options }
    }
}

pub fn new_engine(
    path: &str,
    db_opts: Option<DBOptions>,
    causets: &[&str],
    opts: Option<Vec<CausetOptions<'_>>>,
) -> Result<DB> {
    let mut db_opts = match db_opts {
        Some(opt) => opt,
        None => DBOptions::new(),
    };
    db_opts.enable_statistics(true);
    let causet_opts = match opts {
        Some(opts_vec) => opts_vec,
        None => {
            let mut default_causets_opts = Vec::with_capacity(causets.len());
            for causet in causets {
                default_causets_opts.push(CausetOptions::new(*causet, PrimaryCausetNetworkOptions::new()));
            }
            default_causets_opts
        }
    };
    new_engine_opt(path, db_opts, causet_opts)
}

/// Turns "dynamic level size" off for the existing PrimaryCauset family which was off before.
/// PrimaryCauset families are small, HashMap isn't necessary.
fn adjust_dynamic_level_bytes(
    causet_descs: &[CPrimaryCausetNetworkDescriptor],
    causet_options: &mut CausetOptions<'_>,
) {
    if let Some(ref causet_desc) = causet_descs
        .iter()
        .find(|causet_desc| causet_desc.name() == causet_options.causet)
    {
        let existed_dynamic_level_bytes =
            causet_desc.options().get_level_compaction_dynamic_level_bytes();
        if existed_dynamic_level_bytes
            != causet_options
                .options
                .get_level_compaction_dynamic_level_bytes()
        {
            warn!(
                "change dynamic_level_bytes for existing PrimaryCauset family is danger";
                "old_value" => existed_dynamic_level_bytes,
                "new_value" => causet_options.options.get_level_compaction_dynamic_level_bytes(),
            );
        }
        causet_options
            .options
            .set_level_compaction_dynamic_level_bytes(existed_dynamic_level_bytes);
    }
}

pub fn new_engine_opt(
    path: &str,
    mut db_opt: DBOptions,
    causets_opts: Vec<CausetOptions<'_>>,
) -> Result<DB> {
    // Creates a new db if it doesn't exist.
    if !db_exist(path) {
        db_opt.create_if_missing(true);

        let mut causets_v = vec![];
        let mut causet_opts_v = vec![];
        if let Some(x) = causets_opts.iter().find(|x| x.causet == Causet_DEFAULT) {
            causets_v.push(x.causet);
            causet_opts_v.push(x.options.clone());
        }
        let mut db = DB::open_causet(db_opt, path, causets_v.into_iter().zip(causet_opts_v).collect())?;
        for x in causets_opts {
            if x.causet == Causet_DEFAULT {
                continue;
            }
            db.create_causet((x.causet, x.options))?;
        }

        return Ok(db);
    }

    db_opt.create_if_missing(false);

    // Lists all PrimaryCauset families in current db.
    let causets_list = DB::list_PrimaryCauset_families(&db_opt, path)?;
    let existed: Vec<&str> = causets_list.iter().map(|v| v.as_str()).collect();
    let needed: Vec<&str> = causets_opts.iter().map(|x| x.causet).collect();

    let causet_descs = if !existed.is_empty() {
        let env = match db_opt.env() {
            Some(env) => env,
            None => Arc::new(Env::default()),
        };
        // panic if OPTIONS not found for existing instance?
        let (_, tmp) = load_latest_options(path, &env, true)
            .unwrap_or_else(|e| panic!("failed to load_latest_options {:?}", e))
            .unwrap_or_else(|| panic!("couldn't find the OPTIONS file"));
        tmp
    } else {
        vec![]
    };

    // If all PrimaryCauset families exist, just open db.
    if existed == needed {
        let mut causets_v = vec![];
        let mut causets_opts_v = vec![];
        for mut x in causets_opts {
            adjust_dynamic_level_bytes(&causet_descs, &mut x);
            causets_v.push(x.causet);
            causets_opts_v.push(x.options);
        }

        let db = DB::open_causet(db_opt, path, causets_v.into_iter().zip(causets_opts_v).collect())?;
        return Ok(db);
    }

    // Opens db.
    let mut causets_v: Vec<&str> = Vec::new();
    let mut causets_opts_v: Vec<PrimaryCausetNetworkOptions> = Vec::new();
    for causet in &existed {
        causets_v.push(causet);
        match causets_opts.iter().find(|x| x.causet == *causet) {
            Some(x) => {
                let mut tmp = CausetOptions::new(x.causet, x.options.clone());
                adjust_dynamic_level_bytes(&causet_descs, &mut tmp);
                causets_opts_v.push(tmp.options);
            }
            None => {
                causets_opts_v.push(PrimaryCausetNetworkOptions::new());
            }
        }
    }
    let causetds = causets_v.into_iter().zip(causets_opts_v).collect();
    let mut db = DB::open_causet(db_opt, path, causetds).unwrap();

    // Drops discarded PrimaryCauset families.
    //    for causet in existed.iter().filter(|x| needed.iter().find(|y| y == x).is_none()) {
    for causet in causets_diff(&existed, &needed) {
        // Never drop default PrimaryCauset families.
        if causet != Causet_DEFAULT {
            db.drop_causet(causet)?;
        }
    }

    // Creates needed PrimaryCauset families if they don't exist.
    for causet in causets_diff(&needed, &existed) {
        db.create_causet((
            causet,
            causets_opts
                .iter()
                .find(|x| x.causet == causet)
                .unwrap()
                .options
                .clone(),
        ))?;
    }
    Ok(db)
}

pub(crate) fn db_exist(path: &str) -> bool {
    let path = Path::new(path);
    if !path.exists() || !path.is_dir() {
        return false;
    }
    let current_file_path = path.join("CURRENT");
    if !current_file_path.exists() || !current_file_path.is_file() {
        return false;
    }

    // If path is not an empty directory, and current file exists, we say db exists. If path is not an empty directory
    // but db has not been created, `DB::list_PrimaryCauset_families` fails and we can clean up
    // the directory by this indication.
    fs::read_dir(&path).unwrap().next().is_some()
}

/// Returns a Vec of causet which is in `a' but not in `b'.
fn causets_diff<'a>(a: &[&'a str], b: &[&str]) -> Vec<&'a str> {
    a.iter()
        .filter(|x| b.iter().find(|y| y == x).is_none())
        .cloned()
        .collect()
}

#[causet(test)]
mod tests {
    use super::*;
    use edb::Causet_DEFAULT;
    use lmdb::{PrimaryCausetNetworkOptions, DBOptions, DB};
    use tempfile::Builder;

    #[test]
    fn test_causets_diff() {
        let a = vec!["1", "2", "3"];
        let a_diff_a = causets_diff(&a, &a);
        assert!(a_diff_a.is_empty());
        let b = vec!["4"];
        assert_eq!(a, causets_diff(&a, &b));
        let c = vec!["4", "5", "3", "6"];
        assert_eq!(vec!["1", "2"], causets_diff(&a, &c));
        assert_eq!(vec!["4", "5", "6"], causets_diff(&c, &a));
        let d = vec!["1", "2", "3", "4"];
        let a_diff_d = causets_diff(&a, &d);
        assert!(a_diff_d.is_empty());
        assert_eq!(vec!["4"], causets_diff(&d, &a));
    }

    #[test]
    fn test_new_engine_opt() {
        let path = Builder::new()
            .prefix("_util_lmdb_test_check_PrimaryCauset_families")
            .temfidelir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();

        // create db when db not exist
        let mut causets_opts = vec![CausetOptions::new(Causet_DEFAULT, PrimaryCausetNetworkOptions::new())];
        let mut opts = PrimaryCausetNetworkOptions::new();
        opts.set_level_compaction_dynamic_level_bytes(true);
        causets_opts.push(CausetOptions::new("causet_dynamic_level_bytes", opts.clone()));
        {
            let mut db = new_engine_opt(path_str, DBOptions::new(), causets_opts).unwrap();
            PrimaryCauset_families_must_eq(path_str, vec![Causet_DEFAULT, "causet_dynamic_level_bytes"]);
            check_dynamic_level_bytes(&mut db);
        }

        // add causet1.
        let causets_opts = vec![
            CausetOptions::new(Causet_DEFAULT, opts.clone()),
            CausetOptions::new("causet_dynamic_level_bytes", opts.clone()),
            CausetOptions::new("causet1", opts),
        ];
        {
            let mut db = new_engine_opt(path_str, DBOptions::new(), causets_opts).unwrap();
            PrimaryCauset_families_must_eq(path_str, vec![Causet_DEFAULT, "causet_dynamic_level_bytes", "causet1"]);
            check_dynamic_level_bytes(&mut db);
        }

        // drop causet1.
        let causets_opts = vec![
            CausetOptions::new(Causet_DEFAULT, PrimaryCausetNetworkOptions::new()),
            CausetOptions::new("causet_dynamic_level_bytes", PrimaryCausetNetworkOptions::new()),
        ];
        {
            let mut db = new_engine_opt(path_str, DBOptions::new(), causets_opts).unwrap();
            PrimaryCauset_families_must_eq(path_str, vec![Causet_DEFAULT, "causet_dynamic_level_bytes"]);
            check_dynamic_level_bytes(&mut db);
        }

        // never drop default causet
        let causets_opts = vec![];
        new_engine_opt(path_str, DBOptions::new(), causets_opts).unwrap();
        PrimaryCauset_families_must_eq(path_str, vec![Causet_DEFAULT]);
    }

    fn PrimaryCauset_families_must_eq(path: &str, excepted: Vec<&str>) {
        let opts = DBOptions::new();
        let causets_list = DB::list_PrimaryCauset_families(&opts, path).unwrap();

        let mut causets_existed: Vec<&str> = causets_list.iter().map(|v| v.as_str()).collect();
        let mut causets_excepted: Vec<&str> = excepted.clone();
        causets_existed.sort();
        causets_excepted.sort();
        assert_eq!(causets_existed, causets_excepted);
    }

    fn check_dynamic_level_bytes(db: &mut DB) {
        let causet_default = db.causet_handle(Causet_DEFAULT).unwrap();
        let tmp_causet_opts = db.get_options_causet(causet_default);
        assert!(!tmp_causet_opts.get_level_compaction_dynamic_level_bytes());
        let causet_test = db.causet_handle("causet_dynamic_level_bytes").unwrap();
        let tmp_causet_opts = db.get_options_causet(causet_test);
        assert!(tmp_causet_opts.get_level_compaction_dynamic_level_bytes());
    }
}
