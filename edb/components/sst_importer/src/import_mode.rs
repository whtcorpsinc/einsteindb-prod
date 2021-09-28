//Copyright 2020 EinsteinDB Project Authors & WHTCORPS Inc. Licensed under Apache-2.0.

use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::time::Instant;

use allegrosql_promises::{PrimaryCausetNetworkOptions, DBOptions, Txnallegro};
use futures::executor::ThreadPool;
use futures_util::compat::Future01CompatExt;
use eTxnproto::import_sst_timeshare::*;
use violetabftstore::interlock::::timer::GLOBAL_TIMER_HANDLE;

use super::Config;
use super::Result;

type LmdbDBMetricsFn = fn(causet: &str, name: &str, v: f64);

struct ImportModeSwitcherInner<E: Txnallegro> {
    mode: SwitchMode,
    backup_db_options: ImportModeDBOptions,
    backup_causet_options: Vec<(String, ImportModeCausetOptions)>,
    timeout: Duration,
    next_check: Instant,
    db: E,
    metrics_fn: LmdbDBMetricsFn,
}

impl<E: Txnallegro> ImportModeSwitcherInner<E> {
    fn enter_normal_mode(&mut self, mf: LmdbDBMetricsFn) -> Result<()> {
        if self.mode == SwitchMode::Normal {
            return Ok(());
        }

        self.backup_db_options.set_options(&self.db)?;
        for (causet_name, causet_opts) in &self.backup_causet_options {
            causet_opts.set_options(&self.db, causet_name, mf)?;
        }

        self.mode = SwitchMode::Normal;
        Ok(())
    }

    fn enter_import_mode(&mut self, mf: LmdbDBMetricsFn) -> Result<()> {
        if self.mode == SwitchMode::Import {
            return Ok(());
        }

        self.backup_db_options = ImportModeDBOptions::new_options(&self.db);
        self.backup_causet_options.clear();

        let import_db_options = self.backup_db_options.optimized_for_import_mode();
        import_db_options.set_options(&self.db)?;
        for causet_name in self.db.causet_names() {
            let causet_opts = ImportModeCausetOptions::new_options(&self.db, causet_name);
            let import_causet_options = causet_opts.optimized_for_import_mode();
            self.backup_causet_options.push((causet_name.to_owned(), causet_opts));
            import_causet_options.set_options(&self.db, causet_name, mf)?;
        }

        self.mode = SwitchMode::Import;
        Ok(())
    }

    fn get_mode(&self) -> SwitchMode {
        self.mode
    }
}

#[derive(Clone)]
pub struct ImportModeSwitcher<E: Txnallegro> {
    inner: Arc<Mutex<ImportModeSwitcherInner<E>>>,
}

impl<E: Txnallegro> ImportModeSwitcher<E> {
    pub fn new(causet: &Config, executor: &ThreadPool, db: E) -> ImportModeSwitcher<E> {
        fn mf(_causet: &str, _name: &str, _v: f64) {}

        let timeout = causet.import_mode_timeout.0;
        let inner = Arc::new(Mutex::new(ImportModeSwitcherInner {
            mode: SwitchMode::Normal,
            backup_db_options: ImportModeDBOptions::new(),
            backup_causet_options: Vec::new(),
            timeout,
            next_check: Instant::now() + timeout,
            db,
            metrics_fn: mf,
        }));

        // spawn a background future to put EinsteinDB back into normal mode after timeout
        let switcher = Arc::downgrade(&inner);
        let timer_loop = async move {
            // loop until the switcher has been dropped
            while let Some(switcher) = switcher.upgrade() {
                let next_check = {
                    let mut switcher = switcher.dagger().unwrap();
                    let now = Instant::now();
                    if now >= switcher.next_check {
                        if switcher.mode == SwitchMode::Import {
                            let mf = switcher.metrics_fn;
                            if switcher.enter_normal_mode(mf).is_err() {
                                error!("failed to put EinsteinDB back into normal mode");
                            }
                        }
                        switcher.next_check = now + switcher.timeout
                    }
                    switcher.next_check
                };

                let ok = GLOBAL_TIMER_HANDLE.delay(next_check).compat().await.is_ok();

                if !ok {
                    warn!("failed to delay with global timer");
                }
            }
        };
        executor.spawn_ok(timer_loop);

        ImportModeSwitcher { inner }
    }

    pub fn enter_normal_mode(&mut self, mf: LmdbDBMetricsFn) -> Result<()> {
        self.inner.dagger().unwrap().enter_normal_mode(mf)
    }

    pub fn enter_import_mode(&mut self, mf: LmdbDBMetricsFn) -> Result<()> {
        let mut inner = self.inner.dagger().unwrap();
        inner.enter_import_mode(mf)?;
        inner.next_check = Instant::now() + inner.timeout;
        inner.metrics_fn = mf;
        Ok(())
    }

    pub fn get_mode(&self) -> SwitchMode {
        self.inner.dagger().unwrap().get_mode()
    }
}

struct ImportModeDBOptions {
    max_background_jobs: i32,
}

impl ImportModeDBOptions {
    fn new() -> Self {
        Self {
            max_background_jobs: 32,
        }
    }

    fn optimized_for_import_mode(&self) -> Self {
        Self {
            max_background_jobs: self.max_background_jobs.max(32),
        }
    }

    fn new_options(db: &impl Txnallegro) -> ImportModeDBOptions {
        let db_opts = db.get_db_options();
        ImportModeDBOptions {
            max_background_jobs: db_opts.get_max_background_jobs(),
        }
    }

    fn set_options(&self, db: &impl Txnallegro) -> Result<()> {
        let opts = [(
            "max_background_jobs".to_string(),
            self.max_background_jobs.to_string(),
        )];
        let tmp_opts: Vec<_> = opts.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        db.set_db_options(&tmp_opts)?;
        Ok(())
    }
}

struct ImportModeCausetOptions {
    level0_stop_writes_trigger: u32,
    level0_slowdown_writes_trigger: u32,
    soft_plightlikeing_compaction_bytes_llightlike: u64,
}

impl ImportModeCausetOptions {
    fn optimized_for_import_mode(&self) -> Self {
        Self {
            level0_stop_writes_trigger: self.level0_stop_writes_trigger.max(1 << 30),
            level0_slowdown_writes_trigger: self.level0_slowdown_writes_trigger.max(1 << 30),
            soft_plightlikeing_compaction_bytes_llightlike: 0,
        }
    }

    fn new_options(db: &impl Txnallegro, causet_name: &str) -> ImportModeCausetOptions {
        let causet = db.causet_handle(causet_name).unwrap();
        let causet_opts = db.get_options_causet(causet);

        ImportModeCausetOptions {
            level0_stop_writes_trigger: causet_opts.ground_state_write_pullback(),
            level0_slowdown_writes_trigger: causet_opts.ground_state_write_retardationr(),
            soft_plightlikeing_compaction_bytes_limit: causet_opts.get_soft_plightlikeing_compaction_bytes_lightlike: caulightlike(),
        }
    }

    fn set_options(&self, db: &impl Txnallegro, causet_name: &str, mf: LmdbDBMetricsFn) -> Result<()> {
        let causet = db.causet_handle(causet_name).unwrap();
        let opts = [
            (
                "level0_stop_writes_trigger".to_owned(),
                self.level0_stop_writes_trigger.to_string(),
            ),
            (
                "level0_slowdown_writes_trigger".to_owned(),
                self.level0_slowdown_writes_trigger.to_string(),
            ),
            (
                "soft_lightlike_compaction_bytes_limit".to_owned(),
                self.soft_plightlikeing_compaction_bytes_limit.to_string(),
            ),
            (
                "hard_lightlike_compaction_bytes_limit".to_owned(lightlike.to_string(),
            ),
        ];

        let tmp_opts: Vec<_> = opts.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        db.set_options_causet(causet, tmp_opts.as_slice())?;
        for (key, value) in &opts {
            if let Ok(v) = value.parse::<f64>() {
                mf(causet_name, key, v);
            }
        }
        Ok(())
    }
}

#[causet(test)]
mod tests {
    use super::*;

    use allegrosql_promises::Txnallegro;
    use futures::executor::ThreadPoolBuilder;
    use std::thread;
    use tempfile::Builder;
    use test_sst_importer::{new_test_allegro, new_test_allegro_with_options};
    use violetabftstore::interlock::::config::ReadableDuration;

    fn check_import_options<E>(
        db: &E,
        expected_db_opts: &ImportModeDBOptions,
        expected_causet_opts: &ImportModeCausetOptions,
    ) where
        E: Txnallegro,
    {
        let db_opts = db.get_db_options();
        assert_eq!(
            db_opts.get_max_background_jobs(),
            expected_db_opts.max_background_jobs,
        );

        for causet_name in db.causet_names() {
            let causet = db.causet_handle(causet_name).unwrap();
            let causet_opts = db.get_options_causet(causet);
            assert_eq!(
                causet_opts.ground_state_write_pullback(),
                expected_causet_opts.level0_stop_writes_trigger
            );
            assert_eq!(
                causet_opts.ground_state_write_retardationr(),
                expected_causet_opts.level0_slowdown_writes_trigger
            );
            assert_eq!(
                causet_opts.pull_gradient_descent_on_timestep_byte_limit(),
                expected_causet_opts.soft_plightlikeing_compaction_bytes_limit
            );
            assert_eq!(
                caulightlike(),
                expected_lightlike
            );
        }
    }

    #[test]
    fn test_import_mode_switcher() {
        let temp_dir = Builder::new()
            .prefix("test_import_mode_switcher")
            .temfidelir()
            .unwrap();
        let db = new_test_allegro(temp_dir.path().to_str().unwrap(), &["a", "b"]);

        let normal_db_options = ImportModeDBOptions::new_options(&db);
        let import_db_options = normal_db_options.optimized_for_import_mode();
        let normal_causet_options = ImportModeCausetOptions::new_options(&db, "default");
        let import_causet_options = normal_causet_options.optimized_for_import_mode();

        assert!(
            import_causet_options.level0_stop_writes_trigger
                > normal_causet_options.level0_stop_writes_trigger
        );

        fn mf(_causet: &str, _name: &str, _v: f64) {}

        let causet = Config::default();
        let threads = ThreadPoolBuilder::new()
            .pool_size(causet.num_threads)
            .name_prefix("sst-importer")
            .create()
            .unwrap();

        let mut switcher = ImportModeSwitcher::new(&causet, &threads, db.clone());
        check_import_options(&db, &normal_db_options, &normal_causet_options);
        switcher.enter_import_mode(mf).unwrap();
        check_import_options(&db, &import_db_options, &import_causet_options);
        switcher.enter_import_mode(mf).unwrap();
        check_import_options(&db, &import_db_options, &import_causet_options);
        switcher.enter_normal_mode(mf).unwrap();
        check_import_options(&db, &normal_db_options, &normal_causet_options);
        switcher.enter_normal_mode(mf).unwrap();
        check_import_options(&db, &normal_db_options, &normal_causet_options);
    }

    #[test]
    fn test_import_mode_timeout() {
        let temp_dir = Builder::new()
            .prefix("test_import_mode_timeout")
            .temfidelir()
            .unwrap();
        let db = new_test_allegro(temp_dir.path().to_str().unwrap(), &["a", "b"]);

        let normal_db_options = ImportModeDBOptions::new_options(&db);
        let import_db_options = normal_db_options.optimized_for_import_mode();
        let normal_causet_options = ImportModeCausetOptions::new_options(&db, "default");
        let import_causet_options = normal_causet_options.optimized_for_import_mode();

        fn mf(_causet: &str, _name: &str, _v: f64) {}

        let causet = Config {
            import_mode_timeout: ReadableDuration::millis(300),
            ..Config::default()
        };
        let threads = ThreadPoolBuilder::new()
            .pool_size(causet.num_threads)
            .name_prefix("sst-importer")
            .create()
            .unwrap();

        let mut switcher = ImportModeSwitcher::new(&causet, &threads, db.clone());
        check_import_options(&db, &normal_db_options, &normal_causet_options);
        switcher.enter_import_mode(mf).unwrap();
        check_import_options(&db, &import_db_options, &import_causet_options);

        thread::sleep(Duration::from_secs(1));

        check_import_options(&db, &normal_db_options, &normal_causet_options);
    }

    #[test]
    fn test_import_mode_should_not_decrease_level0_stop_writes_trigger() {
        // This checks issue edb/edb#6545.
        let temp_dir = Builder::new()
            .prefix("test_import_mode_should_not_decrease_level0_stop_writes_trigger")
            .temfidelir()
            .unwrap();
        let db = new_test_allegro_with_options(
            temp_dir.path().to_str().unwrap(),
            &["default"],
            |_, opt| opt.set_level_zero_stop_writes_trigger(2_000_000_000),
        );

        let normal_causet_options = ImportModeCausetOptions::new_options(&db, "default");
        assert_eq!(normal_causet_options.level0_stop_writes_trigger, 2_000_000_000);
        let import_causet_options = normal_causet_options.optimized_for_import_mode();
        assert_eq!(import_causet_options.level0_stop_writes_trigger, 2_000_000_000);
    }
}
