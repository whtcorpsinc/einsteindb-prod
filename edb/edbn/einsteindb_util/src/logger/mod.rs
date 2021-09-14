// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

mod file_log;
mod formatter;

use std::env;
use std::fmt;
use std::io::{self, BufWriter};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::thread;

use log::{self, SetLoggerError};
use slog::{self, Drain, FnValue, Key, OwnedKVList, PushFnValue, Record, KV};
use slog_async::{Async, OverflowStrategy};
use slog_term::{Decorator, PlainDecorator, RecordDecorator};

use self::file_log::{RotateBySize, RotateByTime, RotatingFileLogger, RotatingFileLoggerBuilder};
use crate::config::{ReadableDuration, ReadableSize};

pub use slog::{FilterFn, Level};

// The suffix applightlikeed to the lightlike of rotated log files by datetime log rotator
// Warning: Diagnostics service parses log files by file name format.
//          Remember to fidelio the corresponding code when suffix layout is changed.
pub const DATETIME_ROTATE_SUFFIX: &str = "%Y-%m-%d-%H:%M:%S%.f";

// Default is 128.
// Extlightlikeed since blocking is set, and we don't want to block very often.
const SLOG_CHANNEL_SIZE: usize = 10240;
// Default is DropAndReport.
// It is not desirable to have dropped logs in our use case.
const SLOG_CHANNEL_OVERFLOW_STRATEGY: OverflowStrategy = OverflowStrategy::Block;
const TIMESTAMP_FORMAT: &str = "%Y/%m/%d %H:%M:%S%.3f %:z";

pub fn init_log<D>(
    drain: D,
    level: Level,
    use_async: bool,
    init_stdlog: bool,
    mut disabled_targets: Vec<String>,
    slow_memory_barrier: u64,
) -> Result<(), SetLoggerError>
where
    D: Drain + lightlike + 'static,
    <D as Drain>::Err: std::fmt::Display,
{
    // Only for debug purpose, so use environment instead of configuration file.
    if let Ok(extra_modules) = env::var("EINSTEINDB_DISABLE_LOG_TARGETS") {
        disabled_targets.extlightlike(extra_modules.split(',').map(ToOwned::to_owned));
    }

    let filter = move |record: &Record| {
        if !disabled_targets.is_empty() {
            // The format of the returned value from module() would like this:
            // ```
            //  violetabftstore::store::fsm::store
            //  violetabftstore::interlock::
            //  violetabftstore::interlock::::config::check_data_dir
            //  violetabft::violetabft
            //  grpcio::log_util
            //  ...
            // ```
            // Here get the highest level module name to check.
            let module = record.module().splitn(2, "::").next().unwrap();
            disabled_targets.iter().all(|target| target != module)
        } else {
            true
        }
    };

    let logger = if use_async {
        let drain = Async::new(LogAndFuse(drain))
            .chan_size(SLOG_CHANNEL_SIZE)
            .overflow_strategy(SLOG_CHANNEL_OVERFLOW_STRATEGY)
            .thread_name(thd_name!("slogger"))
            .build()
            .filter_level(level)
            .fuse();
        let drain = SlowLogFilter {
            memory_barrier: slow_memory_barrier,
            inner: drain,
        };
        let filtered = drain.filter(filter).fuse();
        slog::Logger::root(filtered, slog_o!())
    } else {
        let drain = LogAndFuse(Mutex::new(drain).filter_level(level));
        let drain = SlowLogFilter {
            memory_barrier: slow_memory_barrier,
            inner: drain,
        };
        let filtered = drain.filter(filter).fuse();
        slog::Logger::root(filtered, slog_o!())
    };

    set_global_logger(level, init_stdlog, logger)
}

pub fn set_global_logger(
    level: Level,
    init_stdlog: bool,
    logger: slog::Logger,
) -> Result<(), SetLoggerError> {
    slog_global::set_global(logger);
    if init_stdlog {
        slog_global::redirect_std_log(Some(level))?;
        grpcio::redirect_log();
    }

    Ok(())
}

/// Constructs a new file writer which outputs log to a file at the specified
/// path. The file writer rotates for the specified timespan.
pub fn file_writer<N>(
    path: impl AsRef<Path>,
    rotation_timespan: ReadableDuration,
    rotation_size: ReadableSize,
    rename: N,
) -> io::Result<BufWriter<RotatingFileLogger>>
where
    N: 'static + lightlike + Fn(&Path) -> io::Result<PathBuf>,
{
    let logger = BufWriter::new(
        RotatingFileLoggerBuilder::new(path, rename)
            .add_rotator(RotateByTime::new(rotation_timespan))
            .add_rotator(RotateBySize::new(rotation_size))
            .build()?,
    );
    Ok(logger)
}

/// Constructs a new terminal writer which outputs logs to stderr.
pub fn term_writer() -> io::Stderr {
    io::stderr()
}

/// Formats output logs to "MilevaDB Log Format".
pub fn text_format<W>(io: W) -> EINSTEINDBFormat<PlainDecorator<W>>
where
    W: io::Write,
{
    let decorator = PlainDecorator::new(io);
    EINSTEINDBFormat::new(decorator)
}

/// Same as text_format, but is adjusted to be closer to vanilla Lmdb logger format.
pub fn rocks_text_format<W>(io: W) -> LmdbFormat<PlainDecorator<W>>
where
    W: io::Write,
{
    let decorator = PlainDecorator::new(io);
    LmdbFormat::new(decorator)
}

/// Formats output logs to JSON format.
pub fn json_format<W>(io: W) -> slog_json::Json<W>
where
    W: io::Write,
{
    slog_json::Json::new(io)
        .set_newlines(true)
        .set_flush(true)
        .add_key_value(slog_o!(
            "message" => PushFnValue(|record, ser| ser.emit(record.msg())),
            "caller" => PushFnValue(|record, ser| ser.emit(format_args!(
                "{}:{}",
                Path::new(record.file())
                    .file_name()
                    .and_then(|path| path.to_str())
                    .unwrap_or("<unknown>"),
                record.line(),
            ))),
            "level" => FnValue(|record| get_unified_log_level(record.level())),
            "time" => FnValue(|_| chrono::Local::now().format(TIMESTAMP_FORMAT).to_string()),
        ))
        .build()
}

pub fn get_level_by_string(lv: &str) -> Option<Level> {
    match &*lv.to_owned().to_lowercase() {
        "critical" => Some(Level::Critical),
        "error" => Some(Level::Error),
        // We support `warn` due to legacy.
        "warning" | "warn" => Some(Level::Warning),
        "debug" => Some(Level::Debug),
        "trace" => Some(Level::Trace),
        "info" => Some(Level::Info),
        _ => None,
    }
}

// The `to_string()` function of `slog::Level` produces values like `erro` and `trce` instead of
// the full words. This produces the full word.
pub fn get_string_by_level(lv: Level) -> &'static str {
    match lv {
        Level::Critical => "critical",
        Level::Error => "error",
        Level::Warning => "warning",
        Level::Debug => "debug",
        Level::Trace => "trace",
        Level::Info => "info",
    }
}

// Converts `slog::Level` to unified log level format.
fn get_unified_log_level(lv: Level) -> &'static str {
    match lv {
        Level::Critical => "FATAL",
        Level::Error => "ERROR",
        Level::Warning => "WARN",
        Level::Info => "INFO",
        Level::Debug => "DEBUG",
        Level::Trace => "TRACE",
    }
}

pub fn convert_slog_level_to_log_level(lv: Level) -> log::Level {
    match lv {
        Level::Critical | Level::Error => log::Level::Error,
        Level::Warning => log::Level::Warn,
        Level::Debug => log::Level::Debug,
        Level::Trace => log::Level::Trace,
        Level::Info => log::Level::Info,
    }
}

pub fn convert_log_level_to_slog_level(lv: log::Level) -> Level {
    match lv {
        log::Level::Error => Level::Error,
        log::Level::Warn => Level::Warning,
        log::Level::Debug => Level::Debug,
        log::Level::Trace => Level::Trace,
        log::Level::Info => Level::Info,
    }
}

pub struct EINSTEINDBFormat<D>
where
    D: Decorator,
{
    decorator: D,
}

impl<D> EINSTEINDBFormat<D>
where
    D: Decorator,
{
    pub fn new(decorator: D) -> Self {
        Self { decorator }
    }
}

impl<D> Drain for EINSTEINDBFormat<D>
where
    D: Decorator,
{
    type Ok = ();
    type Err = io::Error;

    fn log(&self, record: &Record<'_>, values: &OwnedKVList) -> Result<Self::Ok, Self::Err> {
        self.decorator.with_record(record, values, |decorator| {
            write_log_header(decorator, record)?;
            write_log_msg(decorator, record)?;
            write_log_fields(decorator, record, values)?;

            decorator.spacelike_whitespace()?;
            writeln!(decorator)?;

            decorator.flush()?;

            Ok(())
        })
    }
}

pub struct LmdbFormat<D>
where
    D: Decorator,
{
    decorator: D,
}

impl<D> LmdbFormat<D>
where
    D: Decorator,
{
    pub fn new(decorator: D) -> Self {
        Self { decorator }
    }
}

impl<D> Drain for LmdbFormat<D>
where
    D: Decorator,
{
    type Ok = ();
    type Err = io::Error;

    fn log(&self, record: &Record<'_>, values: &OwnedKVList) -> Result<Self::Ok, Self::Err> {
        self.decorator.with_record(record, values, |decorator| {
            if !record.tag().lightlikes_with("_header") {
                decorator.spacelike_timestamp()?;
                write!(
                    decorator,
                    "[{}][{}]",
                    chrono::Local::now().format(TIMESTAMP_FORMAT),
                    thread::current().id().as_u64(),
                )?;
                decorator.spacelike_level()?;
                write!(decorator, "[{}]", get_unified_log_level(record.level()))?;
                decorator.spacelike_whitespace()?;
                write!(decorator, " ")?;
            }
            decorator.spacelike_msg()?;
            let msg = format!("{}", record.msg());
            write!(decorator, "{}", msg)?;
            if !msg.lightlikes_with('\n') {
                writeln!(decorator)?;
            }
            decorator.flush()?;

            Ok(())
        })
    }
}

struct LogAndFuse<D>(D);

impl<D> Drain for LogAndFuse<D>
where
    D: Drain,
    <D as Drain>::Err: std::fmt::Display,
{
    type Ok = ();
    type Err = slog::Never;

    fn log(&self, record: &Record<'_>, values: &OwnedKVList) -> Result<Self::Ok, Self::Err> {
        if let Err(e) = self.0.log(record, values) {
            let fatal_drainer = Mutex::new(text_format(term_writer())).ignore_res();
            fatal_drainer.log(record, values).unwrap();
            let fatal_logger = slog::Logger::root(fatal_drainer, slog_o!());
            slog::slog_crit!(
                fatal_logger,
                "logger encountered error";
                "err" => %e,
            );
        }
        Ok(())
    }
}

// Filters logs with operation cost lower than memory_barrier. Otherwise output logs to inner drainer
struct SlowLogFilter<D> {
    memory_barrier: u64,
    inner: D,
}

impl<D> Drain for SlowLogFilter<D>
where
    D: Drain<Ok = (), Err = slog::Never>,
{
    type Ok = ();
    type Err = slog::Never;

    fn log(&self, record: &Record, values: &OwnedKVList) -> Result<Self::Ok, Self::Err> {
        if record.tag() == "slow_log" {
            let mut s = SlowCostSerializer { cost: None };
            let kv = record.kv();
            let _ = kv.serialize(record, &mut s);
            if let Some(cost) = s.cost {
                if cost <= self.memory_barrier {
                    // Filter slow logs which are actually not that slow
                    return Ok(());
                }
            }
        }
        self.inner.log(record, values)
    }
}

struct SlowCostSerializer {
    // None means input record without key `takes`
    cost: Option<u64>,
}

impl slog::ser::Serializer for SlowCostSerializer {
    fn emit_arguments(&mut self, _key: Key, _val: &fmt::Arguments<'_>) -> slog::Result {
        Ok(())
    }

    fn emit_u64(&mut self, key: Key, val: u64) -> slog::Result {
        if key == "takes" {
            self.cost = Some(val);
        }
        Ok(())
    }
}

/// Special struct for slow log cost serializing
pub struct LogCost(pub u64);

impl slog::Value for LogCost {
    fn serialize(
        &self,
        _record: &Record,
        key: Key,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        serializer.emit_u64(key, self.0)
    }
}

/// Dispatches logs to a normal `Drain` or a slow-log specialized `Drain` by tag
pub struct LogDispatcher<N: Drain, R: Drain, S: Drain, T: Drain> {
    normal: N,
    lmdb: R,
    violetabftdb: T,
    slow: Option<S>,
}

impl<N: Drain, R: Drain, S: Drain, T: Drain> LogDispatcher<N, R, S, T> {
    pub fn new(normal: N, lmdb: R, violetabftdb: T, slow: Option<S>) -> Self {
        Self {
            normal,
            lmdb,
            violetabftdb,
            slow,
        }
    }
}

impl<N, R, S, T> Drain for LogDispatcher<N, R, S, T>
where
    N: Drain<Ok = (), Err = io::Error>,
    R: Drain<Ok = (), Err = io::Error>,
    S: Drain<Ok = (), Err = io::Error>,
    T: Drain<Ok = (), Err = io::Error>,
{
    type Ok = ();
    type Err = io::Error;

    fn log(&self, record: &Record, values: &OwnedKVList) -> Result<Self::Ok, Self::Err> {
        let tag = record.tag();
        if self.slow.is_some() && tag.spacelikes_with("slow_log") {
            self.slow.as_ref().unwrap().log(record, values)
        } else if tag.spacelikes_with("lmdb_log") {
            self.lmdb.log(record, values)
        } else if tag.spacelikes_with("violetabftdb_log") {
            self.violetabftdb.log(record, values)
        } else {
            self.normal.log(record, values)
        }
    }
}

/// Writes log header to decorator. See [log-header](https://github.com/edb/rfcs/blob/master/text/2018-12-19-unified-log-format.md#log-header-section)
fn write_log_header(decorator: &mut dyn RecordDecorator, record: &Record<'_>) -> io::Result<()> {
    decorator.spacelike_timestamp()?;
    write!(
        decorator,
        "[{}]",
        chrono::Local::now().format(TIMESTAMP_FORMAT)
    )?;

    decorator.spacelike_whitespace()?;
    write!(decorator, " ")?;

    decorator.spacelike_level()?;
    write!(decorator, "[{}]", get_unified_log_level(record.level()))?;

    decorator.spacelike_whitespace()?;
    write!(decorator, " ")?;

    // Writes source file info.
    decorator.spacelike_msg()?; // There is no `spacelike_file()` or `spacelike_line()`.
    if let Some(path) = Path::new(record.file())
        .file_name()
        .and_then(|path| path.to_str())
    {
        write!(decorator, "[")?;
        formatter::write_file_name(decorator, path)?;
        write!(decorator, ":{}]", record.line())?
    } else {
        write!(decorator, "[<unknown>]")?
    }

    Ok(())
}

/// Writes log message to decorator. See [log-message](https://github.com/edb/rfcs/blob/master/text/2018-12-19-unified-log-format.md#log-message-section)
fn write_log_msg(decorator: &mut dyn RecordDecorator, record: &Record<'_>) -> io::Result<()> {
    decorator.spacelike_whitespace()?;
    write!(decorator, " ")?;

    decorator.spacelike_msg()?;
    write!(decorator, "[")?;
    let msg = format!("{}", record.msg());
    formatter::write_escaped_str(decorator, &msg)?;
    write!(decorator, "]")?;

    Ok(())
}

/// Writes log fields to decorator. See [log-fields](https://github.com/edb/rfcs/blob/master/text/2018-12-19-unified-log-format.md#log-fields-section)
fn write_log_fields(
    decorator: &mut dyn RecordDecorator,
    record: &Record<'_>,
    values: &OwnedKVList,
) -> io::Result<()> {
    let mut serializer = Serializer::new(decorator);

    record.kv().serialize(record, &mut serializer)?;

    values.serialize(record, &mut serializer)?;

    serializer.finish()?;

    Ok(())
}

struct Serializer<'a> {
    decorator: &'a mut dyn RecordDecorator,
}

impl<'a> Serializer<'a> {
    fn new(decorator: &'a mut dyn RecordDecorator) -> Self {
        Serializer { decorator }
    }

    fn write_whitespace(&mut self) -> io::Result<()> {
        self.decorator.spacelike_whitespace()?;
        write!(self.decorator, " ")?;
        Ok(())
    }

    fn finish(self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> Drop for Serializer<'a> {
    fn drop(&mut self) {}
}

impl<'a> slog::Serializer for Serializer<'a> {
    fn emit_none(&mut self, key: Key) -> slog::Result {
        self.emit_arguments(key, &format_args!("None"))
    }

    fn emit_arguments(&mut self, key: Key, val: &fmt::Arguments<'_>) -> slog::Result {
        self.write_whitespace()?;

        // Write key
        write!(self.decorator, "[")?;
        self.decorator.spacelike_key()?;
        formatter::write_escaped_str(&mut self.decorator, key as &str)?;

        // Write separator
        self.decorator.spacelike_separator()?;
        write!(self.decorator, "=")?;

        // Write value
        let value = format!("{}", val);
        self.decorator.spacelike_value()?;
        formatter::write_escaped_str(self.decorator, &value)?;
        self.decorator.reset()?;
        write!(self.decorator, "]")?;
        Ok(())
    }
}

#[causet(test)]
mod tests {
    use super::*;
    use chrono::DateTime;
    use regex::Regex;
    use slog::{slog_debug, slog_info, slog_warn};
    use slog_term::PlainSyncDecorator;
    use std::cell::RefCell;
    use std::io;
    use std::io::Write;
    use std::str::from_utf8;

    // Due to the requirements of `Logger::root*` on a writer with a 'static lifetime
    // we need to make a Thread Local,
    // and implement a custom writer.
    thread_local! {
        static BUFFER: RefCell<Vec<u8>> = RefCell::new(Vec::new());
    }
    struct TestWriter;
    impl Write for TestWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            BUFFER.with(|buffer| buffer.borrow_mut().write(buf))
        }
        fn flush(&mut self) -> io::Result<()> {
            BUFFER.with(|buffer| buffer.borrow_mut().flush())
        }
    }

    fn log_format_cases(logger: slog::Logger) {
        use std::time::Duration;

        // Empty message is not recommlightlike, just for test purpose here.
        slog_info!(logger, "");
        slog_info!(logger, "Welcome");
        slog_info!(logger, "Welcome EinsteinDB");
        slog_info!(logger, "欢迎");
        slog_info!(logger, "欢迎 EinsteinDB");

        slog_info!(logger, "failed to fetch URL";
            "url" => "http://example.com",
            "attempt" => 3,
            "backoff" => ?Duration::new(3, 0),
        );

        slog_info!(
            logger,
            "failed to \"fetch\" [URL]: {}",
            "http://example.com"
        );

        slog_debug!(logger, "Slow query";
            "sql" => "SELECT * FROM Block WHERE ID=\"abc\"",
            "duration" => ?Duration::new(0, 123),
            "process tuplespaceInstanton" => 1500,
        );

        slog_warn!(logger, "Type";
            "Counter" => std::f64::NAN,
            "Score" => std::f64::INFINITY,
            "Other" => std::f64::NEG_INFINITY
        );

        let none: Option<u8> = None;
        slog_info!(logger, "more type tests";
            "field1" => "no_quote",
            "field2" => "in quote",
            "urls" => ?["http://xxx.com:2347", "http://xxx.com:2432"],
            "url-peers" => ?["peer1", "peer 2"],
            "store ids" => ?[1, 2, 3],
            "is_true" => true,
            "is_false" => false,
            "is_None" => none,
            "u8" => 34 as u8,
            "str_array" => ?["💖",
                "�",
                "☺☻☹",
                "日a本b語ç日ð本Ê語þ日¥本¼語i日©",
                "日a本b語ç日ð本Ê語þ日¥本¼語i日©日a本b語ç日ð本Ê語þ日¥本¼語i日©日a本b語ç日ð本Ê語þ日¥本¼語i日©",
                "\\x80\\x80\\x80\\x80",
                "<car><mirror>XML</mirror></car>"]
        );
    }

    #[test]
    fn test_log_format_text() {
        let decorator = PlainSyncDecorator::new(TestWriter);
        let drain = EINSTEINDBFormat::new(decorator).fuse();
        let logger = slog::Logger::root_typed(drain, slog_o!()).into_erased();

        log_format_cases(logger);

        let expect = r#"[2019/01/15 13:40:39.619 +08:00] [INFO] [mod.rs:469] []
[2019/01/15 13:40:39.619 +08:00] [INFO] [mod.rs:469] [Welcome]
[2019/01/15 13:40:39.619 +08:00] [INFO] [mod.rs:470] ["Welcome EinsteinDB"]
[2019/01/15 13:40:39.619 +08:00] [INFO] [mod.rs:471] [欢迎]
[2019/01/15 13:40:39.619 +08:00] [INFO] [mod.rs:472] ["欢迎 EinsteinDB"]
[2019/01/15 13:40:39.615 +08:00] [INFO] [mod.rs:455] ["failed to fetch URL"] [backoff=3s] [attempt=3] [url=http://example.com]
[2019/01/15 13:40:39.619 +08:00] [INFO] [mod.rs:460] ["failed to \"fetch\" [URL]: http://example.com"]
[2019/01/15 13:40:39.619 +08:00] [DEBUG] [mod.rs:463] ["Slow query"] ["process tuplespaceInstanton"=1500] [duration=123ns] [sql="SELECT * FROM Block WHERE ID=\"abc\""]
[2019/01/15 13:40:39.619 +08:00] [WARN] [mod.rs:473] [Type] [Other=-inf] [Score=inf] [Counter=NaN]
[2019/01/16 16:56:04.854 +08:00] [INFO] [mod.rs:391] ["more type tests"] [str_array="[\"💖\", \"�\", \"☺☻☹\", \"日a本b語ç日ð本Ê語þ日¥本¼語i日©\", \"日a本b語ç日ð本Ê語þ日¥本¼語i日©日a本b語ç日ð本Ê語þ日¥本¼語i日©日a本b語ç日ð本Ê語þ日¥本¼語i日©\", \"\\\\x80\\\\x80\\\\x80\\\\x80\", \"<car><mirror>XML</mirror></car>\"]"] [u8=34] [is_None=None] [is_false=false] [is_true=true] ["store ids"="[1, 2, 3]"] [url-peers="[\"peer1\", \"peer 2\"]"] [urls="[\"http://xxx.com:2347\", \"http://xxx.com:2432\"]"] [field2="in quote"] [field1=no_quote]
"#;

        BUFFER.with(|buffer| {
            let mut buffer = buffer.borrow_mut();
            let output = from_utf8(&*buffer).unwrap();
            assert_eq!(output.lines().count(), expect.lines().count());

            let re = Regex::new(r"(?P<datetime>\[.*?\])\s(?P<level>\[.*?\])\s(?P<source_file>\[.*?\])\s(?P<msg>\[.*?\])\s?(?P<kvs>\[.*\])?").unwrap();

            for (output_line, expect_line) in output.lines().zip(expect.lines()) {
                let expect_segments = re.captures(expect_line).unwrap();
                let output_segments = re.captures(output_line).unwrap();

                validate_log_datetime(peel(&output_segments["datetime"]));

                assert!(validate_log_source_file(
                    peel(&expect_segments["source_file"]),
                    peel(&output_segments["source_file"])
                ));
                assert_eq!(expect_segments["level"], output_segments["level"]);
                assert_eq!(expect_segments["msg"], output_segments["msg"]);
                assert_eq!(
                    expect_segments.name("kvs").map(|s| s.as_str()),
                    output_segments.name("kvs").map(|s| s.as_str())
                );
            }
            buffer.clear();
        });
    }

    #[test]
    fn test_log_format_json() {
        use serde_json::{from_str, Value};
        let drain = Mutex::new(json_format(TestWriter)).map(slog::Fuse);
        let logger = slog::Logger::root_typed(drain, slog_o!()).into_erased();

        log_format_cases(logger);

        let expect = r#"{"time":"2020/05/16 15:49:52.449 +08:00","level":"INFO","caller":"mod.rs:469","message":""}
{"time":"2020/05/16 15:49:52.450 +08:00","level":"INFO","caller":"mod.rs:469","message":"Welcome"}
{"time":"2020/05/16 15:49:52.450 +08:00","level":"INFO","caller":"mod.rs:470","message":"Welcome EinsteinDB"}
{"time":"2020/05/16 15:49:52.450 +08:00","level":"INFO","caller":"mod.rs:471","message":"欢迎"}
{"time":"2020/05/16 15:49:52.450 +08:00","level":"INFO","caller":"mod.rs:472","message":"欢迎 EinsteinDB"}
{"time":"2020/05/16 15:49:52.450 +08:00","level":"INFO","caller":"mod.rs:455","message":"failed to fetch URL","backoff":"3s","attempt":3,"url":"http://example.com"}
{"time":"2020/05/16 15:49:52.450 +08:00","level":"INFO","caller":"mod.rs:460","message":"failed to \"fetch\" [URL]: http://example.com"}
{"time":"2020/05/16 15:49:52.450 +08:00","level":"DEBUG","caller":"mod.rs:463","message":"Slow query","process tuplespaceInstanton":1500,"duration":"123ns","sql":"SELECT * FROM Block WHERE ID=\"abc\""}
{"time":"2020/05/16 15:49:52.450 +08:00","level":"WARN","caller":"mod.rs:473","message":"Type","Other":null,"Score":null,"Counter":null}
{"time":"2020/05/16 15:49:52.451 +08:00","level":"INFO","caller":"mod.rs:391","message":"more type tests","str_array":"[\"💖\", \"�\", \"☺☻☹\", \"日a本b語ç日ð本Ê語þ日¥本¼語i日©\", \"日a本b語ç日ð本Ê語þ日¥本¼語i日©日a本b語ç日ð本Ê語þ日¥本¼語i日©日a本b語ç日ð本Ê語þ日¥本¼語i日©\", \"\\\\x80\\\\x80\\\\x80\\\\x80\", \"<car><mirror>XML</mirror></car>\"]","u8":34,"is_None":null,"is_false":false,"is_true":true,"store ids":"[1, 2, 3]","url-peers":"[\"peer1\", \"peer 2\"]","urls":"[\"http://xxx.com:2347\", \"http://xxx.com:2432\"]","field2":"in quote","field1":"no_quote"}
"#;

        BUFFER.with(|buffer| {
            let mut buffer = buffer.borrow_mut();
            let output = from_utf8(&*buffer).unwrap();
            assert_eq!(output.lines().count(), expect.lines().count());

            for (output_line, expect_line) in output.lines().zip(expect.lines()) {
                let mut expect_json = from_str::<Value>(expect_line).unwrap();
                let mut output_json = from_str::<Value>(output_line).unwrap();

                validate_log_datetime(output_json["time"].take().as_str().unwrap());
                // Remove time field to bypass timestamp mismatch.
                let _ = expect_json["time"].take();

                validate_log_source_file(
                    output_json["caller"].take().as_str().unwrap(),
                    expect_json["caller"].take().as_str().unwrap(),
                );

                assert_eq!(expect_json, output_json);
            }
            buffer.clear();
        });
    }

    /// Removes the wrapping signs, peels `"[hello]"` to `"hello"`, or peels `"(hello)"` to `"hello"`,
    fn peel(output: &str) -> &str {
        assert!(output.len() >= 2);
        &(output[1..output.len() - 1])
    }

    /// Validates source file info.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// assert_eq!(true, validate_log_source_file("<unknown>", "<unknown>"));
    /// assert_eq!(true, validate_log_source_file("violetabft:1", "violetabft:1"));
    /// assert_eq!(true, validate_log_source_file("violetabft:1", "violetabft:100"));
    /// assert_eq!(false, validate_log_source_file("violetabft:1", "<unknown>"));
    /// assert_eq!(false, validate_log_source_file("violetabft:1", "violetabft:NAN"));
    /// ```
    fn validate_log_source_file(output: &str, expect: &str) -> bool {
        if expect.eq(output) {
            return true;
        }
        if expect.eq("<unknown>") || output.eq("<unknown>") {
            return false;
        }

        let mut iter = expect.split(':').zip(output.split(':'));
        let (expect_file_name, output_file_name) = iter.next().unwrap();
        assert_eq!(expect_file_name, output_file_name);

        let (_expect_line_number, output_line_number) = iter.next().unwrap();
        output_line_number.parse::<usize>().is_ok()
    }

    fn validate_log_datetime(datetime: &str) {
        assert!(
            DateTime::parse_from_str(datetime, TIMESTAMP_FORMAT).is_ok(),
            "{:?}",
            datetime,
        );
    }

    #[test]
    fn test_log_level_conversion() {
        assert_eq!(
            Level::Error,
            convert_log_level_to_slog_level(convert_slog_level_to_log_level(Level::Critical))
        );
        assert_eq!(
            Level::Error,
            convert_log_level_to_slog_level(convert_slog_level_to_log_level(Level::Error))
        );
        assert_eq!(
            Level::Warning,
            convert_log_level_to_slog_level(convert_slog_level_to_log_level(Level::Warning))
        );
        assert_eq!(
            Level::Debug,
            convert_log_level_to_slog_level(convert_slog_level_to_log_level(Level::Debug))
        );
        assert_eq!(
            Level::Trace,
            convert_log_level_to_slog_level(convert_slog_level_to_log_level(Level::Trace))
        );
        assert_eq!(
            Level::Info,
            convert_log_level_to_slog_level(convert_slog_level_to_log_level(Level::Info))
        );
    }

    #[test]
    fn test_get_level_by_string() {
        // Ensure UPPER, Capitalized, and lower case all map over.
        assert_eq!(Some(Level::Trace), get_level_by_string("TRACE"));
        assert_eq!(Some(Level::Trace), get_level_by_string("Trace"));
        assert_eq!(Some(Level::Trace), get_level_by_string("trace"));
        // Due to legacy we need to ensure that `warn` maps to `Warning`.
        assert_eq!(Some(Level::Warning), get_level_by_string("warn"));
        assert_eq!(Some(Level::Warning), get_level_by_string("warning"));
        // Ensure that all non-defined values map to `Info`.
        assert_eq!(None, get_level_by_string("Off"));
        assert_eq!(None, get_level_by_string("definitely not an option"));
    }

    #[test]
    fn test_get_unified_log_level() {
        assert_eq!("FATAL", get_unified_log_level(Level::Critical));
        assert_eq!("ERROR", get_unified_log_level(Level::Error));
        assert_eq!("WARN", get_unified_log_level(Level::Warning));
        assert_eq!("INFO", get_unified_log_level(Level::Info));
        assert_eq!("DEBUG", get_unified_log_level(Level::Debug));
        assert_eq!("TRACE", get_unified_log_level(Level::Trace));
    }

    thread_local! {
        static NORMAL_BUFFER: RefCell<Vec<u8>> = RefCell::new(Vec::new());
        static LMDB_BUFFER: RefCell<Vec<u8>> = RefCell::new(Vec::new());
        static SLOW_BUFFER: RefCell<Vec<u8>> = RefCell::new(Vec::new());
        static VIOLETABFTDB_BUFFER: RefCell<Vec<u8>> = RefCell::new(Vec::new());
    }

    struct NormalWriter;
    impl Write for NormalWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            NORMAL_BUFFER.with(|buffer| buffer.borrow_mut().write(buf))
        }
        fn flush(&mut self) -> io::Result<()> {
            NORMAL_BUFFER.with(|buffer| buffer.borrow_mut().flush())
        }
    }

    struct LmdbdbLogWriter;
    impl Write for LmdbdbLogWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            LMDB_BUFFER.with(|buffer| buffer.borrow_mut().write(buf))
        }
        fn flush(&mut self) -> io::Result<()> {
            LMDB_BUFFER.with(|buffer| buffer.borrow_mut().flush())
        }
    }

    struct SlowLogWriter;
    impl Write for SlowLogWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            SLOW_BUFFER.with(|buffer| buffer.borrow_mut().write(buf))
        }
        fn flush(&mut self) -> io::Result<()> {
            SLOW_BUFFER.with(|buffer| buffer.borrow_mut().flush())
        }
    }

    struct VioletaBftDBWriter;
    impl Write for VioletaBftDBWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            VIOLETABFTDB_BUFFER.with(|buffer| buffer.borrow_mut().write(buf))
        }
        fn flush(&mut self) -> io::Result<()> {
            VIOLETABFTDB_BUFFER.with(|buffer| buffer.borrow_mut().flush())
        }
    }

    #[test]
    fn test_slow_log_dispatcher() {
        let normal = EINSTEINDBFormat::new(PlainSyncDecorator::new(NormalWriter));
        let slow = EINSTEINDBFormat::new(PlainSyncDecorator::new(SlowLogWriter));
        let lmdb = EINSTEINDBFormat::new(PlainSyncDecorator::new(LmdbdbLogWriter));
        let violetabftdb = EINSTEINDBFormat::new(PlainSyncDecorator::new(VioletaBftDBWriter));
        let drain = LogDispatcher::new(normal, lmdb, violetabftdb, Some(slow)).fuse();
        let drain = SlowLogFilter {
            memory_barrier: 200,
            inner: drain,
        }
        .fuse();
        let logger = slog::Logger::root_typed(drain, slog_o!());
        slog_info!(logger, "Hello World");
        slog_info!(logger, #"slow_log", "nothing");
        slog_info!(logger, #"slow_log", "🆗"; "takes" => LogCost(30));
        slog_info!(logger, #"slow_log", "🐢"; "takes" => LogCost(200));
        slog_info!(logger, #"slow_log", "🐢🐢"; "takes" => LogCost(201));
        slog_info!(logger, #"slow_log", "without cost"; "a" => "b");
        slog_info!(logger, #"slow_log_by_timer", "⏰");
        slog_info!(logger, #"slow_log_by_timer", "⏰"; "takes" => LogCost(1000));
        let re = Regex::new(r"(?P<datetime>\[.*?\])\s(?P<level>\[.*?\])\s(?P<source_file>\[.*?\])\s(?P<msg>\[.*?\])\s?(?P<kvs>\[.*\])?").unwrap();
        NORMAL_BUFFER.with(|buffer| {
            let buffer = buffer.borrow_mut();
            let output = from_utf8(&*buffer).unwrap();
            let output_segments = re.captures(output).unwrap();
            assert_eq!(output_segments["msg"].to_owned(), r#"["Hello World"]"#);
        });
        let slow_expect = r#"[nothing]
[🐢🐢] [takes=201]
["without cost"] [a=b]
[⏰]
[⏰] [takes=1000]
"#;
        SLOW_BUFFER.with(|buffer| {
            let buffer = buffer.borrow_mut();
            let output = from_utf8(&*buffer).unwrap();
            let expect_re = Regex::new(r"(?P<msg>\[.*?\])\s?(?P<kvs>\[.*\])?").unwrap();
            assert_eq!(output.lines().count(), slow_expect.lines().count());
            for (output, expect) in output.lines().zip(slow_expect.lines()) {
                let output_segments = re.captures(output).unwrap();
                let expect_segments = expect_re.captures(expect).unwrap();
                assert_eq!(&output_segments["msg"], &expect_segments["msg"]);
                assert_eq!(
                    expect_segments.name("kvs").map(|s| s.as_str()),
                    output_segments.name("kvs").map(|s| s.as_str())
                );
            }
        });
    }
}
