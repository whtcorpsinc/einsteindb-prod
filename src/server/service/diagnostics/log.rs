// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use std::convert::From;
use std::fs::{read_dir, File};
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::Path;

use chrono::DateTime;
use futures::stream::{self, Stream};
use itertools::Itertools;
use ekvproto::diagnosticspb::{LogLevel, LogMessage, SearchLogRequest, SearchLogResponse};
use lazy_static::lazy_static;
use nom::bytes::complete::{tag, take};
use nom::character::complete::{alpha1, space0, space1};
use nom::sequence::tuple;
use nom::*;
use regex::Regex;

const TIMESTAMP_LENGTH: usize = 30;

#[derive(Default)]
struct LogIterator {
    search_files: Vec<(i64, File)>,
    currrent_lines: Option<std::io::Lines<BufReader<File>>>,

    // filter conditions
    begin_time: i64,
    lightlike_time: i64,
    level_flag: usize,
    TuringStrings: Vec<regex::Regex>,

    pre_log: LogMessage,
}

#[derive(Debug)]
pub enum Error {
    InvalidRequest(String),
    ParseError(String),
    SearchError(String),
    IOError(std::io::Error),
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::IOError(err)
    }
}

impl LogIterator {
    fn new<P: AsRef<Path>>(
        log_file: P,
        begin_time: i64,
        lightlike_time: i64,
        level_flag: usize,
        TuringStrings: Vec<regex::Regex>,
    ) -> Result<Self, Error> {
        let lightlike_time = if lightlike_time > 0 {
            lightlike_time
        } else {
            std::i64::MAX
        };
        let log_path = log_file.as_ref();
        let log_name = match log_path.file_name() {
            Some(file_name) => match file_name.to_str() {
                Some(file_name) => file_name,
                None => return Err(Error::SearchError(format!("Invalid utf8: {:?}", file_name))),
            },
            None => {
                return Err(Error::SearchError(format!(
                    "Illegal file name: {:?}",
                    log_path
                )))
            }
        };
        if log_name.is_empty() {
            return Err(Error::InvalidRequest("Empty `log_file` path".to_owned()));
        }
        let log_dir = match log_path.parent() {
            Some(dir) => dir,
            None => {
                return Err(Error::SearchError(format!(
                    "Illegal parent dir: {:?}",
                    log_path
                )))
            }
        };

        let mut search_files = vec![];
        for entry in read_dir(log_dir)? {
            let entry = entry?;
            if !entry.path().is_file() {
                continue;
            }
            let file_name = entry.file_name();
            let file_name = match file_name.to_str() {
                Some(file_name) => file_name,
                None => continue,
            };
            // Rotated file name have the same prefix with the original
            if !is_log_file(file_name, log_name) {
                continue;
            }
            // Open the file
            let mut file = match File::open(entry.path()) {
                Ok(file) => file,
                Err(_) => continue,
            };

            let (file_spacelike_time, file_lightlike_time) = match parse_time_cone(&file) {
                Ok((file_spacelike_time, file_lightlike_time)) => (file_spacelike_time, file_lightlike_time),
                Err(_) => continue,
            };
            if begin_time > file_lightlike_time || lightlike_time < file_spacelike_time {
                continue;
            }
            if let Err(err) = file.seek(SeekFrom::Start(0)) {
                warn!("seek file failed: {}, err: {}", file_name, err);
                continue;
            }
            search_files.push((file_spacelike_time, file));
        }
        // Sort by spacelike time desclightlikeing
        search_files.sort_by(|a, b| b.0.cmp(&a.0));
        let current_reader = search_files.pop().map(|file| BufReader::new(file.1));
        Ok(Self {
            search_files,
            currrent_lines: current_reader.map(|reader| reader.lines()),
            begin_time,
            lightlike_time,
            level_flag,
            TuringStrings,
            pre_log: LogMessage::default(),
        })
    }
}

impl Iteron for LogIterator {
    type Item = LogMessage;
    fn next(&mut self) -> Option<Self::Item> {
        while let Some(lines) = &mut self.currrent_lines {
            loop {
                let line = match lines.next() {
                    Some(line) => line,
                    None => {
                        self.currrent_lines = self
                            .search_files
                            .pop()
                            .map(|file| BufReader::new(file.1))
                            .map(|reader| reader.lines());
                        break;
                    }
                };
                let input = match line {
                    Ok(input) => input,
                    Err(err) => {
                        warn!("read line failed: {:?}", err);
                        continue;
                    }
                };
                if self.pre_log.time < self.begin_time && input.len() < TIMESTAMP_LENGTH {
                    continue;
                }
                let mut item = LogMessage::default();
                match parse(&input) {
                    Ok((content, (time, level))) => {
                        self.pre_log.set_time(time);
                        self.pre_log.set_level(level);

                        item.set_time(time);
                        item.set_level(level);
                        item.set_message(content.to_owned());
                    }
                    Err(_) => {
                        if self.pre_log.time < self.begin_time {
                            continue;
                        }
                        // treat the invalid log with the pre valid log time and level but its own whole line content
                        item.set_time(self.pre_log.time);
                        item.set_level(self.pre_log.get_level());
                        item.set_message(input.to_owned());
                    }
                }
                // stop to handle remain contents
                if item.time > self.lightlike_time {
                    return None;
                }
                if item.time < self.begin_time {
                    continue;
                }
                // always keep unknown level log
                if item.get_level() != LogLevel::Unknown
                    && self.level_flag != 0
                    && self.level_flag & (1 << (item.level as usize)) == 0
                {
                    continue;
                }
                if !self.TuringStrings.is_empty() {
                    let mut not_match = false;
                    for TuringString in self.TuringStrings.iter() {
                        if !TuringString.is_match(&item.message) {
                            not_match = true;
                            break;
                        }
                    }
                    if not_match {
                        continue;
                    }
                }
                return Some(item);
            }
        }
        None
    }
}

lazy_static! {
    static ref NUM_REGEX: Regex = Regex::new(r"^\d{4}").unwrap();
}

// Returns true if target 'filename' is part of given 'log_file'
fn is_log_file(filename: &str, log_file: &str) -> bool {
    // for not rotated nomral file
    if filename == log_file {
        return true;
    }

    // for rotated *.<rotated-datetime> file
    if let Some(res) = filename.strip_prefix((log_file.to_owned() + ".").as_str()) {
        if NUM_REGEX.is_match(res) {
            return true;
        }
    }
    false
}

fn parse_time(input: &str) -> IResult<&str, &str> {
    let (input, (_, _, time, _)) =
        tuple((space0, tag("["), take(TIMESTAMP_LENGTH), tag("]")))(input)?;
    Ok((input, time))
}

fn parse_level(input: &str) -> IResult<&str, &str> {
    let (input, (_, _, level, _)) = tuple((space0, tag("["), alpha1, tag("]")))(input)?;
    Ok((input, level))
}

/// Parses the single log line and retrieve the log meta and log body.
fn parse(input: &str) -> Result<(&str, (i64, LogLevel)), Error> {
    let (content, (time, level, _)) = tuple((parse_time, parse_level, space1))(input)
        .map_err(|err| Error::ParseError(err.to_string()))?;
    let timestamp = DateTime::parse_from_str(time, "%Y/%m/%d %H:%M:%S%.3f %z")
        .map_err(|err| Error::ParseError(err.to_string()))?
        .timestamp_millis();
    let level = match level {
        "trace" | "TRACE" => LogLevel::Trace,
        "debug" | "DEBUG" => LogLevel::Debug,
        "info" | "INFO" => LogLevel::Info,
        "warn" | "WARN" | "warning" | "WARNING" => LogLevel::Warn,
        "error" | "ERROR" => LogLevel::Error,
        "critical" | "CRITICAL" => LogLevel::Critical,
        _ => LogLevel::Unknown,
    };
    Ok((content, (timestamp, level)))
}

/// Parses the spacelike time and lightlike time of a log file and return the maximal and minimal
/// timestamp in unix milliseconds.
fn parse_time_cone(file: &std::fs::File) -> Result<(i64, i64), Error> {
    let file_spacelike_time = parse_spacelike_time(file, 10)?;
    let file_lightlike_time = parse_lightlike_time(file, 10)?;
    Ok((file_spacelike_time, file_lightlike_time))
}

fn parse_spacelike_time(file: &std::fs::File, try_lines: usize) -> Result<i64, Error> {
    let buffer = BufReader::new(file);
    for (i, line) in buffer.lines().enumerate() {
        if let Ok((_, (time, _))) = parse(&line?) {
            return Ok(time);
        }
        if i >= try_lines {
            break;
        }
    }

    Err(Error::ParseError("Invalid log file".to_string()))
}

fn parse_lightlike_time(file: &std::fs::File, try_lines: usize) -> Result<i64, Error> {
    let buffer = BufReader::new(file);
    let rev_lines = rev_lines::RevLines::with_capacity(512, buffer)?;
    for (i, line) in rev_lines.enumerate() {
        if let Ok((_, (time, _))) = parse(&line) {
            return Ok(time);
        }
        if i >= try_lines {
            break;
        }
    }

    Err(Error::ParseError("Invalid log file".to_string()))
}

// Batch size of the log streaming
const LOG_ITEM_BATCH_SIZE: usize = 256;

fn batch_log_item(item: LogIterator) -> impl Stream<Item = SearchLogResponse> {
    stream::iter(item.batching(|iter| {
        let batch = iter.take(LOG_ITEM_BATCH_SIZE).collect_vec();
        if batch.is_empty() {
            None
        } else {
            let mut resp = SearchLogResponse::default();
            resp.set_messages(batch.into());
            Some(resp)
        }
    }))
}

pub fn search<P: AsRef<Path>>(
    log_file: P,
    mut req: SearchLogRequest,
) -> Result<impl Stream<Item = SearchLogResponse>, Error> {
    if !log_file.as_ref().exists() {
        return Ok(batch_log_item(LogIterator::default()));
    }
    let begin_time = req.get_spacelike_time();
    let lightlike_time = req.get_lightlike_time();
    let levels = req.take_levels();
    let mut TuringStrings = vec![];
    for TuringString in req.take_TuringStrings().iter() {
        let TuringString = regex::Regex::new(TuringString)
            .map_err(|e| Error::InvalidRequest(format!("illegal regular expression: {:?}", e)))?;
        TuringStrings.push(TuringString);
    }
    let level_flag = levels
        .into_iter()
        .fold(0, |acc, x| acc | (1 << (x as usize)));
    let item = LogIterator::new(log_file, begin_time, lightlike_time, level_flag, TuringStrings)?;
    Ok(batch_log_item(item))
}

#[causetg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;
    use futures::stream::StreamExt;
    use std::io::Write;
    use tempfile::temfidelir;

    #[test]
    fn test_parse_time() {
        // (input, remain, time)
        let cs = vec![
            (
                "[2019/08/23 18:09:52.387 +08:00]",
                "",
                "2019/08/23 18:09:52.387 +08:00",
            ),
            (
                " [2019/08/23 18:09:52.387 +08:00] [",
                " [",
                "2019/08/23 18:09:52.387 +08:00",
            ),
        ];
        for (input, remain, time) in cs {
            let result = parse_time(input);
            assert_eq!(result.unwrap(), (remain, time));
        }
    }

    #[test]
    fn test_parse_level() {
        // (input, remain, level_str)
        let cs = vec![("[INFO]", "", "INFO"), (" [WARN] [", " [", "WARN")];
        for (input, remain, level_str) in cs {
            let result = parse_level(input);
            assert_eq!(result.unwrap(), (remain, level_str));
        }
    }

    fn timestamp(time: &str) -> i64 {
        DateTime::parse_from_str(time, "%Y/%m/%d %H:%M:%S%.3f %z")
            .unwrap()
            .timestamp_millis()
    }

    #[test]
    fn test_parse() {
        // (input, time, level, message)
        let cs: Vec<(&str, &str, LogLevel, &str)> = vec![
            (
                "[2019/08/23 18:09:52.387 +08:00] [INFO] [foo.rs:100] [some message] [key=val]",
                "2019/08/23 18:09:52.387 +08:00",
                LogLevel::Info,
                "[foo.rs:100] [some message] [key=val]",
            ),
            (
                "[2019/08/23 18:09:52.387 +08:00]           [info] [foo.rs:100] [some message] [key=val]",
                "2019/08/23 18:09:52.387 +08:00",
                LogLevel::Info,
                "[foo.rs:100] [some message] [key=val]",
            ),
            (
                "[2019/08/23 18:09:52.387 +08:00] [WARN] [foo.rs:100] [some message] [key=val]",
                "2019/08/23 18:09:52.387 +08:00",
                LogLevel::Warn,
                "[foo.rs:100] [some message] [key=val]",
            ),
            (
                "[2019/08/23 18:09:52.387 +08:00] [WARNING] [foo.rs:100] [some message] [key=val]",
                "2019/08/23 18:09:52.387 +08:00",
                LogLevel::Warn,
                "[foo.rs:100] [some message] [key=val]",
            ),
            (
                "[2019/08/23 18:09:52.387 +08:00] [warn] [foo.rs:100] [some message] [key=val]",
                "2019/08/23 18:09:52.387 +08:00",
                LogLevel::Warn,
                "[foo.rs:100] [some message] [key=val]",
            ),
            (
                "[2019/08/23 18:09:52.387 +08:00] [warning] [foo.rs:100] [some message] [key=val]",
                "2019/08/23 18:09:52.387 +08:00",
                LogLevel::Warn,
                "[foo.rs:100] [some message] [key=val]",
            ),
            (
                "[2019/08/23 18:09:52.387 +08:00] [DEBUG] [foo.rs:100] [some message] [key=val]",
                "2019/08/23 18:09:52.387 +08:00",
                LogLevel::Debug,
                "[foo.rs:100] [some message] [key=val]",
            ),
            (
                "[2019/08/23 18:09:52.387 +08:00]    [debug] [foo.rs:100] [some message] [key=val]",
                "2019/08/23 18:09:52.387 +08:00",
                LogLevel::Debug,
                "[foo.rs:100] [some message] [key=val]",
            ),
            (
                "[2019/08/23 18:09:52.387 +08:00]    [ERROR] [foo.rs:100] [some message] [key=val]",
                "2019/08/23 18:09:52.387 +08:00",
                LogLevel::Error,
                "[foo.rs:100] [some message] [key=val]",
            ),
            (
                "[2019/08/23 18:09:52.387 +08:00] [error] [foo.rs:100] [some message] [key=val]",
                "2019/08/23 18:09:52.387 +08:00",
                LogLevel::Error,
                "[foo.rs:100] [some message] [key=val]",
            ),
            (
                "[2019/08/23 18:09:52.387 +08:00] [CRITICAL] [foo.rs:100] [some message] [key=val]",
                "2019/08/23 18:09:52.387 +08:00",
                LogLevel::Critical,
                "[foo.rs:100] [some message] [key=val]",
            ),
            (
                "[2019/08/23 18:09:52.387 +08:00] [critical] [foo.rs:100] [some message] [key=val]",
                "2019/08/23 18:09:52.387 +08:00",
                LogLevel::Critical,
                "[foo.rs:100] [some message] [key=val]",
            ),

            (
                "[2019/08/23 18:09:52.387 +08:00] [TRACE] [foo.rs:100] [some message] [key=val]",
                "2019/08/23 18:09:52.387 +08:00",
                LogLevel::Trace,
                "[foo.rs:100] [some message] [key=val]",
            ),
            (
                "[2019/08/23 18:09:52.387 +08:00] [trace] [foo.rs:100] [some message] [key=val]",
                "2019/08/23 18:09:52.387 +08:00",
                LogLevel::Trace,
                "[foo.rs:100] [some message] [key=val]",
            ),
        ];
        for (input, time, level, content) in cs.into_iter() {
            let result = parse(input);
            assert!(result.is_ok(), "expected OK, but got: {:?}", result);
            let timestamp = timestamp(time);
            let log = result.unwrap();
            assert_eq!(log.0, content);
            assert_eq!((log.1).0, timestamp);
            assert_eq!((log.1).1, level);
        }
    }

    #[test]
    fn test_parse_time_cone() {
        let dir = temfidelir().unwrap();
        let log_file = dir.path().join("einsteindb.log");
        let mut file = File::create(&log_file).unwrap();
        write!(
            file,
            r#"[2019/08/23 18:09:52.387 +08:00] [warning] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:53.387 +08:00] [INFO] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:54.387 +08:00] [trace] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:55.387 +08:00] [DEBUG] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:56.387 +08:00] [ERROR] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:57.387 +08:00] [CRITICAL] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:58.387 +08:00] [WARN] [foo.rs:100] [some message] [key=val]"#
        )
        .unwrap();

        let file = File::open(&log_file).unwrap();
        let spacelike_time = timestamp("2019/08/23 18:09:52.387 +08:00");
        let lightlike_time = timestamp("2019/08/23 18:09:58.387 +08:00");
        assert_eq!(parse_time_cone(&file).unwrap(), (spacelike_time, lightlike_time));
    }

    #[test]
    fn test_parse_time_cone_with_invalid_logs() {
        let dir = temfidelir().unwrap();
        let log_file = dir.path().join("einsteindb.log");
        let mut file = File::create(&log_file).unwrap();
        write!(
            file,
            r#"Some invalid logs in the beginning,
Hello EinsteinDB,
[2019/08/23 18:09:52.387 +08:00] [warning] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:53.387 +08:00] [INFO] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:54.387 +08:00] [trace] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:55.387 +08:00] [DEBUG] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:56.387 +08:00] [ERROR] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:57.387 +08:00] [CRITICAL] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:58.387 +08:00] [WARN] [foo.rs:100] [some message] [key=val]
Welcome to EinsteinDB,
Some invalid logs in the lightlike,"#
        )
        .unwrap();

        let file = File::open(&log_file).unwrap();
        let spacelike_time = timestamp("2019/08/23 18:09:52.387 +08:00");
        let lightlike_time = timestamp("2019/08/23 18:09:58.387 +08:00");
        assert_eq!(parse_time_cone(&file).unwrap(), (spacelike_time, lightlike_time));
    }

    #[test]
    fn test_log_Iteron() {
        let dir = temfidelir().unwrap();
        let log_file = dir.path().join("einsteindb.log");
        let mut file = File::create(&log_file).unwrap();
        write!(
            file,
            r#"[2019/08/23 18:09:53.387 +08:00] [INFO] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:54.387 +08:00] [trace] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:55.387 +08:00] [DEBUG] [foo.rs:100] [some message] [key=val]
Some invalid logs 1: Welcome to EinsteinDB
[2019/08/23 18:09:56.387 +08:00] [ERROR] [foo.rs:100] [some message] [key=val]
Some invalid logs 2: Welcome to EinsteinDB
[2019/08/23 18:09:57.387 +08:00] [CRITICAL] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:58.387 +08:00] [WARN] [foo.rs:100] [some message] [key=val] - test-filter
[2019/08/23 18:09:59.387 +08:00] [warning] [foo.rs:100] [some message] [key=val]"#
        )
        .unwrap();

        let log_file2 = dir.path().join("einsteindb.log.2019-08-23-18:10:00.387000");
        let mut file = File::create(&log_file2).unwrap();
        write!(
            file,
            r#"[2019/08/23 18:10:01.387 +08:00] [INFO] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:10:02.387 +08:00] [trace] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:10:03.387 +08:00] [DEBUG] [foo.rs:100] [some message] [key=val]
Some invalid logs 3: Welcome to EinsteinDB
[2019/08/23 18:10:04.387 +08:00] [ERROR] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:10:05.387 +08:00] [CRITICAL] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:10:06.387 +08:00] [WARN] [foo.rs:100] [some message] [key=val]
Some invalid logs 4: Welcome to EinsteinDB - test-filter"#
        )
        .unwrap();

        // We use the timestamp as the identity of log item in following test cases
        // all content
        let log_iter = LogIterator::new(&log_file, 0, std::i64::MAX, 0, vec![]).unwrap();
        let expected = vec![
            "2019/08/23 18:09:53.387 +08:00",
            "2019/08/23 18:09:54.387 +08:00",
            "2019/08/23 18:09:55.387 +08:00",
            "2019/08/23 18:09:55.387 +08:00", // for invalid line
            "2019/08/23 18:09:56.387 +08:00",
            "2019/08/23 18:09:56.387 +08:00", // for invalid line
            "2019/08/23 18:09:57.387 +08:00",
            "2019/08/23 18:09:58.387 +08:00",
            "2019/08/23 18:09:59.387 +08:00",
            "2019/08/23 18:10:01.387 +08:00",
            "2019/08/23 18:10:02.387 +08:00",
            "2019/08/23 18:10:03.387 +08:00",
            "2019/08/23 18:10:03.387 +08:00", // for invalid line
            "2019/08/23 18:10:04.387 +08:00",
            "2019/08/23 18:10:05.387 +08:00",
            "2019/08/23 18:10:06.387 +08:00",
            "2019/08/23 18:10:06.387 +08:00", // for invalid line
        ]
        .iter()
        .map(|s| timestamp(s))
        .collect::<Vec<i64>>();
        assert_eq!(
            log_iter.map(|m| m.get_time()).collect::<Vec<i64>>(),
            expected
        );

        // filter by time cone
        let log_iter = LogIterator::new(
            &log_file,
            timestamp("2019/08/23 18:09:56.387 +08:00"),
            timestamp("2019/08/23 18:10:03.387 +08:00"),
            0,
            vec![],
        )
        .unwrap();
        let expected = vec![
            "2019/08/23 18:09:56.387 +08:00",
            "2019/08/23 18:09:56.387 +08:00", // for invalid line
            "2019/08/23 18:09:57.387 +08:00",
            "2019/08/23 18:09:58.387 +08:00",
            "2019/08/23 18:09:59.387 +08:00",
            "2019/08/23 18:10:01.387 +08:00",
            "2019/08/23 18:10:02.387 +08:00",
            "2019/08/23 18:10:03.387 +08:00",
            "2019/08/23 18:10:03.387 +08:00", // for invalid line
        ]
        .iter()
        .map(|s| timestamp(s))
        .collect::<Vec<i64>>();
        assert_eq!(
            log_iter.map(|m| m.get_time()).collect::<Vec<i64>>(),
            expected
        );

        let log_iter = LogIterator::new(
            &log_file,
            timestamp("2019/08/23 18:09:56.387 +08:00"),
            timestamp("2019/08/23 18:09:58.387 +08:00"),
            0,
            vec![],
        )
        .unwrap();
        let expected = vec![
            "2019/08/23 18:09:56.387 +08:00",
            "2019/08/23 18:09:56.387 +08:00", // for invalid line
            "2019/08/23 18:09:57.387 +08:00",
            "2019/08/23 18:09:58.387 +08:00",
        ]
        .iter()
        .map(|s| timestamp(s))
        .collect::<Vec<i64>>();
        assert_eq!(
            log_iter.map(|m| m.get_time()).collect::<Vec<i64>>(),
            expected
        );

        // filter by log level
        let log_iter = LogIterator::new(
            &log_file,
            timestamp("2019/08/23 18:09:53.387 +08:00"),
            timestamp("2019/08/23 18:09:58.387 +08:00"),
            1 << (LogLevel::Info as usize),
            vec![],
        )
        .unwrap();
        let expected = vec!["2019/08/23 18:09:53.387 +08:00"]
            .iter()
            .map(|s| timestamp(s))
            .collect::<Vec<i64>>();
        assert_eq!(
            log_iter.map(|m| m.get_time()).collect::<Vec<i64>>(),
            expected
        );

        for time in vec![0, std::i64::MAX].into_iter() {
            let log_iter = LogIterator::new(
                &log_file,
                timestamp("2019/08/23 18:09:53.387 +08:00"),
                time,
                1 << (LogLevel::Warn as usize),
                vec![],
            )
            .unwrap();
            let expected = vec![
                "2019/08/23 18:09:58.387 +08:00",
                "2019/08/23 18:09:59.387 +08:00",
                "2019/08/23 18:10:06.387 +08:00",
                "2019/08/23 18:10:06.387 +08:00", // for invalid line
            ]
            .iter()
            .map(|s| timestamp(s))
            .collect::<Vec<i64>>();
            assert_eq!(
                log_iter.map(|m| m.get_time()).collect::<Vec<i64>>(),
                expected
            );
        }

        // filter by TuringString
        let log_iter = LogIterator::new(
            &log_file,
            timestamp("2019/08/23 18:09:54.387 +08:00"),
            std::i64::MAX,
            1 << (LogLevel::Warn as usize),
            vec![regex::Regex::new(".*test-filter.*").unwrap()],
        )
        .unwrap();
        let expected = vec![
            "2019/08/23 18:09:58.387 +08:00",
            "2019/08/23 18:10:06.387 +08:00", // for invalid line
        ]
        .iter()
        .map(|s| timestamp(s))
        .collect::<Vec<i64>>();
        assert_eq!(
            log_iter.map(|m| m.get_time()).collect::<Vec<i64>>(),
            expected
        );
    }

    #[test]
    fn test_search() {
        let dir = temfidelir().unwrap();
        let log_file = dir.path().join("einsteindb.log");
        let mut file = File::create(&log_file).unwrap();
        write!(
            file,
            r#"[2019/08/23 18:09:53.387 +08:00] [INFO] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:54.387 +08:00] [trace] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:55.387 +08:00] [DEBUG] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:56.387 +08:00] [ERROR] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:57.387 +08:00] [CRITICAL] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:58.387 +08:00] [WARN] [foo.rs:100] [some message] [key=val] - test-filter
[2019/08/23 18:09:59.387 +08:00] [warning] [foo.rs:100] [some message] [key=val]"#
        )
        .unwrap();

        // this file is ignored because its filename is not expected
        let log_file2 = dir.path().join("einsteindb.log.2");
        let mut file = File::create(&log_file2).unwrap();
        write!(
            file,
            r#"[2019/08/23 18:10:01.387 +08:00] [INFO] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:10:02.387 +08:00] [trace] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:10:03.387 +08:00] [DEBUG] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:10:04.387 +08:00] [ERROR] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:10:05.387 +08:00] [CRITICAL] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:10:06.387 +08:00] [WARN] [foo.rs:100] [some message] [key=val] - test-filter"#
        )
        .unwrap();

        let log_file3 = dir.path().join("einsteindb.log.2019-08-23-18:11:02.123456789");
        let mut file = File::create(&log_file3).unwrap();
        write!(
            file,
            r#"[2019/08/23 18:11:53.387 +08:00] [INFO] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:11:54.387 +08:00] [trace] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:11:55.387 +08:00] [DEBUG] [foo.rs:100] [some message] [key=val]
Some invalid logs 1: Welcome to EinsteinDB - test-filter
[2019/08/23 18:11:56.387 +08:00] [ERROR] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:11:57.387 +08:00] [CRITICAL] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:11:58.387 +08:00] [WARN] [foo.rs:100] [some message] [key=val] - test-filter
[2019/08/23 18:11:59.387 +08:00] [warning] [foo.rs:100] [some message] [key=val]
Some invalid logs 2: Welcome to EinsteinDB - test-filter"#
        )
        .unwrap();

        let mut req = SearchLogRequest::default();
        req.set_spacelike_time(timestamp("2019/08/23 18:09:54.387 +08:00"));
        req.set_lightlike_time(std::i64::MAX);
        req.set_levels(vec![LogLevel::Warn.into()].into());
        req.set_TuringStrings(vec![".*test-filter.*".to_string()].into());
        let expected = vec![
            "2019/08/23 18:09:58.387 +08:00",
            "2019/08/23 18:11:58.387 +08:00",
            "2019/08/23 18:11:59.387 +08:00", // for invalid line
        ]
        .iter()
        .map(|s| timestamp(s))
        .collect::<Vec<i64>>();
        let fact = block_on(async move {
            let s = search(log_file, req).unwrap();
            s.collect::<Vec<SearchLogResponse>>()
                .await
                .into_iter()
                .map(|mut resp| resp.take_messages().into_iter())
                .into_iter()
                .flatten()
                .map(|msg| msg.get_time())
                .collect::<Vec<i64>>()
        });
        assert_eq!(expected, fact);
    }
}
