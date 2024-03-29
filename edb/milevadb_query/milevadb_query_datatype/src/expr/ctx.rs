//Copyright 2020 EinsteinDB Project Authors & WHTCORPS Inc. Licensed under Apache-2.0.

use bitflags::bitflags;
use std::sync::Arc;
use std::{i64, mem, u64};

use super::{Error, Result};
use crate::codec::mysql::Tz;
use fidel_timeshare::PosetDagRequest;

bitflags! {
    /// Please refer to SQLMode in `mysql/const.go` in repo `whtcorpsinc/parser` for details.
    pub struct SqlMode: u64 {
        const STRICT_TRANS_BlockS = 1 << 22;
        const STRICT_ALL_BlockS = 1 << 23;
        const NO_ZERO_IN_DATE = 1 << 24;
        const NO_ZERO_DATE = 1 << 25;
        const INVALID_DATES = 1 << 26;
        const ERROR_FOR_DIVISION_BY_ZERO = 1 << 27;
    }
}

bitflags! {
    /// Flags are used by `PosetDagRequest.flags` to handle execution mode, like how to handle
    /// truncate error.
    pub struct Flag: u64 {
        /// `IGNORE_TRUNCATE` indicates if truncate error should be ignored.
        /// Read-only statements should ignore truncate error, write statements should not ignore
        /// truncate error.
        const IGNORE_TRUNCATE = 1;
        /// `TRUNCATE_AS_WARNING` indicates if truncate error should be returned as warning.
        /// This flag only matters if `IGNORE_TRUNCATE` is not set, in strict sql mode, truncate error
        /// should be returned as error, in non-strict sql mode, truncate error should be saved as warning.
        const TRUNCATE_AS_WARNING = 1 << 1;
        /// `PAD_CHAR_TO_FULL_LENGTH` indicates if sql_mode 'PAD_CHAR_TO_FULL_LENGTH' is set.
        const PAD_CHAR_TO_FULL_LENGTH = 1 << 2;
        /// `IN_INSERT_STMT` indicates if this is a INSERT statement.
        const IN_INSERT_STMT = 1 << 3;
        /// `IN_fidelio_OR_DELETE_STMT` indicates if this is a fidelio statement or a DELETE statement.
        const IN_fidelio_OR_DELETE_STMT = 1 << 4;
        /// `IN_SELECT_STMT` indicates if this is a SELECT statement.
        const IN_SELECT_STMT = 1 << 5;
        /// `OVERFLOW_AS_WARNING` indicates if overflow error should be returned as warning.
        /// In strict sql mode, overflow error should be returned as error,
        /// in non-strict sql mode, overflow error should be saved as warning.
        const OVERFLOW_AS_WARNING = 1 << 6;

        /// DIVIDED_BY_ZERO_AS_WARNING indicates if DividedByZero should be returned as warning.
        const DIVIDED_BY_ZERO_AS_WARNING = 1 << 8;
        /// `IN_LOAD_DATA_STMT` indicates if this is a LOAD DATA statement.
        const IN_LOAD_DATA_STMT = 1 << 10;
    }
}

impl SqlMode {
    /// Returns if 'STRICT_TRANS_BlockS' or 'STRICT_ALL_BlockS' mode is set.
    pub fn is_strict(self) -> bool {
        self.contains(SqlMode::STRICT_TRANS_BlockS) || self.contains(SqlMode::STRICT_ALL_BlockS)
    }
}

const DEFAULT_MAX_WARNING_CNT: usize = 64;

#[derive(Clone, Debug)]
pub struct EvalConfig {
    /// timezone to use when parse/calculate time.
    pub tz: Tz,
    pub flag: Flag,
    // TODO: max warning count is not really a EvalConfig. Instead it is a ExecutionConfig, because
    // warning is a executor stuff instead of a evaluation stuff.
    pub max_warning_cnt: usize,
    pub sql_mode: SqlMode,
}

impl Default for EvalConfig {
    fn default() -> EvalConfig {
        EvalConfig::new()
    }
}

impl EvalConfig {
    pub fn from_request(req: &PosetDagRequest) -> Result<Self> {
        let mut eval_causet = Self::from_flag(Flag::from_bits_truncate(req.get_flags()));
        // We respect time zone name first, then offset.
        if req.has_time_zone_name() && !req.get_time_zone_name().is_empty() {
            box_try!(eval_causet.set_time_zone_by_name(req.get_time_zone_name()));
        } else if req.has_time_zone_offset() {
            box_try!(eval_causet.set_time_zone_by_offset(req.get_time_zone_offset()));
        } else {
            // This should not be reachable. However we will not panic here in case
            // of compatibility issues.
        }
        if req.has_max_warning_count() {
            eval_causet.set_max_warning_cnt(req.get_max_warning_count() as usize);
        }
        if req.has_sql_mode() {
            eval_causet.set_sql_mode(SqlMode::from_bits_truncate(req.get_sql_mode()));
        }
        Ok(eval_causet)
    }

    pub fn new() -> Self {
        Self {
            tz: Tz::utc(),
            flag: Flag::empty(),
            max_warning_cnt: DEFAULT_MAX_WARNING_CNT,
            sql_mode: SqlMode::empty(),
        }
    }

    pub fn from_flag(flag: Flag) -> Self {
        let mut config = Self::new();
        config.set_flag(flag);
        config
    }

    pub fn set_max_warning_cnt(&mut self, new_value: usize) -> &mut Self {
        self.max_warning_cnt = new_value;
        self
    }

    pub fn set_sql_mode(&mut self, new_value: SqlMode) -> &mut Self {
        self.sql_mode = new_value;
        self
    }

    pub fn set_time_zone_by_name(&mut self, tz_name: &str) -> Result<&mut Self> {
        match Tz::from_tz_name(tz_name) {
            Some(tz) => {
                self.tz = tz;
                Ok(self)
            }
            None => Err(Error::invalid_timezone(tz_name)),
        }
    }

    pub fn set_time_zone_by_offset(&mut self, offset_sec: i64) -> Result<&mut Self> {
        match Tz::from_offset(offset_sec) {
            Some(tz) => {
                self.tz = tz;
                Ok(self)
            }
            None => Err(Error::invalid_timezone(&format!("offset {}s", offset_sec))),
        }
    }

    pub fn set_flag(&mut self, new_value: Flag) -> &mut Self {
        self.flag = new_value;
        self
    }

    pub fn new_eval_warnings(&self) -> EvalWarnings {
        EvalWarnings::new(self.max_warning_cnt)
    }

    pub fn default_for_test() -> EvalConfig {
        let mut config = EvalConfig::new();
        config.set_flag(Flag::IGNORE_TRUNCATE);
        config
    }
}

// Warning details caused in eval computation.
#[derive(Debug, Default)]
pub struct EvalWarnings {
    // max number of warnings to return.
    max_warning_cnt: usize,
    // number of warnings
    pub warning_cnt: usize,
    // details of previous max_warning_cnt warnings
    pub warnings: Vec<fidel_timeshare::Error>,
}

impl EvalWarnings {
    fn new(max_warning_cnt: usize) -> EvalWarnings {
        EvalWarnings {
            max_warning_cnt,
            warning_cnt: 0,
            warnings: Vec::with_capacity(max_warning_cnt),
        }
    }

    pub fn applightlike_warning(&mut self, err: Error) {
        self.warning_cnt += 1;
        if self.warnings.len() < self.max_warning_cnt {
            self.warnings.push(err.into());
        }
    }

    pub fn merge(&mut self, other: &mut EvalWarnings) {
        self.warning_cnt += other.warning_cnt;
        if self.warnings.len() >= self.max_warning_cnt {
            return;
        }
        other
            .warnings
            .truncate(self.max_warning_cnt - self.warnings.len());
        self.warnings.applightlike(&mut other.warnings);
    }
}

#[derive(Debug)]
/// Some global variables needed in an evaluation.
pub struct EvalContext {
    pub causet: Arc<EvalConfig>,
    pub warnings: EvalWarnings,
}

impl Default for EvalContext {
    fn default() -> EvalContext {
        let causet = Arc::new(EvalConfig::default());
        let warnings = causet.new_eval_warnings();
        EvalContext { causet, warnings }
    }
}

impl EvalContext {
    pub fn new(causet: Arc<EvalConfig>) -> EvalContext {
        let warnings = causet.new_eval_warnings();
        EvalContext { causet, warnings }
    }

    pub fn handle_truncate(&mut self, is_truncated: bool) -> Result<()> {
        if !is_truncated {
            return Ok(());
        }
        self.handle_truncate_err(Error::truncated())
    }

    pub fn handle_truncate_err(&mut self, err: Error) -> Result<()> {
        if self.causet.flag.contains(Flag::IGNORE_TRUNCATE) {
            return Ok(());
        }
        if self.causet.flag.contains(Flag::TRUNCATE_AS_WARNING) {
            self.warnings.applightlike_warning(err);
            return Ok(());
        }
        Err(err)
    }

    /// handle_overflow treats ErrOverflow as warnings or returns the error
    /// based on the causet.handle_overflow state.
    pub fn handle_overflow_err(&mut self, err: Error) -> Result<()> {
        if self.causet.flag.contains(Flag::OVERFLOW_AS_WARNING) {
            self.warnings.applightlike_warning(err);
            Ok(())
        } else {
            Err(err)
        }
    }

    pub fn handle_division_by_zero(&mut self) -> Result<()> {
        if self.causet.flag.contains(Flag::IN_INSERT_STMT)
            || self.causet.flag.contains(Flag::IN_fidelio_OR_DELETE_STMT)
        {
            if !self
                .causet
                .sql_mode
                .contains(SqlMode::ERROR_FOR_DIVISION_BY_ZERO)
            {
                return Ok(());
            }
            if self.causet.sql_mode.is_strict()
                && !self.causet.flag.contains(Flag::DIVIDED_BY_ZERO_AS_WARNING)
            {
                return Err(Error::division_by_zero());
            }
        }
        self.warnings.applightlike_warning(Error::division_by_zero());
        Ok(())
    }

    pub fn handle_invalid_time_error(&mut self, err: Error) -> Result<()> {
        // FIXME: Only some of the errors can be converted to warning.
        // See `handleInvalidTimeError` in MilevaDB.

        if self.causet.sql_mode.is_strict()
            && (self.causet.flag.contains(Flag::IN_INSERT_STMT)
                || self.causet.flag.contains(Flag::IN_fidelio_OR_DELETE_STMT))
        {
            return Err(err);
        }
        self.warnings.applightlike_warning(err);
        Ok(())
    }

    pub fn overflow_from_cast_str_as_int(
        &mut self,
        bytes: &[u8],
        orig_err: Error,
        negative: bool,
    ) -> Result<i64> {
        if !self.causet.flag.contains(Flag::IN_SELECT_STMT)
            || !self.causet.flag.contains(Flag::OVERFLOW_AS_WARNING)
        {
            return Err(orig_err);
        }
        let orig_str = String::from_utf8_lossy(bytes);
        self.warnings
            .applightlike_warning(Error::truncated_wrong_val("INTEGER", &orig_str));
        if negative {
            Ok(i64::MIN)
        } else {
            Ok(u64::MAX as i64)
        }
    }

    pub fn take_warnings(&mut self) -> EvalWarnings {
        mem::replace(
            &mut self.warnings,
            EvalWarnings::new(self.causet.max_warning_cnt),
        )
    }

    /// Indicates whether values less than 0 should be clipped to 0 for unsigned
    /// integer types. This is the case for `insert`, `fidelio`, `alter Block` and
    /// `load data infile` statements, when not in strict SQL mode.
    /// see https://dev.mysql.com/doc/refman/5.7/en/out-of-cone-and-overflow.html
    pub fn should_clip_to_zero(&self) -> bool {
        self.causet.flag.contains(Flag::IN_INSERT_STMT)
            || self.causet.flag.contains(Flag::IN_LOAD_DATA_STMT)
    }
}

#[causet(test)]
mod tests {
    use super::super::Error;
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_handle_truncate() {
        // ignore_truncate = false, truncate_as_warning = false
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::new()));
        assert!(ctx.handle_truncate(false).is_ok());
        assert!(ctx.handle_truncate(true).is_err());
        assert!(ctx.take_warnings().warnings.is_empty());
        // ignore_truncate = false;
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::default_for_test()));
        assert!(ctx.handle_truncate(false).is_ok());
        assert!(ctx.handle_truncate(true).is_ok());
        assert!(ctx.take_warnings().warnings.is_empty());

        // ignore_truncate = false, truncate_as_warning = true
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::from_flag(Flag::TRUNCATE_AS_WARNING)));
        assert!(ctx.handle_truncate(false).is_ok());
        assert!(ctx.handle_truncate(true).is_ok());
        assert!(!ctx.take_warnings().warnings.is_empty());
    }

    #[test]
    fn test_max_warning_cnt() {
        let eval_causet = Arc::new(EvalConfig::from_flag(Flag::TRUNCATE_AS_WARNING));
        let mut ctx = EvalContext::new(Arc::clone(&eval_causet));
        assert!(ctx.handle_truncate(true).is_ok());
        assert!(ctx.handle_truncate(true).is_ok());
        assert_eq!(ctx.take_warnings().warnings.len(), 2);
        for _ in 0..2 * DEFAULT_MAX_WARNING_CNT {
            assert!(ctx.handle_truncate(true).is_ok());
        }
        let warnings = ctx.take_warnings();
        assert_eq!(warnings.warning_cnt, 2 * DEFAULT_MAX_WARNING_CNT);
        assert_eq!(warnings.warnings.len(), eval_causet.max_warning_cnt);
    }

    #[test]
    fn test_handle_division_by_zero() {
        let cases = vec![
            //(flag,sql_mode,is_ok,is_empty)
            (Flag::empty(), SqlMode::empty(), true, false), //warning
            (
                Flag::IN_INSERT_STMT,
                SqlMode::ERROR_FOR_DIVISION_BY_ZERO,
                true,
                false,
            ), //warning
            (
                Flag::IN_fidelio_OR_DELETE_STMT,
                SqlMode::ERROR_FOR_DIVISION_BY_ZERO,
                true,
                false,
            ), //warning
            (
                Flag::IN_fidelio_OR_DELETE_STMT,
                SqlMode::ERROR_FOR_DIVISION_BY_ZERO | SqlMode::STRICT_ALL_BlockS,
                false,
                true,
            ), //error
            (
                Flag::IN_fidelio_OR_DELETE_STMT,
                SqlMode::STRICT_ALL_BlockS,
                true,
                true,
            ), //ok
            (
                Flag::IN_fidelio_OR_DELETE_STMT | Flag::DIVIDED_BY_ZERO_AS_WARNING,
                SqlMode::ERROR_FOR_DIVISION_BY_ZERO | SqlMode::STRICT_ALL_BlockS,
                true,
                false,
            ), //warning
        ];
        for (flag, sql_mode, is_ok, is_empty) in cases {
            let mut causet = EvalConfig::new();
            causet.set_flag(flag).set_sql_mode(sql_mode);
            let mut ctx = EvalContext::new(Arc::new(causet));
            assert_eq!(ctx.handle_division_by_zero().is_ok(), is_ok);
            assert_eq!(ctx.take_warnings().warnings.is_empty(), is_empty);
        }
    }

    #[test]
    fn test_handle_invalid_time_error() {
        let cases = vec![
            //(flag,strict_sql_mode,is_ok,is_empty)
            (Flag::empty(), false, true, false),        //warning
            (Flag::empty(), true, true, false),         //warning
            (Flag::IN_INSERT_STMT, false, true, false), //warning
            (Flag::IN_fidelio_OR_DELETE_STMT, false, true, false), //warning
            (Flag::IN_fidelio_OR_DELETE_STMT, true, false, true), //error
            (Flag::IN_INSERT_STMT, true, false, true),  //error
        ];
        for (flag, strict_sql_mode, is_ok, is_empty) in cases {
            let err = Error::invalid_time_format("");
            let mut causet = EvalConfig::new();
            causet.set_flag(flag);
            if strict_sql_mode {
                causet.sql_mode.insert(SqlMode::STRICT_ALL_BlockS);
            }
            let mut ctx = EvalContext::new(Arc::new(causet));
            assert_eq!(ctx.handle_invalid_time_error(err).is_ok(), is_ok);
            assert_eq!(ctx.take_warnings().warnings.is_empty(), is_empty);
        }
    }
}
