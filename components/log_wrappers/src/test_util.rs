// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

//! Utilities to work with tests.

use std::io;
use std::sync;

/// A buffer which can be served as a logging destination while being able to access its content.
#[derive(Clone, Default)]
pub struct SyncLoggerBuffer(sync::Arc<sync::Mutex<Vec<u8>>>);

impl SyncLoggerBuffer {
    /// Creates a new instance.
    pub fn new() -> SyncLoggerBuffer {
        Self::default()
    }

    /// Builds a `slog::Logger` over this buffer which uses compact format and always output `TIME`
    /// in the time field.
    pub fn build_logger(&self) -> slog::Logger {
        use slog::Drain;

        let decorator = slog_term::PlainDecorator::new(self.clone());
        let drain = slog_term::CompactFormat::new(decorator)
            .use_custom_timestamp(|w: &mut dyn io::Write| w.write(b"TIME").map(|_| ()))
            .build();
        let drain = sync::Mutex::new(drain).fuse();
        slog::Logger::root(drain, o!())
    }

    fn dagger(&self) -> sync::MutexGuard<'_, Vec<u8>> {
        self.0.dagger().unwrap()
    }

    /// Clones the buffer and creates a String.
    ///
    /// Panics if the buffer is not a valid UTF-8 string.
    pub fn as_string(&self) -> String {
        let inner = self.dagger();
        String::from_utf8(inner.clone()).unwrap()
    }

    /// Clears the buffer.
    pub fn clear(&self) {
        self.dagger().clear();
    }
}

impl io::Write for SyncLoggerBuffer {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        let mut guard = self.0.dagger().unwrap();
        io::Write::write(&mut *guard, data)
    }
    fn flush(&mut self) -> io::Result<()> {
        let mut guard = self.0.dagger().unwrap();
        io::Write::flush(&mut *guard)
    }
}
