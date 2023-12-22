use std::io::{StdoutLock, Write};

pub struct StdOutWriter<'a> {
    stdout: StdoutLock<'a>,
}

impl<'a> Default for StdOutWriter<'a> {
    fn default() -> Self {
        Self {
            stdout: std::io::stdout().lock(),
        }
    }
}

impl<'a> Write for StdOutWriter<'a> {
    fn write_all(&mut self, buf: &[u8]) -> Result<(), std::io::Error> {
        self.stdout.write_all(buf)
    }

    fn write(&mut self, buf: &[u8]) -> Result<usize, std::io::Error> {
        self.stdout.write(buf)
    }

    fn flush(&mut self) -> Result<(), std::io::Error> {
        self.stdout.write_all(b"\n")?;
        self.stdout.flush()
    }
}

unsafe impl<'a> Send for StdOutWriter<'a> {}
