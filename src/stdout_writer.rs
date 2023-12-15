use std::io::Write;

#[derive(Default)]
pub struct StdOutWriter {}

impl Write for StdOutWriter {
    fn write_all(&mut self, buf: &[u8]) -> Result<(), std::io::Error> {
        let mut stdout = std::io::stdout().lock();
        stdout.write_all(buf)
    }

    fn write(&mut self, buf: &[u8]) -> Result<usize, std::io::Error> {
        let mut stdout = std::io::stdout().lock();
        stdout.write(buf)
    }

    fn flush(&mut self) -> Result<(), std::io::Error> {
        let mut stdout = std::io::stdout().lock();
        stdout.write_all(b"\n")?;
        stdout.flush()
    }
}
