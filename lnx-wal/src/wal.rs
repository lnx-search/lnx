use std::io;
use std::path::Path;

pub trait DataWal {
    fn open(path: &Path) -> io::Result<Self>;

    fn write_all(&mut self, data: &[u8]) -> io::Result<()>;

    fn commit(&mut self) -> io::Result<()>;

    fn recover(&mut self) -> io::Result<()>;
}