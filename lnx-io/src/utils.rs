use std::io;
use std::path::Path;

#[cfg(unix)]
/// A runtime agnostic directory sync.
pub async fn sync_directory(dir: &Path) -> io::Result<()> {
    use std::fs::File;

    use tokio::sync::oneshot;

    let (tx, rx) = oneshot::channel();

    let path = dir.to_path_buf();
    std::thread::spawn(move || {
        let file = File::open(path);
        match file {
            Ok(file) => {
                let res = file.sync_data();
                let _ = tx.send(res);
            },
            Err(e) => {
                let _ = tx.send(Err(e));
            },
        }
    });

    rx.await.expect("Get result")
}

// Windows has no sync directory call like we do on unix.
#[cfg(windows)]
/// A runtime agnostic directory sync.
pub async fn sync_directory(_dir: &Path) -> io::Result<()> {
    Ok(())
}
