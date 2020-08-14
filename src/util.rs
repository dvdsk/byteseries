use std::fs::{File, OpenOptions};
use std::io::Error;
use std::path::PathBuf;

//open file and check if it has the right lenght
//(an interger multiple of the line lenght) if it
//has not warn and repair by truncating
pub fn open_and_check(path: PathBuf, full_line_size: usize) -> Result<(File, u64), Error> {
    let file = OpenOptions::new()
        .read(true)
        .append(true)
        .create(true)
        .open(path)?;
    let metadata = file.metadata()?;

    let rest = metadata.len() % (full_line_size as u64);
    if rest > 0 {
        log::warn!("Last write incomplete, truncating to largest multiple of the line size");
        file.set_len(metadata.len() - rest)?;
    }
    Ok((file, metadata.len()))
}
