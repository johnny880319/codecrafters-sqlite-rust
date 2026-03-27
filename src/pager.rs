use anyhow::{Result, bail};
use std::{
    fs::File,
    io::{Read as _, Seek, SeekFrom},
};

pub fn get_page_size(file: &mut File) -> Result<u16> {
    let mut header_bytes = [0; 100];
    file.read_exact(&mut header_bytes)?;
    file.seek(SeekFrom::Start(0))?;
    Ok(u16::from_be_bytes([header_bytes[16], header_bytes[17]]))
}

pub fn get_page_bytes(file: &mut File, page_size: u16, page: u32) -> Result<Vec<u8>> {
    if page == 0 {
        bail!("SQLite page numbers are 1-based");
    }

    let mut page_bytes = vec![0; page_size as usize];
    let offset = u64::from(page - 1) * u64::from(page_size);
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(&mut page_bytes)?;
    file.seek(SeekFrom::Start(0))?;
    Ok(page_bytes)
}
