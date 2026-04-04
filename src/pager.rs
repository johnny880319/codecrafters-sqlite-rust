use crate::utils;
use anyhow::{Result, bail};
use std::{
    fs::File,
    io::{Read as _, Seek, SeekFrom},
};

pub fn get_page_size(file: &mut File) -> Result<usize> {
    let mut header_bytes = [0; 100];
    file.read_exact(&mut header_bytes)?;
    file.seek(SeekFrom::Start(0))?;
    Ok(utils::bytes_to_usize(&header_bytes, 16, 2))
}

pub fn get_page_bytes(file: &mut File, page_size: usize, page_num: usize) -> Result<Vec<u8>> {
    if page_num == 0 {
        bail!("SQLite page numbers are 1-based");
    }

    let mut page_bytes = vec![0; page_size];
    let offset = (page_num - 1) * page_size;
    file.seek(SeekFrom::Start(offset as u64))?;
    file.read_exact(&mut page_bytes)?;
    file.seek(SeekFrom::Start(0))?;
    Ok(page_bytes)
}
