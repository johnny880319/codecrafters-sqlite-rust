use anyhow::{Result, bail};
use std::{
    fs::File,
    io::{Read as _, Seek, SeekFrom},
};

pub fn get_db_info(file: &mut File) -> Result<(u16, u16, Vec<u8>)> {
    let page_size = get_page_size(file)?;
    let page_bytes = get_page_bytes(file, page_size, 1)?;
    let num_tables = get_cell_count(&page_bytes, true);
    Ok((page_size, num_tables, page_bytes))
}

fn get_page_size(file: &mut File) -> Result<u16> {
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

pub fn get_cell_count(page_bytes: &[u8], is_root: bool) -> u16 {
    if is_root {
        u16::from_be_bytes([page_bytes[103], page_bytes[104]])
    } else {
        u16::from_be_bytes([page_bytes[3], page_bytes[4]])
    }
}

pub struct TableInfo {
    pub tbl_name: String,
    pub root_page: u32,
}

pub fn parse_table_info(raw_bytes: &[u8], mut offset: usize) -> TableInfo {
    // skip record length and rowid
    (_, offset) = handle_varint(raw_bytes, offset);
    (_, offset) = handle_varint(raw_bytes, offset);

    let header_offset = offset;
    let (header_length, offset) = handle_varint(raw_bytes, offset);
    let (type_length, offset) = handle_varint(raw_bytes, offset);
    let (name_length, offset) = handle_varint(raw_bytes, offset);
    let (tbl_name_length, offset) = handle_varint(raw_bytes, offset);
    let (root_page_length, _) = handle_varint(raw_bytes, offset);

    let type_length = (type_length - 13) / 2;
    let name_length = (name_length - 13) / 2;
    let tbl_name_length = (tbl_name_length - 13) / 2;

    let name_start_offset = header_offset + header_length + type_length + name_length;
    let name_end_offset = name_start_offset + tbl_name_length;
    let tbl_name =
        String::from_utf8_lossy(&raw_bytes[name_start_offset..name_end_offset]).to_string();
    let mut root_page = 0;
    for i in 0..root_page_length {
        let byte = raw_bytes[name_end_offset + i];
        root_page = (root_page << 8) | u32::from(byte);
    }
    TableInfo {
        tbl_name,
        root_page,
    }
}

fn handle_varint(raw_bytes: &[u8], mut offset: usize) -> (usize, usize) {
    let mut value = 0;
    loop {
        let byte = raw_bytes[offset];
        value = (value << 7) | (usize::from(byte) & 0x7F);
        offset += 1;
        if byte & 0x80 == 0 {
            break;
        }
    }
    (value, offset)
}
