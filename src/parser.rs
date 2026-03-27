use anyhow::Result;
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

pub fn get_page_bytes(file: &mut File, page_size: u16) -> Result<Vec<u8>> {
    let mut page_bytes = vec![0; page_size as usize];
    file.read_exact(&mut page_bytes)?;
    file.seek(SeekFrom::Start(0))?;
    Ok(page_bytes)
}

pub fn get_table_count(page_bytes: &[u8], is_root: bool) -> u16 {
    if is_root {
        u16::from_be_bytes([page_bytes[103], page_bytes[104]])
    } else {
        u16::from_be_bytes([page_bytes[3], page_bytes[4]])
    }
}

pub fn parse_table_name(raw_bytes: &[u8], mut offset: usize) -> String {
    // skip record length and rowid
    (_, offset) = handle_varint(raw_bytes, offset);
    (_, offset) = handle_varint(raw_bytes, offset);

    let header_offset = offset;
    let (header_length, offset) = handle_varint(raw_bytes, offset);
    let (type_length, offset) = handle_varint(raw_bytes, offset);
    let (name_length, _) = handle_varint(raw_bytes, offset);

    let type_length = (type_length - 13) / 2;
    let name_length = (name_length - 13) / 2;

    let name_start_offset = header_offset + header_length + type_length;
    let name_end_offset = name_start_offset + name_length;
    String::from_utf8_lossy(&raw_bytes[name_start_offset..name_end_offset]).to_string()
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
