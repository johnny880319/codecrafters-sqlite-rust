use crate::{
    pager,
    schema::SchemaEntry,
    utils::{self, SerialType},
};
use anyhow::{Result, bail};
use std::fs::File;

pub fn get_all_rows(
    file: &mut File,
    page_size: usize,
    page_num: usize,
    entry: &SchemaEntry,
) -> Result<Vec<Vec<String>>> {
    let page_bytes = pager::get_page_bytes(file, page_size, page_num)?;
    let page_type = page_bytes[0];
    if page_type == 0x0d {
        return Ok(get_all_rows_leaf(&page_bytes, entry));
    }
    if page_type == 0x05 {
        return get_all_rows_interior(&page_bytes, file, page_size, entry);
    }
    bail!("Unsupported page type: {page_type}");
}

fn get_all_rows_interior(
    page_bytes: &[u8],
    file: &mut File,
    page_size: usize,
    entry: &SchemaEntry,
) -> Result<Vec<Vec<String>>> {
    let mut rows = Vec::new();
    let cell_count = utils::bytes_to_usize(page_bytes, 3, 2);
    for i in 0..cell_count {
        let cell_offset = utils::bytes_to_usize(page_bytes, 12 + i * 2, 2);
        let child_page = utils::bytes_to_usize(page_bytes, cell_offset, 4);
        rows.extend(get_all_rows(file, page_size, child_page, entry)?);
    }
    let right_child_page = utils::bytes_to_usize(page_bytes, 8, 4);
    rows.extend(get_all_rows(file, page_size, right_child_page, entry)?);
    Ok(rows)
}

fn get_all_rows_leaf(page_bytes: &[u8], entry: &SchemaEntry) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    let cell_count = utils::bytes_to_usize(page_bytes, 3, 2);

    for i in 0..cell_count {
        let mut offset = utils::bytes_to_usize(page_bytes, 8 + i * 2, 2);
        // skip payload size
        (_, offset) = utils::handle_varint(page_bytes, offset);
        let rowid;
        (rowid, offset) = utils::handle_varint(page_bytes, offset);

        rows.push(retrieve_row_elements(entry, page_bytes, offset, rowid));
    }
    rows
}

pub fn get_target_row(
    file: &mut File,
    page_size: usize,
    page_num: usize,
    entry: &SchemaEntry,
    target_rowid: usize,
) -> Result<Vec<String>> {
    let page_bytes = pager::get_page_bytes(file, page_size, page_num)?;
    let page_type = page_bytes[0];
    if page_type == 0x0d {
        return get_target_row_leaf(&page_bytes, entry, target_rowid);
    }
    if page_type == 0x05 {
        return get_target_row_interior(&page_bytes, file, page_size, entry, target_rowid);
    }
    bail!("Unsupported page type: {page_type}");
}

fn get_target_row_interior(
    page_bytes: &[u8],
    file: &mut File,
    page_size: usize,
    entry: &SchemaEntry,
    target_rowid: usize,
) -> Result<Vec<String>> {
    let cell_count = utils::bytes_to_usize(page_bytes, 3, 2);
    let right_child_page = utils::bytes_to_usize(page_bytes, 8, 4);

    for i in 0..cell_count {
        let cell_offset = utils::bytes_to_usize(page_bytes, 12 + i * 2, 2);
        let child_page = utils::bytes_to_usize(page_bytes, cell_offset, 4);

        let (rowid, _) = utils::handle_varint(page_bytes, cell_offset + 4);
        if rowid >= target_rowid {
            return get_target_row(file, page_size, child_page, entry, target_rowid);
        }
    }
    get_target_row(file, page_size, right_child_page, entry, target_rowid)
}

fn get_target_row_leaf(
    page_bytes: &[u8],
    entry: &SchemaEntry,
    target_rowid: usize,
) -> Result<Vec<String>> {
    let cell_count = utils::bytes_to_usize(page_bytes, 3, 2);

    for i in 0..cell_count {
        let mut offset = utils::bytes_to_usize(page_bytes, 8 + i * 2, 2);
        // skip payload size
        (_, offset) = utils::handle_varint(page_bytes, offset);
        let rowid;
        (rowid, offset) = utils::handle_varint(page_bytes, offset);

        if rowid == target_rowid {
            return Ok(retrieve_row_elements(entry, page_bytes, offset, rowid));
        }
    }
    bail!("Rowid {target_rowid} not found in leaf page");
}

fn retrieve_row_elements(
    entry: &SchemaEntry,
    page_bytes: &[u8],
    header_offset: usize,
    rowid: usize,
) -> Vec<String> {
    let (header_size, mut offset) = utils::handle_varint(page_bytes, header_offset);

    let mut element_prop = Vec::new();
    for _ in 0..entry.tbl_columns.len() {
        let length;
        (length, offset) = utils::handle_varint(page_bytes, offset);
        element_prop.push(utils::get_serial_type(length));
    }

    offset = header_offset + header_size;
    let mut row = Vec::new();
    for serial_type in element_prop {
        match serial_type {
            SerialType::Null => {
                row.push(rowid.to_string());
            }
            SerialType::Int(length) => {
                let value = utils::bytes_to_usize(page_bytes, offset, length);
                row.push(value.to_string());
            }
            SerialType::Float => {
                let value = utils::bytes_to_usize(page_bytes, offset, 8);
                row.push(value.to_string());
            }
            SerialType::Zero => {
                row.push("0".to_string());
            }
            SerialType::One => {
                row.push("1".to_string());
            }
            SerialType::Text(length) | SerialType::Blob(length) => {
                let value =
                    String::from_utf8_lossy(&page_bytes[offset..offset + length]).to_string();
                row.push(value);
            }
        }
        offset += serial_type.length();
    }
    row
}
