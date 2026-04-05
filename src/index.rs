use crate::{pager, utils};
use anyhow::{Result, bail};
use std::fs::File;

pub fn get_target_rowids(
    file: &mut File,
    page_size: usize,
    page_num: usize,
    target_value: &str,
) -> Result<Vec<usize>> {
    let page_bytes = pager::get_page_bytes(file, page_size, page_num)?;

    let page_type = page_bytes[0];
    if page_type == 0x0a {
        return Ok(get_target_rowids_leaf(&page_bytes, target_value));
    }
    if page_type == 0x02 {
        return get_target_rowids_interior(&page_bytes, file, page_size, target_value);
    }
    bail!("Unsupported page type: {page_type}");
}

fn get_target_rowids_interior(
    page_bytes: &[u8],
    file: &mut File,
    page_size: usize,
    target_value: &str,
) -> Result<Vec<usize>> {
    let mut rowids = Vec::new();
    let cell_count = utils::bytes_to_usize(page_bytes, 3, 2);
    let right_child_page = utils::bytes_to_usize(page_bytes, 8, 4);

    for i in 0..cell_count {
        let cell_offset = utils::bytes_to_usize(page_bytes, 12 + i * 2, 2);
        let child_page = utils::bytes_to_usize(page_bytes, cell_offset, 4);

        let (idx_value, rowid_value) = parse_rowid_from_index_cell(page_bytes, cell_offset + 4);

        if idx_value.as_str() > target_value {
            rowids.extend(get_target_rowids(
                file,
                page_size,
                child_page,
                target_value,
            )?);
            return Ok(rowids);
        }
        if idx_value.as_str() == target_value {
            rowids.extend(get_target_rowids(
                file,
                page_size,
                child_page,
                target_value,
            )?);
            rowids.push(rowid_value);
        }
    }
    rowids.extend(get_target_rowids(
        file,
        page_size,
        right_child_page,
        target_value,
    )?);
    Ok(rowids)
}

fn get_target_rowids_leaf(page_bytes: &[u8], target_value: &str) -> Vec<usize> {
    let mut rowids = Vec::new();
    let cell_count = utils::bytes_to_usize(page_bytes, 3, 2);

    for i in 0..cell_count {
        let cell_offset = utils::bytes_to_usize(page_bytes, 8 + i * 2, 2);

        let (idx_value, rowid_value) = parse_rowid_from_index_cell(page_bytes, cell_offset);
        if idx_value == target_value {
            rowids.push(rowid_value);
        }
    }
    rowids
}

fn parse_rowid_from_index_cell(page_bytes: &[u8], cell_offset: usize) -> (String, usize) {
    let (_, cell_offset) = utils::handle_varint(page_bytes, cell_offset);
    let header_offset = cell_offset;
    let (header_length, cell_offset) = utils::handle_varint(page_bytes, header_offset);
    let (idx_serial_st, cell_offset) = utils::handle_varint(page_bytes, cell_offset);
    let (rowid_serial_st, _) = utils::handle_varint(page_bytes, cell_offset);
    let mut cell_offset = header_offset + header_length;

    let idx_serial_type = utils::get_serial_type(idx_serial_st);
    let rowid_serial_type = utils::get_serial_type(rowid_serial_st);

    let idx_value =
        String::from_utf8_lossy(&page_bytes[cell_offset..cell_offset + idx_serial_type.length()])
            .to_string();
    cell_offset += idx_serial_type.length();
    let rowid_value = utils::bytes_to_usize(page_bytes, cell_offset, rowid_serial_type.length());

    (idx_value, rowid_value)
}
