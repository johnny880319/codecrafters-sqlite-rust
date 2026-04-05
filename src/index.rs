use crate::{pager, utils};
use anyhow::{Result, bail};
use std::fs::File;

pub fn get_target_rowids(
    file: &mut File,
    page_size: usize,
    page_num: usize,
    target: &str,
) -> Result<Vec<usize>> {
    let page_bytes = pager::get_page_bytes(file, page_size, page_num)?;
    let mut rowids = Vec::new();
    let cell_count = utils::bytes_to_usize(&page_bytes, 3, 2);

    let page_type = page_bytes[0];
    if page_type == 0x0a {
        for i in 0..cell_count {
            let cell_offset = utils::bytes_to_usize(&page_bytes, 8 + i * 2, 2);

            let (idx_value, rowid_value) = parse_rowid_from_index_cell(&page_bytes, cell_offset);
            if idx_value == target {
                rowids.push(rowid_value);
            }
        }
        return Ok(rowids);
    }
    if page_type == 0x02 {
        let right_child_page = utils::bytes_to_usize(&page_bytes, 8, 4);

        for i in 0..cell_count {
            let cell_offset = utils::bytes_to_usize(&page_bytes, 12 + i * 2, 2);
            let child_page = utils::bytes_to_usize(&page_bytes, cell_offset, 4);

            let (idx_value, rowid_value) =
                parse_rowid_from_index_cell(&page_bytes, cell_offset + 4);

            if idx_value.as_str() > target {
                rowids.extend(get_target_rowids(file, page_size, child_page, target)?);
                return Ok(rowids);
            }
            if idx_value.as_str() == target {
                rowids.extend(get_target_rowids(file, page_size, child_page, target)?);
                rowids.push(rowid_value);
            }
        }
        rowids.extend(get_target_rowids(
            file,
            page_size,
            right_child_page,
            target,
        )?);
        return Ok(rowids);
    }
    bail!("Unsupported page type: {page_type}");
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
