use crate::{pager, schema::SchemaEntry, utils};
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
        return Ok(get_leaf_rows(&page_bytes, entry));
    }
    if page_type == 0x05 {
        let mut rows = Vec::new();
        let cell_count = utils::bytes_to_usize(&page_bytes, 3, 2);
        for i in 0..cell_count {
            let cell_offset = utils::bytes_to_usize(&page_bytes, 12 + i * 2, 2);
            let child_page = utils::bytes_to_usize(&page_bytes, cell_offset, 4);
            rows.extend(get_all_rows(file, page_size, child_page, entry)?);
        }
        let right_child_page = utils::bytes_to_usize(&page_bytes, 8, 4);
        rows.extend(get_all_rows(file, page_size, right_child_page, entry)?);
        return Ok(rows);
    }
    bail!("Unsupported page type: {page_type}");
}

pub fn get_leaf_rows(page_bytes: &[u8], entry: &SchemaEntry) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    let cell_count = utils::bytes_to_usize(page_bytes, 3, 2);

    for i in 0..cell_count {
        let mut offset = utils::bytes_to_usize(page_bytes, 8 + i * 2, 2);
        // skip payload size and header size
        (_, offset) = utils::handle_varint(page_bytes, offset);
        let rowid;
        (rowid, offset) = utils::handle_varint(page_bytes, offset);

        rows.push(utils::retrieve_row_elements(
            entry, page_bytes, offset, rowid,
        ));
    }
    rows
}

pub fn get_row_by_rowid(
    file: &mut File,
    page_size: usize,
    page_num: usize,
    entry: &SchemaEntry,
    target_rowid: usize,
) -> Result<Vec<String>> {
    let page_bytes = pager::get_page_bytes(file, page_size, page_num)?;
    let page_type = page_bytes[0];
    if page_type == 0x0d {
        return get_row_by_rowid_leaf(&page_bytes, entry, target_rowid);
    }
    if page_type == 0x05 {
        let cell_count = utils::bytes_to_usize(&page_bytes, 3, 2);
        let right_child_page = utils::bytes_to_usize(&page_bytes, 8, 4);

        for i in 0..cell_count {
            let cell_offset = utils::bytes_to_usize(&page_bytes, 12 + i * 2, 2);
            let child_page = utils::bytes_to_usize(&page_bytes, cell_offset, 4);

            let (rowid, _) = utils::handle_varint(&page_bytes, cell_offset + 4);
            if rowid >= target_rowid {
                return get_row_by_rowid(file, page_size, child_page, entry, target_rowid);
            }
        }
        return get_row_by_rowid(file, page_size, right_child_page, entry, target_rowid);
    }
    bail!("Unsupported page type: {page_type}");
}

fn get_row_by_rowid_leaf(
    page_bytes: &[u8],
    entry: &SchemaEntry,
    target_rowid: usize,
) -> Result<Vec<String>> {
    let cell_count = utils::bytes_to_usize(page_bytes, 3, 2);

    for i in 0..cell_count {
        let mut offset = utils::bytes_to_usize(page_bytes, 8 + i * 2, 2);
        // skip payload size and header size
        (_, offset) = utils::handle_varint(page_bytes, offset);
        let rowid;
        (rowid, offset) = utils::handle_varint(page_bytes, offset);

        if rowid == target_rowid {
            return Ok(utils::retrieve_row_elements(
                entry, page_bytes, offset, rowid,
            ));
        }
    }
    bail!("Rowid {target_rowid} not found in leaf page");
}
