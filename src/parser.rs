use crate::{
    pager,
    utils::{self, SerialType},
};
use anyhow::{Result, bail};
use std::fs::File;

pub struct SchemaEntry {
    pub tbl_name: String,
    pub tbl_type: String,
    pub tbl_columns: Vec<String>,
    pub root_page: usize,
}

pub fn parse_schema_entries(
    raw_bytes: &[u8],
    offset: usize,
    num_entries: usize,
) -> Vec<SchemaEntry> {
    let mut entries = Vec::new();
    for i in 0..num_entries {
        let cell_offset = utils::bytes_to_usize(raw_bytes, offset + i * 2, 2);

        let table_info = parse_schema_entry(raw_bytes, cell_offset);
        entries.push(table_info);
    }
    entries
}

fn parse_schema_entry(raw_bytes: &[u8], mut offset: usize) -> SchemaEntry {
    // skip record length and rowid
    (_, offset) = utils::handle_varint(raw_bytes, offset);
    (_, offset) = utils::handle_varint(raw_bytes, offset);

    let header_offset = offset;
    let (header_length, offset) = utils::handle_varint(raw_bytes, offset);
    let (type_st, offset) = utils::handle_varint(raw_bytes, offset);
    let (name_st, offset) = utils::handle_varint(raw_bytes, offset);
    let (tbl_name_st, offset) = utils::handle_varint(raw_bytes, offset);
    let (root_page_length, offset) = utils::handle_varint(raw_bytes, offset);
    let (sql_st, _) = utils::handle_varint(raw_bytes, offset);

    // only header length and root page length don't need to convert to text length.
    let type_serial_type = utils::get_serial_type(type_st);
    let name_serial_type = utils::get_serial_type(name_st);
    let tbl_name_serial_type = utils::get_serial_type(tbl_name_st);
    let sql_serial_type = utils::get_serial_type(sql_st);

    let type_offset = header_offset + header_length;
    let name_offset = type_offset + type_serial_type.length();
    let tbl_name_offset = name_offset + name_serial_type.length();
    let root_page_offset = tbl_name_offset + tbl_name_serial_type.length();
    let sql_offset = root_page_offset + root_page_length;
    let end_offset = sql_offset + sql_serial_type.length();

    let tbl_type = String::from_utf8_lossy(&raw_bytes[type_offset..name_offset]).to_string();
    let tbl_name =
        String::from_utf8_lossy(&raw_bytes[tbl_name_offset..root_page_offset]).to_string();
    let root_page = utils::bytes_to_usize(raw_bytes, root_page_offset, root_page_length);

    let sql_command = String::from_utf8_lossy(&raw_bytes[sql_offset..end_offset]).to_string();
    let tbl_columns = get_column_names(&sql_command);

    SchemaEntry {
        tbl_name,
        tbl_type,
        tbl_columns,
        root_page,
    }
}

fn get_column_names(sql_command: &str) -> Vec<String> {
    let open_paren_index = sql_command.find('(').unwrap();
    let close_paren_index = sql_command.rfind(')').unwrap();
    let columns_str = &sql_command[open_paren_index + 1..close_paren_index];

    columns_str
        .split(',')
        .filter(|s| !s.to_uppercase().starts_with("PRIMARY"))
        .map(|s| s.split_whitespace().next().unwrap().to_string())
        .collect::<Vec<_>>()
}

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
        (_, offset) = utils::handle_varint(page_bytes, offset);

        let mut element_prop = Vec::new();
        for _ in 0..entry.tbl_columns.len() {
            let st;
            (st, offset) = utils::handle_varint(page_bytes, offset);
            element_prop.push(utils::get_serial_type(st));
        }

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
        rows.push(row);
    }
    rows
}

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
        // skip payload size annd header size
        (_, offset) = utils::handle_varint(page_bytes, offset);
        let rowid;
        (rowid, offset) = utils::handle_varint(page_bytes, offset);
        (_, offset) = utils::handle_varint(page_bytes, offset);

        if rowid != target_rowid {
            continue;
        }

        let mut element_prop = Vec::new();
        for _ in 0..entry.tbl_columns.len() {
            let length;
            (length, offset) = utils::handle_varint(page_bytes, offset);
            element_prop.push(utils::get_serial_type(length));
        }

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
        return Ok(row);
    }
    bail!("Rowid {target_rowid} not found in leaf page");
}
