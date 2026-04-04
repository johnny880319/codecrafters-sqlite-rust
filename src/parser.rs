use crate::pager;
use anyhow::{Result, bail};
use std::fs::File;

pub struct SchemaEntry {
    pub tbl_name: String,
    pub tbl_type: String,
    pub tbl_columns: Vec<String>,
    pub root_page: u32,
}

pub fn parse_schema_entries(raw_bytes: &[u8], offset: usize, num_entries: u16) -> Vec<SchemaEntry> {
    let mut entries = Vec::new();
    for i in 0..num_entries {
        let cell_offset = u16::from_be_bytes([
            raw_bytes[offset + (i as usize) * 2],
            raw_bytes[offset + (i as usize) * 2 + 1],
        ]) as usize;
        let table_info = parse_schema_entry(raw_bytes, cell_offset);
        entries.push(table_info);
    }
    entries
}

fn parse_schema_entry(raw_bytes: &[u8], mut offset: usize) -> SchemaEntry {
    // skip record length and rowid
    (_, offset) = handle_varint(raw_bytes, offset);
    (_, offset) = handle_varint(raw_bytes, offset);

    let header_offset = offset;
    let (header_length, offset) = handle_varint(raw_bytes, offset);
    let (type_length, offset) = handle_varint(raw_bytes, offset);
    let (name_length, offset) = handle_varint(raw_bytes, offset);
    let (tbl_name_length, offset) = handle_varint(raw_bytes, offset);
    let (root_page_length, offset) = handle_varint(raw_bytes, offset);
    let (sql_length, _) = handle_varint(raw_bytes, offset);

    // only header length and root page length don't need to convert to text length.
    let type_length = (type_length - 13) / 2;
    let name_length = (name_length - 13) / 2;
    let tbl_name_length = (tbl_name_length - 13) / 2;
    let sql_length = (sql_length - 13) / 2;

    let type_offset = header_offset + header_length;
    let name_offset = type_offset + type_length;
    let tbl_name_offset = name_offset + name_length;
    let root_page_offset = tbl_name_offset + tbl_name_length;
    let sql_offset = root_page_offset + root_page_length;
    let end_offset = sql_offset + sql_length;

    let tbl_type = String::from_utf8_lossy(&raw_bytes[type_offset..name_offset]).to_string();
    let tbl_name =
        String::from_utf8_lossy(&raw_bytes[tbl_name_offset..root_page_offset]).to_string();
    let mut root_page = 0;
    for i in 0..root_page_length {
        let byte = raw_bytes[root_page_offset + i];
        root_page = (root_page << 8) | u32::from(byte);
    }
    let sql_command = String::from_utf8_lossy(&raw_bytes[sql_offset..end_offset]).to_string();
    let tbl_columns = get_column_names(&sql_command);

    SchemaEntry {
        tbl_name,
        tbl_type,
        tbl_columns,
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
    page_size: u16,
    page_num: u32,
    entry: &SchemaEntry,
) -> Result<Vec<Vec<String>>> {
    let page_bytes = pager::get_page_bytes(file, page_size, page_num)?;
    let page_type = page_bytes[0];
    if page_type == 0x0d {
        return Ok(get_leaf_rows(&page_bytes, entry));
    }
    if page_type == 0x05 {
        let mut rows = Vec::new();
        let cell_count = u16::from_be_bytes([page_bytes[3], page_bytes[4]]) as usize;
        for i in 0..cell_count {
            let cell_offset =
                u16::from_be_bytes([page_bytes[12 + i * 2], page_bytes[12 + i * 2 + 1]]) as usize;
            let child_page = u32::from_be_bytes([
                page_bytes[cell_offset],
                page_bytes[cell_offset + 1],
                page_bytes[cell_offset + 2],
                page_bytes[cell_offset + 3],
            ]);
            rows.extend(get_all_rows(file, page_size, child_page, entry)?);
        }
        let right_child_page =
            u32::from_be_bytes([page_bytes[8], page_bytes[9], page_bytes[10], page_bytes[11]]);
        rows.extend(get_all_rows(file, page_size, right_child_page, entry)?);
        return Ok(rows);
    }
    bail!("Unsupported page type: {page_type}");
}

pub fn get_leaf_rows(page_bytes: &[u8], entry: &SchemaEntry) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    let cell_count = u16::from_be_bytes([page_bytes[3], page_bytes[4]]) as usize;

    for i in 0..cell_count {
        let mut offset =
            u16::from_be_bytes([page_bytes[8 + i * 2], page_bytes[8 + i * 2 + 1]]) as usize;
        // skip payload size annd header size
        (_, offset) = handle_varint(page_bytes, offset);
        let rowid;
        (rowid, offset) = handle_varint(page_bytes, offset);
        (_, offset) = handle_varint(page_bytes, offset);

        let mut element_prop = Vec::new();
        for _ in 0..entry.tbl_columns.len() {
            let length;
            (length, offset) = handle_varint(page_bytes, offset);
            element_prop.push(get_serial_type(length));
        }

        let mut row = Vec::new();
        for (length, data_type) in element_prop {
            if length == 0 {
                row.push(rowid.to_string());
                continue;
            }
            if data_type == "TEXT" {
                let value =
                    String::from_utf8_lossy(&page_bytes[offset..offset + length]).to_string();
                row.push(value);
                offset += length;
                continue;
            }
            let mut value = 0;
            for i in 0..length {
                let byte = page_bytes[offset + i];
                value = (value << 8) | u64::from(byte);
            }
            row.push(value.to_string());
            offset += length;
        }
        rows.push(row);
    }
    rows
}

pub fn get_target_rowids(
    file: &mut File,
    page_size: u16,
    page_num: u32,
    target: &str,
) -> Result<Vec<u32>> {
    let page_bytes = pager::get_page_bytes(file, page_size, page_num)?;
    let page_type = page_bytes[0];
    if page_type == 0x0a {
        return get_target_rowids_leaf(&page_bytes, target);
    }
    if page_type == 0x02 {
        return get_target_rowids_interior(&page_bytes, target, file, page_size);
    }
    bail!("Unsupported page type: {page_type}");
}

fn get_target_rowids_leaf(page_bytes: &[u8], target: &str) -> Result<Vec<u32>> {
    let mut rows = Vec::new();
    let cell_count = u16::from_be_bytes([page_bytes[3], page_bytes[4]]) as usize;

    for i in 0..cell_count {
        let cell_offset =
            u16::from_be_bytes([page_bytes[8 + i * 2], page_bytes[8 + i * 2 + 1]]) as usize;

        let (idx_value, rowid_value) = parse_rowid_from_index_cell(page_bytes, cell_offset);
        if idx_value == target {
            rows.push(u32::try_from(rowid_value)?);
        }
    }
    Ok(rows)
}

fn get_target_rowids_interior(
    page_bytes: &[u8],
    target: &str,
    file: &mut File,
    page_size: u16,
) -> Result<Vec<u32>> {
    let mut rows = Vec::new();
    let cell_count = u16::from_be_bytes([page_bytes[3], page_bytes[4]]) as usize;
    let right_child_page =
        u32::from_be_bytes([page_bytes[8], page_bytes[9], page_bytes[10], page_bytes[11]]);

    for i in 0..cell_count {
        let cell_offset =
            u16::from_be_bytes([page_bytes[12 + i * 2], page_bytes[12 + i * 2 + 1]]) as usize;
        let child_page = u32::from_be_bytes([
            page_bytes[cell_offset],
            page_bytes[cell_offset + 1],
            page_bytes[cell_offset + 2],
            page_bytes[cell_offset + 3],
        ]);
        let (idx_value, rowid_value) = parse_rowid_from_index_cell(page_bytes, cell_offset + 4);

        if idx_value.as_str() > target {
            rows.extend(get_target_rowids(file, page_size, child_page, target)?);
            return Ok(rows);
        }
        if idx_value.as_str() == target {
            rows.extend(get_target_rowids(file, page_size, child_page, target)?);
            rows.push(u32::try_from(rowid_value)?);
        }
    }
    rows.extend(get_target_rowids(
        file,
        page_size,
        right_child_page,
        target,
    )?);
    Ok(rows)
}

fn parse_rowid_from_index_cell(page_bytes: &[u8], cell_offset: usize) -> (String, u64) {
    let (_, cell_offset) = handle_varint(page_bytes, cell_offset);
    let header_offset = cell_offset;
    let (header_length, cell_offset) = handle_varint(page_bytes, header_offset);
    let (idx_serial_type, cell_offset) = handle_varint(page_bytes, cell_offset);
    let (rowid_serial_type, _) = handle_varint(page_bytes, cell_offset);
    let mut cell_offset = header_offset + header_length;

    let (idx_length, _) = get_serial_type(idx_serial_type);
    let (rowid_length, _) = get_serial_type(rowid_serial_type);

    let idx_value =
        String::from_utf8_lossy(&page_bytes[cell_offset..cell_offset + idx_length]).to_string();
    cell_offset += idx_length;
    let mut rowid_value = 0;
    for i in 0..rowid_length {
        let byte = page_bytes[cell_offset + i];
        rowid_value = (rowid_value << 8) | u64::from(byte);
    }
    (idx_value, rowid_value)
}

pub fn get_row_by_rowid(
    file: &mut File,
    page_size: u16,
    page_num: u32,
    entry: &SchemaEntry,
    target_rowid: usize,
) -> Result<Vec<String>> {
    let page_bytes = pager::get_page_bytes(file, page_size, page_num)?;
    let page_type = page_bytes[0];
    if page_type == 0x0d {
        return get_row_by_rowid_leaf(&page_bytes, entry, target_rowid);
    }
    if page_type == 0x05 {
        let cell_count = u16::from_be_bytes([page_bytes[3], page_bytes[4]]) as usize;
        let right_child_page =
            u32::from_be_bytes([page_bytes[8], page_bytes[9], page_bytes[10], page_bytes[11]]);

        for i in 0..cell_count {
            let cell_offset =
                u16::from_be_bytes([page_bytes[12 + i * 2], page_bytes[12 + i * 2 + 1]]) as usize;
            let child_page = u32::from_be_bytes([
                page_bytes[cell_offset],
                page_bytes[cell_offset + 1],
                page_bytes[cell_offset + 2],
                page_bytes[cell_offset + 3],
            ]);
            let (rowid, _) = handle_varint(&page_bytes, cell_offset + 4);
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
    let cell_count = u16::from_be_bytes([page_bytes[3], page_bytes[4]]) as usize;

    for i in 0..cell_count {
        let mut offset =
            u16::from_be_bytes([page_bytes[8 + i * 2], page_bytes[8 + i * 2 + 1]]) as usize;
        // skip payload size annd header size
        (_, offset) = handle_varint(page_bytes, offset);
        let rowid;
        (rowid, offset) = handle_varint(page_bytes, offset);
        (_, offset) = handle_varint(page_bytes, offset);

        if rowid != target_rowid {
            continue;
        }

        let mut element_prop = Vec::new();
        for _ in 0..entry.tbl_columns.len() {
            let length;
            (length, offset) = handle_varint(page_bytes, offset);
            element_prop.push(get_serial_type(length));
        }

        let mut row = Vec::new();
        for (length, data_type) in element_prop {
            if length == 0 {
                row.push(rowid.to_string());
                continue;
            }
            if data_type == "TEXT" {
                let value =
                    String::from_utf8_lossy(&page_bytes[offset..offset + length]).to_string();
                row.push(value);
                offset += length;
                continue;
            }
            let mut value = 0;
            for i in 0..length {
                let byte = page_bytes[offset + i];
                value = (value << 8) | u64::from(byte);
            }
            row.push(value.to_string());
            offset += length;
        }
        return Ok(row);
    }
    bail!("Rowid {target_rowid} not found in leaf page");
}

fn get_serial_type(length: usize) -> (usize, String) {
    match length {
        0 => (0, "NULL".to_string()),
        l if (1..=4).contains(&l) => (l, "INT".to_string()),
        5 => (6, "INT".to_string()),
        6 => (8, "INT".to_string()),
        7 => (8, "FLOAT".to_string()),
        8 => (0, "ZERO".to_string()),
        9 => (0, "ONE".to_string()),
        l if l >= 13 && l % 2 == 1 => ((l - 13) / 2, "TEXT".to_string()),
        l if l >= 12 && l % 2 == 0 => ((l - 12) / 2, "BLOB".to_string()),
        _ => panic!("Invalid serial type length: {length}"),
    }
}
