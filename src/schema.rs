use crate::utils;

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
