pub struct SchemaEntry {
    pub tbl_name: String,
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

pub fn get_table_rows(page_bytes: &[u8], entry: &SchemaEntry) -> Vec<Vec<String>> {
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
            element_prop.push(if length >= 13 {
                ((length - 13) / 2, "TEXT")
            } else {
                (length, "INT")
            });
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
