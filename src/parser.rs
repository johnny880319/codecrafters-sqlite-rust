pub fn get_cell_count(page_bytes: &[u8], is_root: bool) -> u16 {
    if is_root {
        u16::from_be_bytes([page_bytes[103], page_bytes[104]])
    } else {
        u16::from_be_bytes([page_bytes[3], page_bytes[4]])
    }
}

pub struct SchemaEntry {
    pub tbl_name: String,
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
    SchemaEntry {
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
