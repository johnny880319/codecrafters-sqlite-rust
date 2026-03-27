pub struct Header {
    pub page_size: u16,
}

pub struct Page {
    pub num_cells: u16,
}

pub fn parse_header(raw_bytes: &[u8]) -> (Header, usize) {
    let page_size = u16::from_be_bytes([raw_bytes[16], raw_bytes[17]]);
    (Header { page_size }, 100)
}

pub fn parse_page(raw_bytes: &[u8], offset: usize) -> Page {
    let num_cells = u16::from_be_bytes([raw_bytes[offset + 3], raw_bytes[offset + 4]]);
    Page { num_cells }
}

pub fn parse_table_name(raw_bytes: &[u8], offset: usize) -> String {
    let header_length = raw_bytes[offset + 2] as usize;
    let type_length = (raw_bytes[offset + 3] as usize - 13) / 2;
    let name_length = (raw_bytes[offset + 4] as usize - 13) / 2;
    print!("{:?}", &raw_bytes[offset + 2..offset + 5]);
    let name_start_offset = offset + 2 + header_length + type_length;
    let name_end_offset = name_start_offset + name_length;
    String::from_utf8_lossy(&raw_bytes[name_start_offset..name_end_offset]).to_string()
}
