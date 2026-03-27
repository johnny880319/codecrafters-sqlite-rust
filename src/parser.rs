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
