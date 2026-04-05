use crate::schema::SchemaEntry;

// varint
pub fn handle_varint(raw_bytes: &[u8], mut offset: usize) -> (usize, usize) {
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

// serial type
pub enum SerialType {
    Null,
    Int(usize),
    Float,
    Zero,
    One,
    Text(usize),
    Blob(usize),
}

impl SerialType {
    pub const fn length(&self) -> usize {
        match self {
            Self::Null | Self::Zero | Self::One => 0,
            Self::Int(len) | Self::Text(len) | Self::Blob(len) => *len,
            Self::Float => 8,
        }
    }
}

pub fn get_serial_type(serial_type: usize) -> SerialType {
    match serial_type {
        0 => SerialType::Null,
        st if (1..=4).contains(&st) => SerialType::Int(st),
        5 => SerialType::Int(6),
        6 => SerialType::Int(8),
        7 => SerialType::Float,
        8 => SerialType::Zero,
        9 => SerialType::One,
        st if st >= 13 && st % 2 == 1 => SerialType::Text((st - 13) / 2),
        st if st >= 12 && st % 2 == 0 => SerialType::Blob((st - 12) / 2),
        _ => panic!("Invalid serial type: {serial_type}"),
    }
}

// integer
pub fn bytes_to_usize(bytes: &[u8], start: usize, length: usize) -> usize {
    let mut result = 0;
    for i in 0..length {
        result <<= 8;
        result |= bytes[start + i] as usize;
    }
    result
}

// row retrieval
pub fn retrieve_row_elements(
    entry: &SchemaEntry,
    page_bytes: &[u8],
    header_offset: usize,
    rowid: usize,
) -> Vec<String> {
    let (header_size, mut offset) = handle_varint(page_bytes, header_offset);

    let mut element_prop = Vec::new();
    for _ in 0..entry.tbl_columns.len() {
        let length;
        (length, offset) = handle_varint(page_bytes, offset);
        element_prop.push(get_serial_type(length));
    }

    offset = header_offset + header_size;
    let mut row = Vec::new();
    for serial_type in element_prop {
        match serial_type {
            SerialType::Null => {
                row.push(rowid.to_string());
            }
            SerialType::Int(length) => {
                let value = bytes_to_usize(page_bytes, offset, length);
                row.push(value.to_string());
            }
            SerialType::Float => {
                let value = bytes_to_usize(page_bytes, offset, 8);
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
    row
}
