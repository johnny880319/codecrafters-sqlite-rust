pub fn bytes_to_usize(bytes: &[u8], start: usize, length: usize) -> usize {
    let mut result = 0;
    for i in 0..length {
        result <<= 8;
        result |= bytes[start + i] as usize;
    }
    result
}

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
