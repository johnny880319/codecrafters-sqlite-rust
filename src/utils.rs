pub fn bytes_to_usize(bytes: &[u8], start: usize, length: usize) -> usize {
    let mut result = 0;
    for i in 0..length {
        result <<= 8;
        result |= bytes[start + i] as usize;
    }
    result
}
