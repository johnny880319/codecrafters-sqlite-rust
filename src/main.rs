use anyhow::{Result, bail};
use std::{fs::File, io::Read as _};
mod parser;

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Parse command and act accordingly
    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            let mut file = File::open(&args[1])?;
            let mut raw_bytes = [0; 200];
            file.read_exact(&mut raw_bytes)?;

            let (header, offset) = parser::parse_header(&raw_bytes);
            let page = parser::parse_page(&raw_bytes, offset);

            println!("database page size: {}", header.page_size);
            println!("number of tables: {}", page.num_cells);

            let page_size = u16::from_be_bytes([raw_bytes[16], raw_bytes[17]]);

            println!("database page size: {page_size}");
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
