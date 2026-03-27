use crate::parser;
use anyhow::{Result, bail};
use std::{fs::File, io::Read as _};

pub fn match_command(args: &[String]) -> Result<()> {
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => cmd_dbinfo(args),
        ".tables" => cmd_tables(args),
        _ => bail!("Unknown command: {command}"),
    }
}

fn cmd_dbinfo(args: &[String]) -> Result<()> {
    let mut file = File::open(&args[1])?;
    let mut raw_bytes = [0; 4096];
    file.read_exact(&mut raw_bytes)?;

    let (header, offset) = parser::parse_header(&raw_bytes);
    let page = parser::parse_page(&raw_bytes, offset);

    println!("database page size: {}", header.page_size);
    println!("number of tables: {}", page.num_cells);

    let page_size = u16::from_be_bytes([raw_bytes[16], raw_bytes[17]]);

    println!("database page size: {page_size}");
    Ok(())
}

fn cmd_tables(args: &[String]) -> Result<()> {
    let mut file = File::open(&args[1])?;
    let mut raw_bytes = [0; 4096];
    file.read_exact(&mut raw_bytes)?;

    let (_, offset) = parser::parse_header(&raw_bytes);
    let page = parser::parse_page(&raw_bytes, offset);

    let cell_array_offset = if raw_bytes[100] == 0x0d { 108 } else { 112 };

    let mut table_name_list = Vec::new();
    eprint!("{cell_array_offset}");
    for i in 0..page.num_cells {
        let cell_offset = u16::from_be_bytes([
            raw_bytes[cell_array_offset + (i as usize) * 2],
            raw_bytes[cell_array_offset + (i as usize) * 2 + 1],
        ]) as usize;
        let table_name = parser::parse_table_name(&raw_bytes, cell_offset);

        if !table_name.starts_with("sqlite_") {
            table_name_list.push(table_name);
        }
    }
    table_name_list.sort();
    for (i, table_name) in table_name_list.iter().enumerate() {
        print!("{table_name}");
        if i != table_name_list.len() - 1 {
            print!(" ");
        }
    }
    Ok(())
}
