use crate::parser;
use anyhow::{Result, bail};
use std::fs::File;

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
    let page_size = parser::get_page_size(&mut file)?;
    let page_bytes = parser::get_page_bytes(&mut file, page_size)?;
    let num_tables = parser::get_table_count(&page_bytes, true);

    println!("database page size: {page_size}");
    println!("number of tables: {num_tables}");

    Ok(())
}

fn cmd_tables(args: &[String]) -> Result<()> {
    let mut file = File::open(&args[1])?;
    let page_size = parser::get_page_size(&mut file)?;
    let page_bytes = parser::get_page_bytes(&mut file, page_size)?;
    let num_tables = parser::get_table_count(&page_bytes, true);

    let cell_array_offset = if page_bytes[100] == 0x0d { 108 } else { 112 };

    let mut table_name_list = Vec::new();
    for i in 0..num_tables {
        let cell_offset = u16::from_be_bytes([
            page_bytes[cell_array_offset + (i as usize) * 2],
            page_bytes[cell_array_offset + (i as usize) * 2 + 1],
        ]) as usize;
        let table_name = parser::parse_table_name(&page_bytes, cell_offset);

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
