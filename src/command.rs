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
        _ => cmd_sql_query(args),
    }
}

fn cmd_dbinfo(args: &[String]) -> Result<()> {
    let mut file = File::open(&args[1])?;
    let (page_size, num_tables, _) = parser::get_db_info(&mut file)?;

    println!("database page size: {page_size}");
    println!("number of tables: {num_tables}");

    Ok(())
}

fn cmd_tables(args: &[String]) -> Result<()> {
    let mut file = File::open(&args[1])?;
    let (_, num_tables, page_bytes) = parser::get_db_info(&mut file)?;

    let cell_array_offset = if page_bytes[100] == 0x0d { 108 } else { 112 };

    let mut table_name_list = Vec::new();
    for i in 0..num_tables {
        let cell_offset = u16::from_be_bytes([
            page_bytes[cell_array_offset + (i as usize) * 2],
            page_bytes[cell_array_offset + (i as usize) * 2 + 1],
        ]) as usize;
        let table_info = parser::parse_table_info(&page_bytes, cell_offset);

        if !table_info.tbl_name.starts_with("sqlite_") {
            table_name_list.push(table_info.tbl_name);
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

// Assume the command is "SELECT COUNT(*) FROM table_name" for now.
fn cmd_sql_query(args: &[String]) -> Result<()> {
    let target_table_name = args[2].split_whitespace().nth(3).unwrap();

    let mut file = File::open(&args[1])?;
    let (page_size, num_tables, page_bytes) = parser::get_db_info(&mut file)?;

    let cell_array_offset = if page_bytes[100] == 0x0d { 108 } else { 112 };

    for i in 0..num_tables {
        let cell_offset = u16::from_be_bytes([
            page_bytes[cell_array_offset + (i as usize) * 2],
            page_bytes[cell_array_offset + (i as usize) * 2 + 1],
        ]) as usize;
        let table_info = parser::parse_table_info(&page_bytes, cell_offset);

        if table_info.tbl_name == target_table_name {
            let page_bytes = parser::get_page_bytes(&mut file, page_size, table_info.root_page)?;
            let num_rows = parser::get_cell_count(&page_bytes, false);
            println!("{num_rows}");
            return Ok(());
        }
    }
    Ok(())
}
