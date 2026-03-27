use crate::{pager, parser};
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
    let (page_size, cell_count, _) = get_db_info(&mut file)?;

    println!("database page size: {page_size}");
    println!("number of tables: {cell_count}");

    Ok(())
}

fn cmd_tables(args: &[String]) -> Result<()> {
    let mut file = File::open(&args[1])?;
    let (_, cell_count, page_bytes) = get_db_info(&mut file)?;

    let cell_array_offset = if page_bytes[100] == 0x0d { 108 } else { 112 };

    let schema_entries = parser::parse_schema_entries(&page_bytes, cell_array_offset, cell_count);

    let mut table_name_list = schema_entries
        .into_iter()
        .filter(|entry| !entry.tbl_name.starts_with("sqlite_"))
        .map(|entry| entry.tbl_name)
        .collect::<Vec<_>>();

    table_name_list.sort_unstable();
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
    let (page_size, cell_count, page_bytes) = get_db_info(&mut file)?;

    let cell_array_offset = if page_bytes[100] == 0x0d { 108 } else { 112 };

    let schema_entries = parser::parse_schema_entries(&page_bytes, cell_array_offset, cell_count);

    for entry in schema_entries {
        if entry.tbl_name == target_table_name {
            let page_bytes = pager::get_page_bytes(&mut file, page_size, entry.root_page)?;
            let num_rows = parser::get_cell_count(&page_bytes, false);
            println!("{num_rows}");
            return Ok(());
        }
    }
    bail!("Table {target_table_name} not found");
}

fn get_db_info(file: &mut File) -> Result<(u16, u16, Vec<u8>)> {
    let page_size = pager::get_page_size(file)?;
    let page_bytes = pager::get_page_bytes(file, page_size, 1)?;
    let cell_count = parser::get_cell_count(&page_bytes, true);
    Ok((page_size, cell_count, page_bytes))
}
