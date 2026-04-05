use crate::{
    index, pager,
    schema::{self},
    sql::{self},
    utils,
};
use anyhow::{Result, bail};
use std::fs::File;

pub fn execute(args: &[String]) -> Result<()> {
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
    let (page_size, cell_count, _) = read_db_header(&mut file)?;

    println!("database page size: {page_size}");
    println!("number of tables: {cell_count}");

    Ok(())
}

fn cmd_tables(args: &[String]) -> Result<()> {
    let mut file = File::open(&args[1])?;
    let (_, cell_count, page_bytes) = read_db_header(&mut file)?;

    let cell_array_offset = if page_bytes[100] == 0x0d { 108 } else { 112 };

    let schema_entries = schema::parse_schema_entries(&page_bytes, cell_array_offset, cell_count);

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

// Assume the command are one of the below for now
// "SELECT COUNT(*) FROM table_name"
// "SELECT column_name_1, column_name_2, ... FROM table_name (WHERE column_name=value)"
fn cmd_sql_query(args: &[String]) -> Result<()> {
    let sql_query = sql::parse_sql_query(&args[2])?;

    let mut file = File::open(&args[1])?;
    let (page_size, cell_count, page_bytes) = read_db_header(&mut file)?;

    let cell_array_offset = if page_bytes[100] == 0x0d { 108 } else { 112 };

    let schema_entries = schema::parse_schema_entries(&page_bytes, cell_array_offset, cell_count);

    if let Some(where_clause) = &sql_query.where_clause {
        let index_entry = schema_entries.iter().find(|entry| {
            entry.tbl_name == sql_query.table
                && entry.tbl_type.eq_ignore_ascii_case("INDEX")
                && entry.tbl_columns[0] == where_clause.column
        });
        if let Some(index_entry) = index_entry {
            let rowids = index::get_target_rowids(
                &mut file,
                page_size,
                index_entry.root_page,
                &where_clause.value,
            )?;
            return sql::query_by_index(rowids, &schema_entries, &sql_query, file, page_size);
        }
    }

    sql::query_by_table(schema_entries, &sql_query, file, page_size)
}

pub fn read_db_header(file: &mut File) -> Result<(usize, usize, Vec<u8>)> {
    let page_size = pager::get_page_size(file)?;
    let page_bytes = pager::get_page_bytes(file, page_size, 1)?;
    let cell_count = utils::bytes_to_usize(&page_bytes, 103, 2);
    Ok((page_size, cell_count, page_bytes))
}
