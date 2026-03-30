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

// Assume the command are one of the below for now
// "SELECT COUNT(*) FROM table_name"
// "SELECT column_name FROM table_name"
fn cmd_sql_query(args: &[String]) -> Result<()> {
    let sql_query = parse_sql_query(&args[2])?;
    let column_name = sql_query.columns[0].clone();
    let target_table_name = sql_query.table;

    let mut file = File::open(&args[1])?;
    let (page_size, cell_count, page_bytes) = get_db_info(&mut file)?;

    let cell_array_offset = if page_bytes[100] == 0x0d { 108 } else { 112 };

    let schema_entries = parser::parse_schema_entries(&page_bytes, cell_array_offset, cell_count);

    for entry in schema_entries {
        if entry.tbl_name != target_table_name {
            continue;
        }

        let page_bytes = pager::get_page_bytes(&mut file, page_size, entry.root_page)?;
        let rows = parser::get_table_rows(&page_bytes, &entry);
        if column_name.to_uppercase() == "COUNT(*)" {
            println!("{}", rows.len());
            return Ok(());
        }
        let col_idx = entry.tbl_columns.iter().position(|col| *col == column_name);
        for row in rows {
            if let Some(col_idx) = col_idx {
                println!("{}", row[col_idx]);
            } else {
                bail!("Column {column_name} not found in table {target_table_name}");
            }
        }
        return Ok(());
    }
    bail!("Table {target_table_name} not found");
}

fn get_db_info(file: &mut File) -> Result<(u16, u16, Vec<u8>)> {
    let page_size = pager::get_page_size(file)?;
    let page_bytes = pager::get_page_bytes(file, page_size, 1)?;
    let cell_count = u16::from_be_bytes([page_bytes[103], page_bytes[104]]);
    Ok((page_size, cell_count, page_bytes))
}

struct SqlQuery {
    columns: Vec<String>,
    table: String,
}

fn parse_sql_query(mut sql: &str) -> Result<SqlQuery> {
    sql = sql.trim();
    sql = sql.strip_suffix(';').unwrap_or(sql);
    sql = sql.trim();
    let sql = sql.to_uppercase();
    let splited_sql = sql.split_whitespace().collect::<Vec<&str>>();
    if splited_sql.len() != 4 {
        bail!("Only support simple SQL query with format: SELECT column_name FROM table_name");
    }
    if splited_sql[0] != "SELECT" || splited_sql[2] != "FROM" {
        bail!("Only support simple SQL query with format: SELECT column_name FROM table_name");
    }
    Ok(SqlQuery {
        columns: vec![splited_sql[1].to_string()],
        table: splited_sql[3].to_string(),
    })
}
