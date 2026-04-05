use crate::{
    index, pager,
    schema::{self, SchemaEntry},
    table, utils,
};
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
// "SELECT column_name FROM table_name"
fn cmd_sql_query(args: &[String]) -> Result<()> {
    let sql_query = parse_sql_query(&args[2])?;

    let mut file = File::open(&args[1])?;
    let (page_size, cell_count, page_bytes) = get_db_info(&mut file)?;

    let cell_array_offset = if page_bytes[100] == 0x0d { 108 } else { 112 };

    let schema_entries = schema::parse_schema_entries(&page_bytes, cell_array_offset, cell_count);

    if let Some((where_col, _)) = &sql_query.where_clause {
        for entry in &schema_entries {
            if entry.tbl_name == sql_query.table
                && entry.tbl_type.to_uppercase() == "INDEX"
                && entry.tbl_columns[0] == *where_col
            {
                let ids = index::get_target_rowids(
                    &mut file,
                    page_size,
                    entry.root_page,
                    &sql_query.where_clause.as_ref().unwrap().1,
                )?;
                return print_result_by_index(ids, &schema_entries, &sql_query, file, page_size);
            }
        }
    }

    print_result_by_table(schema_entries, sql_query, file, page_size)
}

fn print_result_by_index(
    ids: Vec<usize>,
    schema_entries: &[SchemaEntry],
    sql_query: &SqlQuery,
    mut file: File,
    page_size: usize,
) -> Result<()> {
    let schema_entry = schema_entries
        .iter()
        .find(|entry| entry.tbl_name == sql_query.table && entry.tbl_type.to_uppercase() == "TABLE")
        .unwrap();
    let mut rows = Vec::new();
    for id in ids {
        rows.push(table::get_target_row(
            &mut file,
            page_size,
            schema_entry.root_page,
            schema_entry,
            id,
        )?);
    }

    print_rows(rows, &sql_query.columns, schema_entry)
}

fn print_result_by_table(
    schema_entries: Vec<SchemaEntry>,
    sql_query: SqlQuery,
    mut file: File,
    page_size: usize,
) -> Result<()> {
    let (where_clause_col, where_clause_val) = if let Some((col, val)) = sql_query.where_clause {
        (Some(col), Some(val))
    } else {
        (None, None)
    };

    for entry in schema_entries {
        if entry.tbl_name != sql_query.table || entry.tbl_type.to_uppercase() != "TABLE" {
            continue;
        }

        let mut rows = table::get_all_rows(&mut file, page_size, entry.root_page, &entry)?;

        let where_clause_idx = if let Some(where_col) = where_clause_col {
            entry.tbl_columns.iter().position(|col| col == &where_col)
        } else {
            None
        };
        if let Some(where_idx) = where_clause_idx {
            rows.retain(|row| row[where_idx] == *where_clause_val.as_ref().unwrap());
        }

        return print_rows(rows, &sql_query.columns, &entry);
    }
    bail!("Table {} not found", sql_query.table);
}

fn print_rows(rows: Vec<Vec<String>>, column_names: &[String], entry: &SchemaEntry) -> Result<()> {
    if column_names.len() == 1 && column_names[0].to_uppercase() == "COUNT(*)" {
        println!("{}", rows.len());
        return Ok(());
    }
    let col_idx_list = column_names
        .iter()
        .map(|col_name| entry.tbl_columns.iter().position(|col| col == col_name))
        .collect::<Vec<_>>();

    for row in rows {
        for (i, col_idx) in col_idx_list.iter().enumerate() {
            if let Some(col_idx) = col_idx {
                print!("{}", row[*col_idx]);
            } else {
                bail!("Column index not found");
            }
            if i != col_idx_list.len() - 1 {
                print!("|");
            }
        }
        println!();
    }
    Ok(())
}

fn get_db_info(file: &mut File) -> Result<(usize, usize, Vec<u8>)> {
    let page_size = pager::get_page_size(file)?;
    let page_bytes = pager::get_page_bytes(file, page_size, 1)?;
    let cell_count = utils::bytes_to_usize(&page_bytes, 103, 2);
    Ok((page_size, cell_count, page_bytes))
}

struct SqlQuery {
    columns: Vec<String>,
    table: String,
    where_clause: Option<(String, String)>,
}

fn parse_sql_query(mut sql: &str) -> Result<SqlQuery> {
    sql = sql.trim();
    sql = sql.strip_suffix(';').unwrap_or(sql);
    sql = sql.trim();

    let where_part;
    let where_idx = sql.to_uppercase().find("WHERE");
    (sql, where_part) = where_idx.map_or((sql, None), |idx| (&sql[..idx], Some(&sql[idx + 5..])));

    let split_sql = sql.split_whitespace().collect::<Vec<&str>>();

    let mut idx = 0;
    if split_sql[idx].to_uppercase() != "SELECT" {
        bail!("Only support simple SQL query with format: SELECT column_name FROM table_name");
    }
    idx += 1;
    let mut columns = Vec::new();
    while split_sql[idx].to_uppercase() != "FROM" {
        columns.push(
            split_sql[idx]
                .strip_suffix(',')
                .unwrap_or(split_sql[idx])
                .to_string(),
        );
        idx += 1;
    }
    idx += 1;

    let table = split_sql[idx].to_string();

    let where_clause = if let Some(where_part) = where_part {
        let (col, val) = where_part.split_once('=').ok_or_else(|| {
            anyhow::anyhow!("Only support simple WHERE clause with format: column_name=value")
        })?;
        Some((
            col.trim().to_string(),
            val.trim().to_string().trim_matches('\'').to_string(),
        ))
    } else {
        None
    };

    Ok(SqlQuery {
        columns,
        table,
        where_clause,
    })
}
