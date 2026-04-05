use crate::{schema::SchemaEntry, table};
use anyhow::{Result, bail};
use std::fs::File;

pub struct SqlQuery {
    pub columns: Vec<String>,
    pub table: String,
    pub where_clause: Option<WhereClause>,
}

pub struct WhereClause {
    pub column: String,
    pub value: String,
}

pub fn query_by_index(
    rowids: Vec<usize>,
    schema_entries: &[SchemaEntry],
    sql_query: &SqlQuery,
    mut file: File,
    page_size: usize,
) -> Result<()> {
    let schema_entry = schema_entries
        .iter()
        .find(|entry| {
            entry.tbl_name == sql_query.table && entry.tbl_type.eq_ignore_ascii_case("TABLE")
        })
        .unwrap();
    let mut rows = Vec::new();
    for rowid in rowids {
        rows.push(table::get_target_row(
            &mut file,
            page_size,
            schema_entry.root_page,
            schema_entry,
            rowid,
        )?);
    }

    print_rows(rows, &sql_query.columns, schema_entry)
}

pub fn query_by_table(
    schema_entries: Vec<SchemaEntry>,
    sql_query: &SqlQuery,
    mut file: File,
    page_size: usize,
) -> Result<()> {
    for entry in schema_entries {
        if entry.tbl_name != sql_query.table || !entry.tbl_type.eq_ignore_ascii_case("TABLE") {
            continue;
        }

        let mut rows = table::get_all_rows(&mut file, page_size, entry.root_page, &entry)?;

        if let Some(where_clause) = &sql_query.where_clause {
            let where_clause_idx = entry
                .tbl_columns
                .iter()
                .position(|col| col == &where_clause.column);
            if let Some(where_clause_idx) = where_clause_idx {
                rows.retain(|row| row[where_clause_idx] == *where_clause.value);
            }
        }

        return print_rows(rows, &sql_query.columns, &entry);
    }
    bail!("Table {} not found", sql_query.table);
}

fn print_rows(rows: Vec<Vec<String>>, column_names: &[String], entry: &SchemaEntry) -> Result<()> {
    if column_names.len() == 1 && column_names[0].eq_ignore_ascii_case("COUNT(*)") {
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

pub fn parse_sql_query(mut sql: &str) -> Result<SqlQuery> {
    sql = sql.trim();
    sql = sql.strip_suffix(';').unwrap_or(sql);
    sql = sql.trim();

    let where_part;
    let where_idx = sql.to_uppercase().find("WHERE");
    (sql, where_part) = where_idx.map_or((sql, None), |idx| (&sql[..idx], Some(&sql[idx + 5..])));

    let split_sql = sql.split_whitespace().collect::<Vec<&str>>();

    let mut idx = 0;
    if !split_sql[idx].eq_ignore_ascii_case("SELECT") {
        bail!("Only support simple SQL query with format: SELECT column_name FROM table_name");
    }
    idx += 1;
    let mut columns = Vec::new();
    while !split_sql[idx].eq_ignore_ascii_case("FROM") {
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
        Some(WhereClause {
            column: col.trim().to_string(),
            value: val.trim().to_string().trim_matches('\'').to_string(),
        })
    } else {
        None
    };

    Ok(SqlQuery {
        columns,
        table,
        where_clause,
    })
}
