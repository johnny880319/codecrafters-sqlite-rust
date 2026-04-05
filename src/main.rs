use anyhow::Result;
mod command;
mod index;
mod pager;
mod schema;
mod table;
mod utils;

fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    command::match_command(&args)?;
    Ok(())
}
