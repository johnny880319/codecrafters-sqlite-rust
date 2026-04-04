use anyhow::Result;
mod command;
mod pager;
mod parser;
mod utils;

fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    command::match_command(&args)?;
    Ok(())
}
