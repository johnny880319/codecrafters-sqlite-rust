use anyhow::Result;
mod command;
mod parser;

fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    command::match_command(&args)?;
    Ok(())
}
