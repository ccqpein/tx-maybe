use std::env;
use std::io::{Error, ErrorKind};

use tx_maybe::entry_func;

fn main() -> Result<(), Error> {
    let mut args = env::args();
    let path = args
        .nth(1)
        .ok_or("no input file")
        .map_err(|_| Error::new(ErrorKind::InvalidInput, "no input file"))?;

    println!("{}", entry_func(path)?);

    Ok(())
}
