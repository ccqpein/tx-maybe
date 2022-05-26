use std::{collections::HashMap, io::Error, io::ErrorKind, path::Path};

use csv::ReaderBuilder;

/// csv parser part
/// also the defination of record
mod csv_parser;
use csv_parser::*;

/// client part
mod client;
use client::*;

/// glue function
pub fn entry_func(path: impl AsRef<Path>) -> Result<(), Error> {
    let raw_data = handle_transaction_file(path)?;
    let mut rdr = ReaderBuilder::new().from_reader(raw_data.as_slice());

    let mut client_map = HashMap::new();

    for record in rdr.deserialize::<Record>() {
        let r = record.map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
        let tx: Transactions = (&r).into();
        client_map
            .entry(tx.get_client_id())
            .or_insert(Client::new(tx.get_client_id()))
            .handle_transaction(tx)
            .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
    }

    write_csv(client_map.values())
        .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;

    Ok(())
}
