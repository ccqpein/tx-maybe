use csv::Reader;
use serde::ser::StdError;
use serde::Deserialize;
use std::fs::File;
use std::io::{BufReader, Read};
use std::{error::Error, path::Path};

pub fn handle_transaction_file(p: impl AsRef<Path>) -> Result<Vec<u8>, std::io::Error> {
    let mut f = File::open(p)?;
    let mut contents = String::new();
    f.read_to_string(&mut contents)?; //:= may has perfermance issue when the file is too large

    Ok(clean(contents.bytes()))
}

fn clean(content: impl Iterator<Item = u8>) -> Vec<u8> {
    content.filter(|b| *b != b' ').collect()
}

/// data type of each record in csv
#[derive(Debug, Deserialize, PartialEq)]
pub struct Record {
    #[serde(rename(deserialize = "type"))]
    record_type: String,

    #[serde(rename(deserialize = "client"))]
    client_id: u16,

    #[serde(rename(deserialize = "tx"))]
    tx_id: u32,

    amount: f64, //:= maybe f128?
}

impl Record {
    fn new(record_type: String, client_id: u16, tx_id: u32, amount: f64) -> Self {
        Self {
            record_type,
            client_id,
            tx_id,
            amount,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use csv::ReaderBuilder;

    #[test]
    fn test_read() {
        let test_case = "\
        type,  client,  tx,     amount

deposit,     1,1, 1.0000
deposit,  2,2, 2
deposit,  1,3, 2
withdrawal,  1,     4, 1.5000
    withdrawal,2,5,3.0
";
        let result_case = vec![
            Record::new("deposit".into(), 1, 1, 1_f64),
            Record::new("deposit".into(), 2, 2, 2_f64),
            Record::new("deposit".into(), 1, 3, 2_f64),
            Record::new("withdrawal".into(), 1, 4, 1.5_f64),
            Record::new("withdrawal".into(), 2, 5, 3.0_f64),
        ];

        let clean_data = clean(test_case.bytes());
        let mut rdr = ReaderBuilder::new().from_reader(clean_data.as_slice());
        //dbg!(rdr.headers());
        for (ind, record) in rdr.deserialize::<Record>().enumerate() {
            let r: Record = record.unwrap();
            assert_eq!(r, result_case[ind]);
        }
    }
}