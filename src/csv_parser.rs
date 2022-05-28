use serde::Deserialize;
use std::fs::File;
use std::io::Read;
use std::path::Path;

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
    pub record_type: String,

    #[serde(rename(deserialize = "client"))]
    pub client_id: u16,

    #[serde(rename(deserialize = "tx"))]
    pub tx_id: u32,

    pub amount: f64, //:= maybe f128?
}

impl Record {
    #[cfg(test)]
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
    use std::io::Write;

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

    #[test]
    fn test_read_single_line() {
        use csv::{ByteRecord, StringRecord};

        let record = ByteRecord::from(vec!["deposit", "1", "1", "1.0000"]);

        assert_eq!(
            record.deserialize::<Record>(None).unwrap(),
            Record::new("deposit".into(), 1, 1, 1_f64)
        );
    }

    #[test]
    fn test_read_stream() -> Result<(), std::io::Error> {
        use csv::ByteRecord;
        use std::io::{BufRead, BufReader};
        use std::net::{TcpListener, TcpStream};
        use std::thread;

        let listener = TcpListener::bind("127.0.0.1:9090").unwrap();

        thread::spawn(move || {
            let test_case = "

    deposit,     1,1, 1.0000
    deposit,  2,2, 2
    deposit,  1,3, 2
    withdrawal,  1,     4, 1.5000
        withdrawal,2,5,3.0
    ";
            let mut stream = TcpStream::connect("127.0.0.1:9090").unwrap();
            stream.write_all(test_case.as_bytes());
        });

        let result_case = vec![
            Record::new("deposit".into(), 1, 1, 1_f64),
            Record::new("deposit".into(), 2, 2, 2_f64),
            Record::new("deposit".into(), 1, 3, 2_f64),
            Record::new("withdrawal".into(), 1, 4, 1.5_f64),
            Record::new("withdrawal".into(), 2, 5, 3.0_f64),
        ];

        for stream in listener.incoming() {
            let mut br = BufReader::new(stream?);
            let mut line_buffer = String::new();

            let mut nth = 0;

            loop {
                match br.read_line(&mut line_buffer) {
                    Ok(a) if a == 0 => break,
                    Err(_) => break,
                    _ => {
                        //dbg!(&line_buffer);
                        let clean_line = line_buffer
                            .split(|c| c == ' ' || c == ',' || c == '\n')
                            .filter(|&w| w != "")
                            .collect::<Vec<&str>>();

                        if clean_line.is_empty() {
                            break;
                        }

                        let record = ByteRecord::from(clean_line);

                        //dbg!(record.deserialize::<Record>(None));

                        assert_eq!(
                            result_case[nth],
                            record.deserialize::<Record>(None).unwrap()
                        );
                        line_buffer.clear();
                        nth += 1;
                    }
                }
            }
            break; // I only need the first stream connection;
        }

        Ok(())
    }
}
