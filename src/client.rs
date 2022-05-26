use csv::Writer;
use serde::{Serialize, Serializer};
use std::collections::{HashMap, HashSet};

use crate::csv_parser::Record;

#[derive(Debug, PartialEq)]
pub enum Transactions {
    Deposit(u16, u32, f64),
    Withdrawal(u16, u32, f64),
    Dispute(u16, u32),
    Resolve(u16, u32),
    Chargeback(u16, u32),
}

impl From<&Record> for Transactions {
    fn from(r: &Record) -> Self {
        let Record {
            record_type,
            client_id,
            tx_id,
            amount,
        }: &Record = r;

        match record_type.to_lowercase().as_str() {
            "deposit" => Transactions::Deposit(*client_id, *tx_id, *amount),
            "withdrawal" => Transactions::Withdrawal(*client_id, *tx_id, *amount),
            "dispute" => Transactions::Dispute(*client_id, *tx_id),
            "resolve" => Transactions::Resolve(*client_id, *tx_id),
            "chargeback" => Transactions::Chargeback(*client_id, *tx_id),
            _ => unreachable!(), //:= here should panic actually
        }
    }
}

impl Transactions {
    pub fn get_client_id(&self) -> u16 {
        match self {
            &Transactions::Deposit(client_id, _, _) => client_id,
            &Transactions::Withdrawal(client_id, _, _) => client_id,
            &Transactions::Dispute(client_id, _) => client_id,
            &Transactions::Resolve(client_id, _) => client_id,
            &Transactions::Chargeback(client_id, _) => client_id,
        }
    }
}

#[derive(Debug, PartialEq, Default, Serialize)]
pub struct Client {
    #[serde(rename(serialize = "client"))]
    id: u16,

    #[serde(rename(serialize = "available"), serialize_with = "four_place")]
    available: f64,

    #[serde(serialize_with = "four_place")]
    held: f64,

    #[serde(serialize_with = "four_place")]
    total: f64,

    locked: bool,

    // store all Deposit, Withdrawal transactions of this client
    #[serde(skip_serializing)]
    transaction_map: HashMap<u32, Transactions>,

    // dispute transactions
    #[serde(skip_serializing)]
    dispute_transactions: HashSet<u32>,
}

impl Client {
    pub fn new(cid: u16) -> Self {
        let mut c: Self = Default::default();
        c.id = cid;
        c
    }

    pub fn handle_transaction(&mut self, mut tx: Transactions) -> Result<(), String> {
        match tx {
            Transactions::Deposit(_, tx_id, amount) => {
                self.available += amount;
                self.total += amount;
                if self.transaction_map.insert(tx_id, tx).is_some() {
                    // roll back
                    self.available -= amount;
                    self.total -= amount;

                    return Err(format!("duplicated tx id {} of client {}", tx_id, self.id));
                }
                Ok(())
            }

            Transactions::Withdrawal(_, tx_id, amount) => {
                if self.available < amount {
                    return Err(format!(
                        "withdrawal (tx id {}) amount less than available amount of client {}",
                        tx_id, self.id
                    ));
                }

                self.available -= amount;
                self.total -= amount;

                if self.transaction_map.insert(tx_id, tx).is_some() {
                    self.available += amount;
                    self.total += amount;
                    return Err(format!("duplicated tx id {} of client {}", tx_id, self.id));
                }
                Ok(())
            }

            Transactions::Dispute(_, tx_id) => match self.transaction_map.get(&tx_id) {
                Some(ttxx) => {
                    if !self.dispute_transactions.contains(&tx_id) {
                        match ttxx {
                            //:= I guess the dispute only works for Deposit?
                            Transactions::Deposit(_, _, amount) => {
                                self.available -= amount;
                                self.held += amount;

                                // store this transaction is disputed
                                self.dispute_transactions.insert(tx_id);
                            }
                            _ => {
                                return Err(format!(
                                    "dispute tx id {} of client {} isn't Deposit",
                                    tx_id, self.id
                                ))
                            }
                        }
                    }
                    Ok(())
                }
                None => Ok(()), // ignore it
            },

            Transactions::Resolve(_, tx_id) => {
                match self.dispute_transactions.take(&tx_id) {
                    Some(tx_id) => {
                        match self.transaction_map.get(&tx_id) {
                            Some(&Transactions::Deposit(_, _, amount)) => {
                                self.held -= amount;
                                self.available += amount;
                            }
                            _ => (), // ignore it again,,
                        }

                        Ok(())
                    }
                    None => Ok(()), // ignore it again,
                }
            }

            Transactions::Chargeback(_, tx_id) => {
                match self.dispute_transactions.take(&tx_id) {
                    Some(tx_id) => {
                        match self.transaction_map.get(&tx_id) {
                            Some(&Transactions::Deposit(_, _, amount)) => {
                                self.held -= amount;
                                self.total -= amount;
                                self.locked = true;
                            }
                            _ => (), // ignore it again,
                        }

                        Ok(())
                    }
                    None => Ok(()), // ignore it one more time,
                }
            }
        }
    }
}

pub fn write_csv<'a>(clients: impl Iterator<Item = &'a Client>) -> Result<String, String> {
    let mut wtr = Writer::from_writer(vec![]);
    for c in clients {
        wtr.serialize(c);
    }

    Ok(
        String::from_utf8(wtr.into_inner().map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?,
    )
}

fn four_place<S>(x: &f64, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    // lol: https://stackoverflow.com/a/63214916/4493361
    s.serialize_f64(f64::trunc(x * 10000.0) / 10000.0)
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_client_deposit() {
        let mut c0: Client = Default::default();
        c0.id = 1;

        let records = vec![
            Record {
                record_type: "deposit".into(),
                client_id: 1,
                tx_id: 1,
                amount: 1.0000_f64,
            },
            Record {
                record_type: "deposit".into(),
                client_id: 1,
                tx_id: 3,
                amount: 2.0000_f64,
            },
            Record {
                record_type: "withdrawal".into(),
                client_id: 1,
                tx_id: 4,
                amount: 1.5000_f64,
            },
        ];

        for r in records.iter() {
            c0.handle_transaction(r.into());
        }

        assert_eq!(c0.available, 1.5);
        assert_eq!(c0.total, 1.5);

        // dispute 1
        c0.handle_transaction(Transactions::Dispute(1, 1));
        assert_eq!(c0.available, 0.5);
        assert_eq!(c0.total, 1.5);
        assert_eq!(c0.held, 1_f64);

        // dispute 1 again, shouldn't happen anything
        c0.handle_transaction(Transactions::Dispute(1, 1));
        assert_eq!(c0.available, 0.5);
        assert_eq!(c0.total, 1.5);
        assert_eq!(c0.held, 1_f64);

        // resolve 1
        c0.handle_transaction(Transactions::Resolve(1, 1));
        assert_eq!(c0.available, 1.5);
        assert_eq!(c0.total, 1.5);
        assert_eq!(c0.held, 0_f64);

        // resolve 1 again, shouldn't happen anything
        c0.handle_transaction(Transactions::Resolve(1, 1));
        assert_eq!(c0.available, 1.5);
        assert_eq!(c0.total, 1.5);
        assert_eq!(c0.held, 0_f64);

        // nothing happen, dispute set doesn't has tx_id 1 anymore
        c0.handle_transaction(Transactions::Chargeback(1, 1));
        assert_eq!(c0.available, 1.5);
        assert_eq!(c0.total, 1.5);
        assert_eq!(c0.held, 0_f64);

        // dispute
        c0.handle_transaction(Transactions::Dispute(1, 1));
        // chargeback
        c0.handle_transaction(Transactions::Chargeback(1, 1));
        assert_eq!(c0.available, 0.5);
        assert_eq!(c0.total, 0.5);
        assert_eq!(c0.held, 0_f64);
    }

    #[test]
    fn test_clients_write_to_csv() {
        let mut c0: Client = Default::default();
        c0.id = 1;

        let records = vec![
            Record {
                record_type: "deposit".into(),
                client_id: 1,
                tx_id: 1,
                amount: 1.0000_f64,
            },
            Record {
                record_type: "deposit".into(),
                client_id: 1,
                tx_id: 3,
                amount: 2.0000_f64,
            },
            Record {
                record_type: "withdrawal".into(),
                client_id: 1,
                tx_id: 4,
                amount: 1.5000_f64,
            },
        ];

        for r in records.iter() {
            c0.handle_transaction(r.into());
        }

        let mut test_set = vec![c0];
        assert_eq!(
            write_csv(test_set.iter()),
            Ok("client,available,held,total,locked\n1,1.5,0.0,1.5,false\n".into())
        );

        let mut c1: Client = Client {
            id: 2,
            available: 0.123456789,
            held: 0.12345,
            total: 0.12345,
            locked: false,
            transaction_map: Default::default(),
            dispute_transactions: Default::default(),
        };

        test_set.push(c1);

        assert_eq!(
            write_csv(test_set.iter()),
            Ok("client,available,held,total,locked\n1,1.5,0.0,1.5,false\n2,0.1234,0.1234,0.1234,false\n".into())
        );
    }
}
