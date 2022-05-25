use std::collections::{HashMap, HashSet};

use crate::csv_parser::Record;

#[derive(Debug, PartialEq)]
enum Transactions {
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
    fn get_client_id(&self) -> u16 {
        match self {
            &Transactions::Deposit(client_id, _, _) => client_id,
            &Transactions::Withdrawal(client_id, _, _) => client_id,
            &Transactions::Dispute(client_id, _) => client_id,
            &Transactions::Resolve(client_id, _) => client_id,
            &Transactions::Chargeback(client_id, _) => client_id,
        }
    }
}

#[derive(Debug, PartialEq)]
struct Client {
    id: u16,
    available: f64,
    held: f64,
    total: f64,
    locked: bool,

    // store all Deposit, Withdrawal transactions of this client
    transaction_map: HashMap<u32, Transactions>,

    // dispute transactions
    dispute_transactions: HashSet<u32>,
}

impl Client {
    fn handle_transaction(&mut self, mut tx: Transactions) -> Result<(), String> {
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
                Some(ttxx) => match ttxx {
                    //:= I guess the dispute only works for Deposit?
                    Transactions::Deposit(_, _, amount) => {
                        //:= what if the available less than amount
                        self.available -= amount;
                        self.held += amount;

                        // store this transaction is disputed
                        self.dispute_transactions.insert(tx_id);

                        Ok(())
                    }
                    _ => {
                        return Err(format!(
                            "dispute tx id {} of client {} isn't Deposit",
                            tx_id, self.id
                        ))
                    }
                },
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

#[cfg(test)]
mod tests {
    //:= todo: test
}
