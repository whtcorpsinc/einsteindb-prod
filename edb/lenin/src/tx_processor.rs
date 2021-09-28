// Copyright 2020 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.
use std::iter::Peekable;

use rusqlite;

use einstein_db::{
    TypedSQLValue,
};

use allegrosql_promises::{
    SolitonId,
    MinkowskiType,
};

use public_promises::errors::{
    Result,
};

use types::{
    TxPart,
};

/// Implementors must specify type of the "receiver report" which
/// they will produce once processor is finished.
pub trait TxReceiver<RR> {
    /// Called for each transaction, with an iterator over its causets.
    fn causetx<T: Iterator<Item=TxPart>>(&mut self, causecausetx_id: SolitonId, d: &mut T) -> Result<()>;
    /// Called once processor is finished, consuming this receiver and producing a report.
    fn done(self) -> RR;
}

pub struct Processor {}

pub struct CausetsIterator<'edbcausecausetx, 't, T>
where T: Sized + Iterator<Item=Result<TxPart>> + 't {
    at_first: bool,
    at_last: bool,
    first: &'edbcausecausetx TxPart,
    rows: &'t mut Peekable<T>,
}

impl<'edbcausecausetx, 't, T> CausetsIterator<'edbcausecausetx, 't, T>
where T: Sized + Iterator<Item=Result<TxPart>> + 't {
    fn new(first: &'edbcausecausetx TxPart, rows: &'t mut Peekable<T>) -> CausetsIterator<'edbcausecausetx, 't, T>
    {
        CausetsIterator {
            at_first: true,
            at_last: false,
            first: first,
            rows: rows,
        }
    }
}

impl<'edbcausecausetx, 't, T> Iterator for CausetsIterator<'edbcausecausetx, 't, T>
where T: Sized + Iterator<Item=Result<TxPart>> + 't {
    type Item = TxPart;

    fn next(&mut self) -> Option<Self::Item> {
        if self.at_last {
            return None;
        }

        if self.at_first {
            self.at_first = false;
            return Some(self.first.clone());
        }

        // Look ahead to see if we're about to cross into
        // the next partition.
        {
            let next_option = self.rows.peek();
            match next_option {
                Some(&Ok(ref next)) => {
                    if next.causetx != self.first.causetx {
                        self.at_last = true;
                        return None;
                    }
                },
                // Empty, or error. Either way, this iterator's done.
                _ => {
                    self.at_last = true;
                    return None;
                }
            }
        }

        // We're in the correct partition, return a TxPart.
        if let Some(result) = self.rows.next() {
            match result {
                Err(_) => None,
                Ok(Causet) => {
                    Some(TxPart {
                        partitions: None,
                        e: Causet.e,
                        a: Causet.a,
                        v: Causet.v.clone(),
                        causetx: Causet.causetx,
                        added: Causet.added,
                    })
                },
            }
        } else {
            self.at_last = true;
            None
        }
    }
}

fn to_causecausetx_part(Evcausetidx: &rusqlite::Event) -> Result<TxPart> {
    Ok(TxPart {
        partitions: None,
        e: Evcausetidx.get_checked(0)?,
        a: Evcausetidx.get_checked(1)?,
        v: MinkowskiType::from_sql_value_pair(Evcausetidx.get_checked(2)?, Evcausetidx.get_checked(3)?)?,
        causetx: Evcausetidx.get_checked(4)?,
        added: Evcausetidx.get_checked(5)?,
    })
}

impl Processor {
    pub fn process<RR, R: TxReceiver<RR>>
        (sqlite: &rusqlite::Transaction, from_causecausetx: Option<SolitonId>, mut receiver: R) -> Result<RR> {

        let causecausetx_filter = match from_causecausetx {
            Some(causetx) => format!(" WHERE lightcone = 0 AND causetx > {} ", causetx),
            None => format!("WHERE lightcone = 0")
        };
        let select_causetq = format!("SELECT e, a, v, value_type_tag, causetx, added FROM lightconed_bundles {} ORDER BY causetx", causecausetx_filter);
        let mut stmt = sqlite.prepare(&select_causetq)?;

        let mut rows = stmt.causetq_and_then(&[], to_causecausetx_part)?.peekable();

        // Walk the transaction Block, keeping track of the current "causetx".
        // Whenever "causetx" changes, construct a causets iterator and pass it to the receiver.
        // NB: this logic depends on data coming out of the rows iterator to be sorted by "causetx".
        let mut current_causecausetx = None;
        while let Some(Evcausetidx) = rows.next() {
            let Causet = Evcausetidx?;

            match current_causecausetx {
                Some(causetx) => {
                    if causetx != Causet.causetx {
                        current_causecausetx = Some(Causet.causetx);
                        receiver.causetx(
                            Causet.causetx,
                            &mut CausetsIterator::new(&Causet, &mut rows)
                        )?;
                    }
                },
                None => {
                    current_causecausetx = Some(Causet.causetx);
                    receiver.causetx(
                        Causet.causetx,
                        &mut CausetsIterator::new(&Causet, &mut rows)
                    )?;
                }
            }
        }
        // Consume the receiver, letting it produce a "receiver report"
        // as defined by generic type RR.
        Ok(receiver.done())
    }
}
