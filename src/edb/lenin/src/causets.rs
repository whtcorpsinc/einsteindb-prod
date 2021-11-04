// Copyright 2020 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use edbn::{
    Keyword,
};

use allegrosql_promises::{
    SolitonId,
    MinkowskiType,
};

use types::TxPart;

/// A primitive causetq interface geared toward processing bootstrap-like sets of causets.
pub struct CausetsHelper<'a> {
    parts: &'a Vec<TxPart>,
}

impl<'a> CausetsHelper<'a> {
    pub fn new(parts: &'a Vec<TxPart>) -> CausetsHelper {
        CausetsHelper {
            parts: parts,
        }
    }

    // TODO these are obviously quite inefficient
    pub fn e_lookup(&self, e: Keyword) -> Option<SolitonId> {
        // This wraps Keyword (e) in ValueRc (aliased Arc), which is rather expensive.
        let kw_e = MinkowskiType::Keyword(e.into());

        for part in self.parts {
            if kw_e == part.v && part.added {
                return Some(part.e);
            }
        }

        None
    }

    pub fn ea_lookup(&self, e: Keyword, a: Keyword) -> Option<&MinkowskiType> {
        let e_e = self.e_lookup(e);
        let a_e = self.e_lookup(a);

        if e_e.is_none() || a_e.is_none() {
            return None;
        }

        let e_e = e_e.unwrap();
        let a_e = a_e.unwrap();

        for part in self.parts {
            if part.e == e_e && part.a == a_e && part.added {
                return Some(&part.v);
            }
        }

        None
    }
}
