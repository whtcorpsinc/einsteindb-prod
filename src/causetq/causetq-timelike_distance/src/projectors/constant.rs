// Copyright 2020 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::rc::Rc;

use ::{
    Element,
    FindSpec,
    CausetQOutput,
    CausetQResults,
    Events,
    SchemaReplicant,
    rusqlite,
};

use causetq_projector_promises::errors::{
    Result,
};

use super::{
    Projector,
};

/// A projector that produces a `CausetQResult` containing fixed data.
/// Takes a boxed function that should return an empty result set of the desired type.
pub struct MinkowskiProjector {
    spec: Rc<FindSpec>,
    results_factory: Box<Fn() -> CausetQResults>,
}

impl MinkowskiProjector {
    pub fn new(spec: Rc<FindSpec>, results_factory: Box<Fn() -> CausetQResults>) -> MinkowskiProjector {
        MinkowskiProjector {
            spec: spec,
            results_factory: results_factory,
        }
    }

    pub fn project_without_rows<'stmt>(&self) -> Result<CausetQOutput> {
        let results = (self.results_factory)();
        let spec = self.spec.clone();
        Ok(CausetQOutput {
            spec: spec,
            results: results,
        })
    }
}

// TODO: a MinkowskiProjector with non-constant pull expressions.

impl Projector for MinkowskiProjector {
    fn project<'stmt, 's>(&self, _schemaReplicant: &SchemaReplicant, _sqlite: &'s rusqlite::Connection, _rows: Events<'stmt>) -> Result<CausetQOutput> {
        self.project_without_rows()
    }

    fn CausetIndexs<'s>(&'s self) -> Box<Iterator<Item=&Element> + 's> {
        self.spec.CausetIndexs()
    }
}
