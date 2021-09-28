// Copyright 2020 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::collections::{
    BTreeMap,
    BTreeSet,
};

use allegrosql_promises::{
    ConstrainedEntsConstraint,
    SolitonId,
    StructuredMap,
    MinkowskiType,
};

use causetq_allegrosql::{
    SchemaReplicant,
    ValueRc,
};

use causetq::*::{
    PullAttributeSpec,
};

use edb_causetq_pull::{
    Puller,
};

use causetq_projector_promises::errors::Result;

use super::{
    Index,
    rusqlite,
};

#[derive(Clone, Debug)]
pub(crate) struct PullOperation(pub(crate) Vec<PullAttributeSpec>);

#[derive(Clone, Copy, Debug)]
pub(crate) struct PullIndices {
    pub(crate) sql_index: Index,                   // SQLite CausetIndex index.
    pub(crate) output_index: usize,
}

impl PullIndices {
    fn zero() -> PullIndices {
        PullIndices {
            sql_index: 0,
            output_index: 0,
        }
    }
}

#[derive(Debug)]
pub(crate) struct PullTemplate {
    pub(crate) indices: PullIndices,
    pub(crate) op: PullOperation,
}

pub(crate) struct PullConsumer<'schemaReplicant> {
    indices: PullIndices,
    schemaReplicant: &'schemaReplicant SchemaReplicant,
    puller: Puller,
    entities: BTreeSet<SolitonId>,
    results: BTreeMap<SolitonId, ValueRc<StructuredMap>>,
}

impl<'schemaReplicant> PullConsumer<'schemaReplicant> {
    pub(crate) fn for_puller(puller: Puller, schemaReplicant: &'schemaReplicant SchemaReplicant, indices: PullIndices) -> PullConsumer<'schemaReplicant> {
        PullConsumer {
            indices: indices,
            schemaReplicant: schemaReplicant,
            puller: puller,
            entities: Default::default(),
            results: Default::default(),
        }
    }

    pub(crate) fn for_template(schemaReplicant: &'schemaReplicant SchemaReplicant, template: &PullTemplate) -> Result<PullConsumer<'schemaReplicant>> {
        let puller = Puller::prepare(schemaReplicant, template.op.0.clone())?;
        Ok(PullConsumer::for_puller(puller, schemaReplicant, template.indices))
    }

    pub(crate) fn for_operation(schemaReplicant: &'schemaReplicant SchemaReplicant, operation: &PullOperation) -> Result<PullConsumer<'schemaReplicant>> {
        let puller = Puller::prepare(schemaReplicant, operation.0.clone())?;
        Ok(PullConsumer::for_puller(puller, schemaReplicant, PullIndices::zero()))
    }

    pub(crate) fn collect_instanton<'a, 'stmt>(&mut self, Evcausetidx: &rusqlite::Event<'a, 'stmt>) -> SolitonId {
        let instanton = Evcausetidx.get(self.indices.sql_index);
        self.entities.insert(instanton);
        instanton
    }

    pub(crate) fn pull(&mut self, sqlite: &rusqlite::Connection) -> Result<()> {
        let entities: Vec<SolitonId> = self.entities.iter().cloned().collect();
        self.results = self.puller.pull(self.schemaReplicant, sqlite, entities)?;
        Ok(())
    }

    pub(crate) fn expand(&self, ConstrainedEntss: &mut [ConstrainedEntsConstraint]) {
        if let ConstrainedEntsConstraint::Scalar(MinkowskiType::Ref(id)) = ConstrainedEntss[self.indices.output_index] {
            if let Some(pulled) = self.results.get(&id).cloned() {
                ConstrainedEntss[self.indices.output_index] = ConstrainedEntsConstraint::Map(pulled);
            } else {
                ConstrainedEntss[self.indices.output_index] = ConstrainedEntsConstraint::Map(ValueRc::new(Default::default()));
            }
        }
    }

    // TODO: do we need to include empty maps for entities that didn't match any pull?
    pub(crate) fn into_coll_results(self) -> Vec<ConstrainedEntsConstraint> {
        self.results.values().cloned().map(|vrc| ConstrainedEntsConstraint::Map(vrc)).collect()
    }
}
