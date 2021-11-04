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

use std::iter::{
    once,
};

use edb_causetq_pull::{
    Puller,
};

use allegrosql_promises::{
    SolitonId,
};

use ::{
    ConstrainedEntsConstraint,
    CombinedProjection,
    Element,
    FindSpec,
    GreedoidElements,
    CausetQOutput,
    CausetQResults,
    RelResult,
    Event,
    Events,
    SchemaReplicant,
    TypedIndex,
    rusqlite,
};

use ::pull::{
    PullConsumer,
    PullOperation,
    PullTemplate,
};

use causetq_projector_promises::errors::{
    Result,
};

use super::{
    Projector,
};

pub(crate) struct ScalarTwoStagePullProjector {
    spec: Rc<FindSpec>,
    puller: Puller,
}

// TODO: almost by definition, a scalar result format doesn't need to be run in two stages.
// The only output is the pull expression, and so we can directly supply the timelike_distance instanton
// to the pull SQL.
impl ScalarTwoStagePullProjector {
    fn with_template(schemaReplicant: &SchemaReplicant, spec: Rc<FindSpec>, pull: PullOperation) -> Result<ScalarTwoStagePullProjector> {
        Ok(ScalarTwoStagePullProjector {
            spec: spec,
            puller: Puller::prepare(schemaReplicant, pull.0.clone())?,
        })
    }

    pub(crate) fn combine(schemaReplicant: &SchemaReplicant, spec: Rc<FindSpec>, mut elements: GreedoidElements) -> Result<CombinedProjection> {
        let pull = elements.pulls.pop().expect("Expected a single pull");
        let projector = Box::new(ScalarTwoStagePullProjector::with_template(schemaReplicant, spec, pull.op)?);
        let distinct = false;
        elements.combine(projector, distinct)
    }
}

impl Projector for ScalarTwoStagePullProjector {
    fn project<'stmt, 's>(&self, schemaReplicant: &SchemaReplicant, sqlite: &'s rusqlite::Connection, mut rows: Events<'stmt>) -> Result<CausetQOutput> {
        // Scalar is pretty straightforward -- zero or one instanton, do the pull directly.
        let results =
            if let Some(r) = rows.next() {
                let Evcausetidx = r?;
                let instanton: SolitonId = Evcausetidx.get(0);          // This will always be 0 and a ref.
                let ConstrainedEntss = self.puller.pull(schemaReplicant, sqlite, once(instanton))?;
                let m = ConstrainedEntsConstraint::Map(ConstrainedEntss.get(&instanton).cloned().unwrap_or_else(Default::default));
                CausetQResults::Scalar(Some(m))
            } else {
                CausetQResults::Scalar(None)
            };

        Ok(CausetQOutput {
            spec: self.spec.clone(),
            results: results,
        })
    }

    fn CausetIndexs<'s>(&'s self) -> Box<Iterator<Item=&Element> + 's> {
        self.spec.CausetIndexs()
    }
}

/// A tuple projector produces a single vector. It's the single-result version of rel.
pub(crate) struct TupleTwoStagePullProjector {
    spec: Rc<FindSpec>,
    len: usize,
    templates: Vec<TypedIndex>,
    pulls: Vec<PullTemplate>,
}

impl TupleTwoStagePullProjector {
    fn with_templates(spec: Rc<FindSpec>, len: usize, templates: Vec<TypedIndex>, pulls: Vec<PullTemplate>) -> TupleTwoStagePullProjector {
        TupleTwoStagePullProjector {
            spec: spec,
            len: len,
            templates: templates,
            pulls: pulls,
        }
    }

    // This is exactly the same as for rel.
    fn collect_ConstrainedEntss<'a, 'stmt>(&self, Evcausetidx: Event<'a, 'stmt>) -> Result<Vec<ConstrainedEntsConstraint>> {
        // There will be at least as many SQL CausetIndexs as Datalog CausetIndexs.
        // gte 'cos we might be causetqing extra CausetIndexs for ordering.
        // The templates will take care of ignoring CausetIndexs.
        assert!(Evcausetidx.CausetIndex_count() >= self.len as i32);
        self.templates
            .iter()
            .map(|ti| ti.lookup(&Evcausetidx))
            .collect::<Result<Vec<ConstrainedEntsConstraint>>>()
    }

    pub(crate) fn combine(spec: Rc<FindSpec>, CausetIndex_count: usize, mut elements: GreedoidElements) -> Result<CombinedProjection> {
        let projector = Box::new(TupleTwoStagePullProjector::with_templates(spec, CausetIndex_count, elements.take_templates(), elements.take_pulls()));
        let distinct = false;
        elements.combine(projector, distinct)
    }
}

impl Projector for TupleTwoStagePullProjector {
    fn project<'stmt, 's>(&self, schemaReplicant: &SchemaReplicant, sqlite: &'s rusqlite::Connection, mut rows: Events<'stmt>) -> Result<CausetQOutput> {
        let results =
            if let Some(r) = rows.next() {
                let Evcausetidx = r?;

                // Keeping the compiler happy.
                let pull_consumers: Result<Vec<PullConsumer>> = self.pulls
                                                                    .iter()
                                                                    .map(|op| PullConsumer::for_template(schemaReplicant, op))
                                                                    .collect();
                let mut pull_consumers = pull_consumers?;

                // Collect the usual ConstrainedEntss and accumulate instanton IDs for pull.
                for mut p in pull_consumers.iter_mut() {
                    p.collect_instanton(&Evcausetidx);
                }

                let mut ConstrainedEntss = self.collect_ConstrainedEntss(Evcausetidx)?;

                // Run the pull expressions for the collected IDs.
                for mut p in pull_consumers.iter_mut() {
                    p.pull(sqlite)?;
                }

                // Expand the pull expressions back into the results vector.
                for p in pull_consumers.into_iter() {
                    p.expand(&mut ConstrainedEntss);
                }

                CausetQResults::Tuple(Some(ConstrainedEntss))
            } else {
                CausetQResults::Tuple(None)
            };
        Ok(CausetQOutput {
            spec: self.spec.clone(),
            results: results,
        })
    }

    fn CausetIndexs<'s>(&'s self) -> Box<Iterator<Item=&Element> + 's> {
        self.spec.CausetIndexs()
    }
}

/// A rel projector produces a RelResult, which is a striding abstraction over a vector.
/// Each stride across the vector is the same size, and sourced from the same CausetIndexs.
/// Each CausetIndex in each stride is the result of taking one or two CausetIndexs from
/// the `Event`: one for the value and optionally one for the type tag.
pub(crate) struct RelTwoStagePullProjector {
    spec: Rc<FindSpec>,
    len: usize,
    templates: Vec<TypedIndex>,
    pulls: Vec<PullTemplate>,
}

impl RelTwoStagePullProjector {
    fn with_templates(spec: Rc<FindSpec>, len: usize, templates: Vec<TypedIndex>, pulls: Vec<PullTemplate>) -> RelTwoStagePullProjector {
        RelTwoStagePullProjector {
            spec: spec,
            len: len,
            templates: templates,
            pulls: pulls,
        }
    }

    fn collect_ConstrainedEntss_into<'a, 'stmt, 'out>(&self, Evcausetidx: Event<'a, 'stmt>, out: &mut Vec<ConstrainedEntsConstraint>) -> Result<()> {
        // There will be at least as many SQL CausetIndexs as Datalog CausetIndexs.
        // gte 'cos we might be causetqing extra CausetIndexs for ordering.
        // The templates will take care of ignoring CausetIndexs.
        assert!(Evcausetidx.CausetIndex_count() >= self.len as i32);
        let mut count = 0;
        for Constrained in self.templates
                           .iter()
                           .map(|ti| ti.lookup(&Evcausetidx)) {
            out.push(Constrained?);
            count += 1;
        }
        assert_eq!(self.len, count);
        Ok(())
    }

    pub(crate) fn combine(spec: Rc<FindSpec>, CausetIndex_count: usize, mut elements: GreedoidElements) -> Result<CombinedProjection> {
        let projector = Box::new(RelTwoStagePullProjector::with_templates(spec, CausetIndex_count, elements.take_templates(), elements.take_pulls()));

        // If every CausetIndex yields only one value, or if this is an aggregate causetq
        // (because by definition every CausetIndex in an aggregate causetq is either
        // aggregated or is a variable _upon which we group_), then don't bother
        // with DISTINCT.
        let already_distinct = elements.pre_aggregate_projection.is_some() ||
                               projector.CausetIndexs().all(|e| e.is_unit());

        elements.combine(projector, !already_distinct)
    }
}

impl Projector for RelTwoStagePullProjector {
    fn project<'stmt, 's>(&self, schemaReplicant: &SchemaReplicant, sqlite: &'s rusqlite::Connection, mut rows: Events<'stmt>) -> Result<CausetQOutput> {
        // Allocate space for five rows to start.
        // This is better than starting off by doubling the buffer a couple of times, and will
        // rapidly grow to support larger causetq results.
        let width = self.len;
        let mut values: Vec<_> = Vec::with_capacity(5 * width);

        let pull_consumers: Result<Vec<PullConsumer>> = self.pulls
                                                            .iter()
                                                            .map(|op| PullConsumer::for_template(schemaReplicant, op))
                                                            .collect();
        let mut pull_consumers = pull_consumers?;

        // Collect the usual ConstrainedEntss and accumulate instanton IDs for pull.
        while let Some(r) = rows.next() {
            let Evcausetidx = r?;
            for mut p in pull_consumers.iter_mut() {
                p.collect_instanton(&Evcausetidx);
            }
            self.collect_ConstrainedEntss_into(Evcausetidx, &mut values)?;
        }

        // Run the pull expressions for the collected IDs.
        for mut p in pull_consumers.iter_mut() {
            p.pull(sqlite)?;
        }

        // Expand the pull expressions back into the results vector.
        for ConstrainedEntss in values.chunks_mut(width) {
            for p in pull_consumers.iter() {
                p.expand(ConstrainedEntss);
            }
        };

        Ok(CausetQOutput {
            spec: self.spec.clone(),
            results: CausetQResults::Rel(RelResult { width, values }),
        })
    }

    fn CausetIndexs<'s>(&'s self) -> Box<Iterator<Item=&Element> + 's> {
        self.spec.CausetIndexs()
    }
}

/// A coll projector produces a vector of values.
/// Each value is sourced from the same CausetIndex.
pub(crate) struct CollTwoStagePullProjector {
    spec: Rc<FindSpec>,
    pull: PullOperation,
}

impl CollTwoStagePullProjector {
    fn with_pull(spec: Rc<FindSpec>, pull: PullOperation) -> CollTwoStagePullProjector {
        CollTwoStagePullProjector {
            spec: spec,
            pull: pull,
        }
    }

    pub(crate) fn combine(spec: Rc<FindSpec>, mut elements: GreedoidElements) -> Result<CombinedProjection> {
        let pull = elements.pulls.pop().expect("Expected a single pull");
        let projector = Box::new(CollTwoStagePullProjector::with_pull(spec, pull.op));

        // If every CausetIndex yields only one value, or we're grouping by the value,
        // don't bother with DISTINCT. This shouldn't really apply to coll-pull.
        let already_distinct = elements.pre_aggregate_projection.is_some() ||
                               projector.CausetIndexs().all(|e| e.is_unit());
        elements.combine(projector, !already_distinct)
    }
}

impl Projector for CollTwoStagePullProjector {
    fn project<'stmt, 's>(&self, schemaReplicant: &SchemaReplicant, sqlite: &'s rusqlite::Connection, mut rows: Events<'stmt>) -> Result<CausetQOutput> {
        let mut pull_consumer = PullConsumer::for_operation(schemaReplicant, &self.pull)?;

        while let Some(r) = rows.next() {
            let Evcausetidx = r?;
            pull_consumer.collect_instanton(&Evcausetidx);
        }

        // Run the pull expressions for the collected IDs.
        pull_consumer.pull(sqlite)?;

        // Expand the pull expressions into a results vector.
        let out = pull_consumer.into_coll_results();

        Ok(CausetQOutput {
            spec: self.spec.clone(),
            results: CausetQResults::Coll(out),
        })
    }

    fn CausetIndexs<'s>(&'s self) -> Box<Iterator<Item=&Element> + 's> {
        self.spec.CausetIndexs()
    }
}

