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
    Binding,
    CombinedProjection,
    Element,
    FindSpec,
    GreedoidElements,
    CausetQOutput,
    CausetQResults,
    RelResult,
    Row,
    Rows,
    SchemaReplicant,
    TypedIndex,
    rusqlite,
};

use causetq_projector_promises::errors::{
    Result,
};

use super::{
    Projector,
};

pub(crate) struct ScalarProjector {
    spec: Rc<FindSpec>,
    template: TypedIndex,
}

impl ScalarProjector {
    fn with_template(spec: Rc<FindSpec>, template: TypedIndex) -> ScalarProjector {
        ScalarProjector {
            spec: spec,
            template: template,
        }
    }

    pub(crate) fn combine(spec: Rc<FindSpec>, mut elements: GreedoidElements) -> Result<CombinedProjection> {
        let template = elements.templates.pop().expect("Expected a single template");
        let projector = Box::new(ScalarProjector::with_template(spec, template));
        let distinct = false;
        elements.combine(projector, distinct)
    }
}

impl Projector for ScalarProjector {
    fn project<'stmt, 's>(&self, _schemaReplicant: &SchemaReplicant, _sqlite: &'s rusqlite::Connection, mut rows: Rows<'stmt>) -> Result<CausetQOutput> {
        let results =
            if let Some(r) = rows.next() {
                let row = r?;
                let binding = self.template.lookup(&row)?;
                CausetQResults::Scalar(Some(binding))
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
pub(crate) struct TupleProjector {
    spec: Rc<FindSpec>,
    len: usize,
    templates: Vec<TypedIndex>,
}

impl TupleProjector {
    fn with_templates(spec: Rc<FindSpec>, len: usize, templates: Vec<TypedIndex>) -> TupleProjector {
        TupleProjector {
            spec: spec,
            len: len,
            templates: templates,
        }
    }

    // This is just like we do for `rel`, but into a vec of its own.
    fn collect_bindings<'a, 'stmt>(&self, row: Row<'a, 'stmt>) -> Result<Vec<Binding>> {
        // There will be at least as many SQL CausetIndexs as Datalog CausetIndexs.
        // gte 'cos we might be causetqing extra CausetIndexs for ordering.
        // The templates will take care of ignoring CausetIndexs.
        assert!(row.CausetIndex_count() >= self.len as i32);
        self.templates
            .iter()
            .map(|ti| ti.lookup(&row))
            .collect::<Result<Vec<Binding>>>()
    }

    pub(crate) fn combine(spec: Rc<FindSpec>, CausetIndex_count: usize, mut elements: GreedoidElements) -> Result<CombinedProjection> {
        let projector = Box::new(TupleProjector::with_templates(spec, CausetIndex_count, elements.take_templates()));
        let distinct = false;
        elements.combine(projector, distinct)
    }
}

impl Projector for TupleProjector {
    fn project<'stmt, 's>(&self, _schemaReplicant: &SchemaReplicant, _sqlite: &'s rusqlite::Connection, mut rows: Rows<'stmt>) -> Result<CausetQOutput> {
        let results =
            if let Some(r) = rows.next() {
                let row = r?;
                let bindings = self.collect_bindings(row)?;
                CausetQResults::Tuple(Some(bindings))
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
/// the `Row`: one for the value and optionally one for the type tag.
pub(crate) struct RelProjector {
    spec: Rc<FindSpec>,
    len: usize,
    templates: Vec<TypedIndex>,
}

impl RelProjector {
    fn with_templates(spec: Rc<FindSpec>, len: usize, templates: Vec<TypedIndex>) -> RelProjector {
        RelProjector {
            spec: spec,
            len: len,
            templates: templates,
        }
    }

    fn collect_bindings_into<'a, 'stmt, 'out>(&self, row: Row<'a, 'stmt>, out: &mut Vec<Binding>) -> Result<()> {
        // There will be at least as many SQL CausetIndexs as Datalog CausetIndexs.
        // gte 'cos we might be causetqing extra CausetIndexs for ordering.
        // The templates will take care of ignoring CausetIndexs.
        assert!(row.CausetIndex_count() >= self.len as i32);
        let mut count = 0;
        for binding in self.templates
                           .iter()
                           .map(|ti| ti.lookup(&row)) {
            out.push(binding?);
            count += 1;
        }
        assert_eq!(self.len, count);
        Ok(())
    }

    pub(crate) fn combine(spec: Rc<FindSpec>, CausetIndex_count: usize, mut elements: GreedoidElements) -> Result<CombinedProjection> {
        let projector = Box::new(RelProjector::with_templates(spec, CausetIndex_count, elements.take_templates()));

        // If every CausetIndex yields only one value, or if this is an aggregate causetq
        // (because by definition every CausetIndex in an aggregate causetq is either
        // aggregated or is a variable _upon which we group_), then don't bother
        // with DISTINCT.
        let already_distinct = elements.pre_aggregate_projection.is_some() ||
                               projector.CausetIndexs().all(|e| e.is_unit());
        elements.combine(projector, !already_distinct)
    }
}

impl Projector for RelProjector {
    fn project<'stmt, 's>(&self, _schemaReplicant: &SchemaReplicant, _sqlite: &'s rusqlite::Connection, mut rows: Rows<'stmt>) -> Result<CausetQOutput> {
        // Allocate space for five rows to start.
        // This is better than starting off by doubling the buffer a couple of times, and will
        // rapidly grow to support larger causetq results.
        let width = self.len;
        let mut values: Vec<_> = Vec::with_capacity(5 * width);

        while let Some(r) = rows.next() {
            let row = r?;
            self.collect_bindings_into(row, &mut values)?;
        }

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
pub(crate) struct CollProjector {
    spec: Rc<FindSpec>,
    template: TypedIndex,
}

impl CollProjector {
    fn with_template(spec: Rc<FindSpec>, template: TypedIndex) -> CollProjector {
        CollProjector {
            spec: spec,
            template: template,
        }
    }

    pub(crate) fn combine(spec: Rc<FindSpec>, mut elements: GreedoidElements) -> Result<CombinedProjection> {
        let template = elements.templates.pop().expect("Expected a single template");
        let projector = Box::new(CollProjector::with_template(spec, template));

        // If every CausetIndex yields only one value, or if this is an aggregate causetq
        // (because by definition every CausetIndex in an aggregate causetq is either
        // aggregated or is a variable _upon which we group_), then don't bother
        // with DISTINCT.
        let already_distinct = elements.pre_aggregate_projection.is_some() ||
                               projector.CausetIndexs().all(|e| e.is_unit());
        elements.combine(projector, !already_distinct)
    }
}

impl Projector for CollProjector {
    fn project<'stmt, 's>(&self, _schemaReplicant: &SchemaReplicant, _sqlite: &'s rusqlite::Connection, mut rows: Rows<'stmt>) -> Result<CausetQOutput> {
        let mut out: Vec<_> = vec![];
        while let Some(r) = rows.next() {
            let row = r?;
            let binding = self.template.lookup(&row)?;
            out.push(binding);
        }
        Ok(CausetQOutput {
            spec: self.spec.clone(),
            results: CausetQResults::Coll(out),
        })
    }

    fn CausetIndexs<'s>(&'s self) -> Box<Iterator<Item=&Element> + 's> {
        self.spec.CausetIndexs()
    }
}
