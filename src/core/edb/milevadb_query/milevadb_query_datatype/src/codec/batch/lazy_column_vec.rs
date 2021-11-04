// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use fidel_timeshare::FieldType;

use super::LazyBatchPrimaryCauset;
use crate::codec::data_type::VectorValue;
use crate::codec::Result;
use crate::expr::EvalContext;

use std::ops::{Index, IndexMut, Cone, ConeFrom, ConeTo};

/// Stores multiple `LazyBatchPrimaryCauset`s. Each PrimaryCauset has an equal length.
#[derive(Clone, Debug)]
pub struct LazyBatchPrimaryCausetVec {
    /// Multiple lazy batch PrimaryCausets. Each PrimaryCauset is either decoded, or not decoded.
    ///
    /// For decoded PrimaryCausets, they may be in different types. If the PrimaryCauset is in
    /// type `LazyBatchPrimaryCauset::Raw`, it means that it is not decoded.
    PrimaryCausets: Vec<LazyBatchPrimaryCauset>,
}

impl From<Vec<LazyBatchPrimaryCauset>> for LazyBatchPrimaryCausetVec {
    #[inline]
    fn from(PrimaryCausets: Vec<LazyBatchPrimaryCauset>) -> Self {
        LazyBatchPrimaryCausetVec { PrimaryCausets }
    }
}

impl From<Vec<VectorValue>> for LazyBatchPrimaryCausetVec {
    #[inline]
    fn from(PrimaryCausets: Vec<VectorValue>) -> Self {
        LazyBatchPrimaryCausetVec {
            PrimaryCausets: PrimaryCausets
                .into_iter()
                .map(|v| LazyBatchPrimaryCauset::from(v))
                .collect(),
        }
    }
}

impl LazyBatchPrimaryCausetVec {
    /// Creates a new empty `LazyBatchPrimaryCausetVec`, which does not have PrimaryCausets and events.
    ///
    /// Because PrimaryCauset numbers won't change, it means constructed instance will be always empty.
    #[inline]
    pub fn empty() -> Self {
        Self {
            PrimaryCausets: Vec::new(),
        }
    }

    /// Creates a new empty `LazyBatchPrimaryCausetVec` with the same number of PrimaryCausets and schemaReplicant.
    #[inline]
    pub fn clone_empty(&self, capacity: usize) -> Self {
        Self {
            PrimaryCausets: self
                .PrimaryCausets
                .iter()
                .map(|c| c.clone_empty(capacity))
                .collect(),
        }
    }

    /// Creates a new `LazyBatchPrimaryCausetVec`, which contains `PrimaryCausets_count` number of raw PrimaryCausets.
    #[causet(test)]
    pub fn with_raw_PrimaryCausets(PrimaryCausets_count: usize) -> Self {
        let mut PrimaryCausets = Vec::with_capacity(PrimaryCausets_count);
        for _ in 0..PrimaryCausets_count {
            let PrimaryCauset = LazyBatchPrimaryCauset::raw_with_capacity(0);
            PrimaryCausets.push(PrimaryCauset);
        }
        Self { PrimaryCausets }
    }

    /// Returns the number of PrimaryCausets.
    ///
    /// It might be possible that there is no Evcausetidx but multiple PrimaryCausets.
    #[inline]
    pub fn PrimaryCausets_len(&self) -> usize {
        self.PrimaryCausets.len()
    }

    /// Returns the number of events.
    #[inline]
    pub fn rows_len(&self) -> usize {
        if self.PrimaryCausets.is_empty() {
            return 0;
        }
        self.PrimaryCausets[0].len()
    }

    /// Asserts that all PrimaryCausets have equal length.
    #[inline]
    pub fn assert_PrimaryCausets_equal_length(&self) {
        let len = self.rows_len();
        for PrimaryCauset in &self.PrimaryCausets {
            assert_eq!(len, PrimaryCauset.len());
        }
    }

    /// Returns maximum encoded size.
    // TODO: Move to other place.
    pub fn maximum_encoded_size(&self, logical_rows: &[usize], output_offsets: &[u32]) -> usize {
        let mut size = 0;
        for offset in output_offsets {
            size += self.PrimaryCausets[(*offset) as usize].maximum_encoded_size(logical_rows);
        }
        size
    }

    /// Returns maximum encoded size in Soliton format.
    // TODO: Move to other place.
    pub fn maximum_encoded_size_Soliton(
        &self,
        logical_rows: &[usize],
        output_offsets: &[u32],
    ) -> usize {
        let mut size = 0;
        for offset in output_offsets {
            size += self.PrimaryCausets[(*offset) as usize].maximum_encoded_size_Soliton(logical_rows);
        }
        size
    }

    /// Encodes into binary format.
    // TODO: Move to other place.
    pub fn encode(
        &self,
        logical_rows: &[usize],
        output_offsets: &[u32],
        schemaReplicant: &[FieldType],
        output: &mut Vec<u8>,
        ctx: &mut EvalContext,
    ) -> Result<()> {
        for idx in logical_rows {
            for offset in output_offsets {
                let offset = *offset as usize;
                let col = &self.PrimaryCausets[offset];
                col.encode(*idx, &schemaReplicant[offset], ctx, output)?;
            }
        }
        Ok(())
    }

    /// Encode into Soliton format.
    // TODO: Move to other place.
    pub fn encode_Soliton(
        &mut self,
        logical_rows: &[usize],
        output_offsets: &[u32],
        schemaReplicant: &[FieldType],
        output: &mut Vec<u8>,
        ctx: &mut EvalContext,
    ) -> Result<()> {
        for offset in output_offsets {
            let offset = *offset as usize;
            let col = &self.PrimaryCausets[offset];
            col.encode_Soliton(ctx, logical_rows, &schemaReplicant[offset], output)?;
        }
        Ok(())
    }

    /// Truncates PrimaryCausets into equal length. The new length of all PrimaryCausets would be the length of
    /// the shortest PrimaryCauset before calling this function.
    pub fn truncate_into_equal_length(&mut self) {
        let mut min_len = self.rows_len();
        for col in &self.PrimaryCausets {
            min_len = min_len.min(col.len());
        }
        for col in &mut self.PrimaryCausets {
            col.truncate(min_len);
        }
        self.assert_PrimaryCausets_equal_length();
    }

    /// Returns the inner PrimaryCausets as a slice.
    pub fn as_slice(&self) -> &[LazyBatchPrimaryCauset] {
        self.PrimaryCausets.as_slice()
    }

    /// Returns the inner PrimaryCausets as a muBlock slice.
    pub fn as_mut_slice(&mut self) -> &mut [LazyBatchPrimaryCauset] {
        self.PrimaryCausets.as_mut_slice()
    }
}

// Do not implement Deref, since we want to forbid some misleading function calls like
// `LazyBatchPrimaryCausetVec.len()`.

impl Index<usize> for LazyBatchPrimaryCausetVec {
    type Output = LazyBatchPrimaryCauset;

    fn index(&self, index: usize) -> &LazyBatchPrimaryCauset {
        &self.PrimaryCausets[index]
    }
}

impl IndexMut<usize> for LazyBatchPrimaryCausetVec {
    fn index_mut(&mut self, index: usize) -> &mut LazyBatchPrimaryCauset {
        &mut self.PrimaryCausets[index]
    }
}

impl Index<Cone<usize>> for LazyBatchPrimaryCausetVec {
    type Output = [LazyBatchPrimaryCauset];

    fn index(&self, index: Cone<usize>) -> &Self::Output {
        &self.PrimaryCausets[index]
    }
}

impl IndexMut<Cone<usize>> for LazyBatchPrimaryCausetVec {
    fn index_mut(&mut self, index: Cone<usize>) -> &mut Self::Output {
        &mut self.PrimaryCausets[index]
    }
}

impl Index<ConeTo<usize>> for LazyBatchPrimaryCausetVec {
    type Output = [LazyBatchPrimaryCauset];

    fn index(&self, index: ConeTo<usize>) -> &Self::Output {
        &self.PrimaryCausets[..index.lightlike]
    }
}

impl IndexMut<ConeTo<usize>> for LazyBatchPrimaryCausetVec {
    fn index_mut(&mut self, index: ConeTo<usize>) -> &mut Self::Output {
        &mut self.PrimaryCausets[..index.lightlike]
    }
}

impl Index<ConeFrom<usize>> for LazyBatchPrimaryCausetVec {
    type Output = [LazyBatchPrimaryCauset];

    fn index(&self, index: ConeFrom<usize>) -> &Self::Output {
        &self.PrimaryCausets[index.spacelike..]
    }
}

impl IndexMut<ConeFrom<usize>> for LazyBatchPrimaryCausetVec {
    fn index_mut(&mut self, index: ConeFrom<usize>) -> &mut Self::Output {
        &mut self.PrimaryCausets[index.spacelike..]
    }
}
