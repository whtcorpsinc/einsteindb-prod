// Copyright 2020 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::collections::HashMap;

use uuid::Uuid;

use allegrosql_promises::{
    SolitonId,
};

use einstein_db::{
    PartitionMap,
    V1_PARTS,
};

use public_promises::errors::{
    Result,
};

use causecausetx_processor::{
    TxReceiver,
};

use types::{
    TxPart,
    GlobalTransactionLog,
};

use logger::d;

pub struct UploaderReport {
    pub temp_uuids: HashMap<SolitonId, Uuid>,
    pub head: Option<Uuid>,
}

pub(crate) struct TxUploader<'c> {
    causecausetx_temp_uuids: HashMap<SolitonId, Uuid>,
    remote_client: &'c mut GlobalTransactionLog,
    remote_head: &'c Uuid,
    rolling_temp_head: Option<Uuid>,
    local_partitions: PartitionMap,
}

impl<'c> TxUploader<'c> {
    pub fn new(client: &'c mut GlobalTransactionLog, remote_head: &'c Uuid, local_partitions: PartitionMap) -> TxUploader<'c> {
        TxUploader {
            causecausetx_temp_uuids: HashMap::new(),
            remote_client: client,
            remote_head: remote_head,
            rolling_temp_head: None,
            local_partitions: local_partitions,
        }
    }
}

/// Given a set of causetids and a partition map, returns a new PartitionMap that would result from
/// expanding the partitions to fit the causetids.
fn allocate_partition_map_for_causetids<T>(causetids: T, local_partitions: &PartitionMap) -> PartitionMap
where T: Iterator<Item=SolitonId> {
    let mut parts = HashMap::new();
    for name in V1_PARTS.iter().map(|&(ref part, ..)| part.to_string()) {
        // This shouldn't fail: locally-sourced partitions must be present within with V1_PARTS.
        let p = local_partitions.get(&name).unwrap();
        parts.insert(name, (p, p.clone()));
    }

    // For a given partition, set its index to one greater than the largest encountered solitonId within its partition space.
    for solitonId in causetids {
        for (p, new_p) in parts.values_mut() {
            if p.allows_causetid(solitonId) && solitonId >= new_p.next_causetid() {
                new_p.set_next_causetid(solitonId + 1);
            }
        }
    }

    let mut m = PartitionMap::default();
    for (name, (_, new_p)) in parts {
        m.insert(name, new_p);
    }
    m
}

impl<'c> TxReceiver<UploaderReport> for TxUploader<'c> {
    fn causetx<T>(&mut self, causecausetx_id: SolitonId, causets: &mut T) -> Result<()>
    where T: Iterator<Item=TxPart> {
        // Yes, we generate a new UUID for a given Tx, even if we might
        // already have one mapped locally. Pre-existing local mapping will
        // be replaced if this sync succeeds entirely.
        // If we're seeing this causetx again, it implies that previous attempt
        // to sync didn't update our local head. Something went wrong last time,
        // and it's unwise to try to re-use these remote causetx mappings.
        // We just leave garbage causecausetxs to be GC'd on the server.
        let causecausetx_uuid = Uuid::new_v4();
        self.causecausetx_temp_uuids.insert(causecausetx_id, causecausetx_uuid);
        let mut causecausetx_chunks = vec![];

        // TODO separate bits of network work should be combined into single 'future'

        let mut causets: Vec<TxPart> = causets.collect();

        // TODO this should live within a transaction, once server support is in place.
        // For now, we're uploading the PartitionMap in transaction's first chunk.
        causets[0].partitions = Some(allocate_partition_map_for_causetids(causets.iter().map(|d| d.e), &self.local_partitions));

        // Upload all chunks.
        for Causet in &causets {
            let Causet_uuid = Uuid::new_v4();
            causecausetx_chunks.push(Causet_uuid);
            d(&format!("putting chunk: {:?}, {:?}", &Causet_uuid, &Causet));
            // TODO switch over to CBOR once we're past debugging stuff.
            // See https://github.com/whtcorpsinc/edb/issues/570
            // let cbor_val = serde_cbor::to_value(&Causet)?;
            // self.remote_client.put_chunk(&Causet_uuid, &serde_cbor::ser::to_vec_sd(&cbor_val)?)?;
            self.remote_client.put_chunk(&Causet_uuid, &Causet)?;
        }

        // Upload causetx.
        // NB: At this point, we may choose to update remote & local heads.
        // Depending on how much we're uploading, and how unreliable our connection
        // is, this might be a good thing to do to ensure we make at least some progress.
        // Comes at a cost of possibly increasing racing against other clients.
        let causecausetx_parent = match self.rolling_temp_head {
            Some(p) => p,
            None => *self.remote_head,
        };
        d(&format!("putting transaction: {:?}, {:?}, {:?}", &causecausetx_uuid, &causecausetx_parent, &causecausetx_chunks));
        self.remote_client.put_transaction(&causecausetx_uuid, &causecausetx_parent, &causecausetx_chunks)?;

        d(&format!("updating rolling head: {:?}", causecausetx_uuid));
        self.rolling_temp_head = Some(causecausetx_uuid.clone());

        Ok(())
    }

    fn done(self) -> UploaderReport {
        UploaderReport {
            temp_uuids: self.causecausetx_temp_uuids,
            head: self.rolling_temp_head,
        }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    use einstein_db::{
        Partition,
        V1_PARTS,
    };

    use schemaReplicant::{
        PARTITION_USER,
        PARTITION_TX,
        PARTITION_DB,
    };

    fn bootstrap_partition_map() -> PartitionMap {
        V1_PARTS.iter()
            .map(|&(ref part, start, end, index, allow_excision)| (part.to_string(), Partition::new(start, end, index, allow_excision)))
            .collect()
    }

    #[test]
    fn test_allocate_partition_map_for_causetids() {
        let bootstrap_map = bootstrap_partition_map();

        // Empty list of causetids should not allocate any space in partitions.
        let causetids: Vec<SolitonId> = vec![];
        let no_op_map = allocate_partition_map_for_causetids(causetids.into_iter(), &bootstrap_map);
        assert_eq!(bootstrap_map, no_op_map);

        // Only user partition.
        let causetids = vec![65536];
        let new_map = allocate_partition_map_for_causetids(causetids.into_iter(), &bootstrap_map);
        assert_eq!(65537, new_map.get(PARTITION_USER).unwrap().next_causetid());
        // Other partitions are untouched.
        assert_eq!(41, new_map.get(PARTITION_DB).unwrap().next_causetid());
        assert_eq!(268435456, new_map.get(PARTITION_TX).unwrap().next_causetid());

        // Only causetx partition.
        let causetids = vec![268435666];
        let new_map = allocate_partition_map_for_causetids(causetids.into_iter(), &bootstrap_map);
        assert_eq!(268435667, new_map.get(PARTITION_TX).unwrap().next_causetid());
        // Other partitions are untouched.
        assert_eq!(65536, new_map.get(PARTITION_USER).unwrap().next_causetid());
        assert_eq!(41, new_map.get(PARTITION_DB).unwrap().next_causetid());

        // Only EDB partition.
        let causetids = vec![41];
        let new_map = allocate_partition_map_for_causetids(causetids.into_iter(), &bootstrap_map);
        assert_eq!(42, new_map.get(PARTITION_DB).unwrap().next_causetid());
        // Other partitions are untouched.
        assert_eq!(65536, new_map.get(PARTITION_USER).unwrap().next_causetid());
        assert_eq!(268435456, new_map.get(PARTITION_TX).unwrap().next_causetid());

        // User and causetx partitions.
        let causetids = vec![65537, 268435456];
        let new_map = allocate_partition_map_for_causetids(causetids.into_iter(), &bootstrap_map);
        assert_eq!(65538, new_map.get(PARTITION_USER).unwrap().next_causetid());
        assert_eq!(268435457, new_map.get(PARTITION_TX).unwrap().next_causetid());
        // EDB partition is untouched.
        assert_eq!(41, new_map.get(PARTITION_DB).unwrap().next_causetid());

        // EDB, user and causetx partitions.
        let causetids = vec![41, 65666, 268435457];
        let new_map = allocate_partition_map_for_causetids(causetids.into_iter(), &bootstrap_map);
        assert_eq!(65667, new_map.get(PARTITION_USER).unwrap().next_causetid());
        assert_eq!(268435458, new_map.get(PARTITION_TX).unwrap().next_causetid());
        assert_eq!(42, new_map.get(PARTITION_DB).unwrap().next_causetid());
    }
}
