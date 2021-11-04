//! Module contains a representation of chunk metadata
use std::{convert::TryFrom, num::NonZeroU32, sync::Arc};
use snafu::Snafu;
use std::{borrow::Cow, ops::RangeInclusive};
use bytes::Bytes;
use snafu::{ResultExt, Snafu};
use time::Time;
use uuid::Uuid;

use crate::partition_addr::PartitionAddr;

// Address of the chunk within the catalog
#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Ord)]
/**1. It has a constructor that takes a partition address and a chunk id.
2. It implements the Display trait so that it can be printed.**/

pub struct ChunkAddr {
    /// Database name
    pub db_name: Arc<str>,

    /// What table does the chunk belong to?
    pub table_name: Arc<str>,

    /// What partition does the chunk belong to?
    pub partition_key: Arc<str>,

    /// The ID of the chunk
    pub chunk_id: ChunkId,
}

impl ChunkAddr {
    pub fn new(partition: &PartitionAddr, chunk_id: ChunkId) -> Self {
        Self {
            db_name: Arc::clone(&partition.db_name),
            table_name: Arc::clone(&partition.table_name),
            partition_key: Arc::clone(&partition.partition_key),
            chunk_id,
        }
    }

    pub fn into_partition(self) -> PartitionAddr {
        PartitionAddr {
            db_name: self.db_name,
            table_name: self.table_name,
            partition_key: self.partition_key,
        }
    }
}

impl std::fmt::Display for ChunkAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Chunk('{}':'{}':'{}':{})",
            self.db_name,
            self.table_name,
            self.partition_key,
            self.chunk_id.get()
        )
    }
}
/*
The ChunkId type is a wrapper around a NonZeroU32. This is to ensure that the chunk id is never zero.

The ChunkMetadata type is a wrapper around a ChunkMetadataInner. This is to ensure that the chunk metadata is never
zero.

The ChunkMetadataInner type is a wrapper around a Bytes. This is to ensure that the chunk metadata is never zero.
*/



#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub enum ChunkStorage {
    
    OpenMutableBuffer,


    ClosedMutableBuffer,


    ReadBuffer,

 
    ReadBufferAndObjectStore,

    
    ObjectStoreOnly,
}

impl ChunkStorage {
   
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::OpenMutableBuffer => "OpenMutableBuffer",
            Self::ClosedMutableBuffer => "ClosedMutableBuffer",
            Self::ReadBuffer => "ReadBuffer",
            Self::ReadBufferAndObjectStore => "ReadBufferAndObjectStore",
            Self::ObjectStoreOnly => "ObjectStoreOnly",
        }
    }
}


/*
 1. find the node with the highest hash value
 2. find the node with the highest hash value that is lower than the hash value of the key
 3. if no node is found, return the node with the lowest hash value
*/

/// that is closest to a given hash value.
#[derive(Debug, Eq, PartialEq, Clone, Default)]
pub struct ConsistentHasher<T>
where
    T: Copy + Hash,
{
    ring: Vec<(u64, T)>,
}

impl<T> ConsistentHasher<T>
where
    T: Copy + Hash,
{
    pub fn new(nodes: &[T]) -> Self {
        let mut ring: Vec<_> = nodes.iter().map(|node| (Self::hash(node), *node)).collect();
        ring.sort_by_key(|(hash, _)| *hash);
        Self { ring }
    }

    pub fn find<H: Hash>(&self, point: H) -> Option<T> {
        let point_hash = Self::hash(point);
        self.ring
            .iter()
            .find(|(node_hash, _)| node_hash > &point_hash)
            .or_else(|| self.ring.first())
            .map(|(_, node)| *node)
    }

    pub fn is_empty(&self) -> bool {
        self.ring.is_empty()
    }

    pub fn len(&self) -> usize {
        self.ring.len()
    }

    fn hash<H: Hash>(h: H) -> u64 {
        let mut hasher = DefaultHasher::new();
        h.hash(&mut hasher);
        hasher.finish()
    }
}


impl<T> From<ConsistentHasher<T>> for Vec<T>
where
    T: Copy + Hash,
{
    fn from(hasher: ConsistentHasher<T>) -> Self {
        hasher.ring.into_iter().map(|(_, node)| node).collect()
    }
}

impl<T> From<Vec<T>> for ConsistentHasher<T>
where
    T: Copy + Hash,
{
    fn from(vec: Vec<T>) -> Self {
        Self::new(&vec)
    }
}
/*
Here's what the above class is doing:
1. The constructor takes a list of nodes and creates a hash ring.
2. The find method takes a point and returns the node closest to that point.
3. The is_empty method returns true if the ring is empty.
4. The len method returns the number of nodes in the ring.
5. The from method creates a ConsistentHasher from a vector of nodes.
6. The into method creates a vector of nodes from a ConsistentHasher.

The from method creates a ConsistentHasher from a vector of nodes.
The into method creates a vector of nodes from a ConsistentHasher.
""
*/
