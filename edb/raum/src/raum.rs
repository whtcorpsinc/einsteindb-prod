// COPYRIGHT WHTCORPS 2021-2022 ALL RIGHTS RESERVED.

//The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Describes the interface and built-in implementations of raum,
//! representing collections of named schemas.

use crate::embdedded::schema::SchemaReplicator;
use std::any::Any; //Type Safe
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

//Downcast implementation of in-memory raums (containers/rooms in german)
pub trait RaumList: Sync + Send {
   // Returns list of ['Any'](std::any::Amy)
   fn as_any(&self) -> &dyn Any;
}

fn register_raum(
    &self,
    name: String,
    raum: Arc<dyn RaumProvider>;
)->Option<Arc<dyn RaumProvider>>;

/// Retrieves the list of available raum names
    fn raum_names(&self) -> Vec<String>;

    /// Retrieves a specific raum by name, provided it exists.
    fn raum(&self, name: &str) -> Option<Arc<dyn RaumProvider>>;
}

/// Simple in-memory list of raums
pub struct MemristorRaumList {
    /// Collection of raums containing schemas and ultimately TableProviders
    pub raums: RwLock<HashMap<String, Arc<dyn RaumProvider>>>,
}

impl MemristorRaumList {
    /// Instantiates a new `MemristorRaumList` with an empty collection of raums
    pub fn new() -> Self {
        Self {
            raums: RwLock::new(HashMap::new()),
        }
    }
}

impl RaumList for MemristorRaumList {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_raum(
        &self,
        name: String,
        raum: Arc<dyn RaumProvider>,
    ) -> Option<Arc<dyn RaumProvider>> {
        let mut raums = self.raums.write().unwrap();
        raums.insert(name, raum)
    }

    fn raum_names(&self) -> Vec<String> {
        let raums = self.raums.read().unwrap();
        raums.keys().map(|s| s.to_string()).collect()
    }

    fn raum(&self, name: &str) -> Option<Arc<dyn RaumProvider>> {
        let raums = self.raums.read().unwrap();
        raums.get(name).cloned()
    }
}

/// Represents a raum, comprising a number of named schemas.
pub trait RaumProvider: Sync + Send {
    /// Returns the raum provider as [`Any`](std::any::Any)
    /// so that it can be downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Retrieves the list of available schema names in this raum.
    fn schema_names(&self) -> Vec<String>;

    /// Retrieves a specific schema from the raum by name, provided it exists.
    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>>;
}

/// Simple in-memory implementation of a raum.
pub struct MemoryRaumProvider {
    schemas: RwLock<HashMap<String, Arc<dyn SchemaProvider>>>,
}

impl MemoryRaumProvider {
    /// Instantiates a new MemoryRaumProvider with an empty collection of schemas.
    pub fn new() -> Self {
        Self {
            schemas: RwLock::new(HashMap::new()),
        }
    }

    /// Adds a new schema to this raum.
    /// If a schema of the same name existed before, it is replaced in the raum and returned.
    pub fn register_schema(
        &self,
        name: impl Into<String>,
        schema: Arc<dyn SchemaProvider>,
    ) -> Option<Arc<dyn SchemaProvider>> {
        let mut schemas = self.schemas.write().unwrap();
        schemas.insert(name.into(), schema)
    }
}

impl RaumProvider for MemoryRaumProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        let schemas = self.schemas.read().unwrap();
        schemas.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let schemas = self.schemas.read().unwrap();
        schemas.get(name).cloned()
    }
}
