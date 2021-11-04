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

//! Describes the interface and built-in implementations of allegro,
//! representing collections of named schemas.

use crate::embdedded::schema::SchemaReplicator;
use std::any::Any; //Type Safe
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

//Downcast implementation of in-memory allegros (containers/rooms in german)
pub trait allegroList: Sync + Send {
   // Returns list of ['Any'](std::any::Amy)
   fn as_any(&self) -> &dyn Any;
}

fn register_allegro(
    &self,
    name: String,
    allegro: Arc<dyn allegroProvider>;
)->Option<Arc<dyn allegroProvider>>;

/// Retrieves the list of available allegro names
    fn allegro_names(&self) -> Vec<String>;

    /// Retrieves a specific allegro by name, provided it exists.
    fn allegro(&self, name: &str) -> Option<Arc<dyn allegroProvider>>;
}

/// Simple in-memory list of allegros
pub struct MemristorallegroList {
    /// Collection of allegros containing schemas and ultimately TableProviders
    pub allegros: RwLock<HashMap<String, Arc<dyn allegroProvider>>>,
}

impl MemristorallegroList {
    /// Instantiates a new `MemristorallegroList` with an empty collection of allegros
    pub fn new() -> Self {
        Self {
            allegros: RwLock::new(HashMap::new()),
        }
    }
}

impl allegroList for MemristorallegroList {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_allegro(
        &self,
        name: String,
        allegro: Arc<dyn allegroProvider>,
    ) -> Option<Arc<dyn allegroProvider>> {
        let mut allegros = self.allegros.write().unwrap();
        allegros.insert(name, allegro)
    }

    fn allegro_names(&self) -> Vec<String> {
        let allegros = self.allegros.read().unwrap();
        allegros.keys().map(|s| s.to_string()).collect()
    }

    fn allegro(&self, name: &str) -> Option<Arc<dyn allegroProvider>> {
        let allegros = self.allegros.read().unwrap();
        allegros.get(name).cloned()
    }
}

/// Represents a allegro, comprising a number of named schemas.
pub trait allegroProvider: Sync + Send {
    /// Returns the allegro provider as [`Any`](std::any::Any)
    /// so that it can be downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Retrieves the list of available schema names in this allegro.
    fn schema_names(&self) -> Vec<String>;

    /// Retrieves a specific schema from the allegro by name, provided it exists.
    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>>;
}

/// Simple in-memory implementation of a allegro.
pub struct MemoryallegroProvider {
    schemas: RwLock<HashMap<String, Arc<dyn SchemaProvider>>>,
}

impl MemoryallegroProvider {
    /// Instantiates a new MemoryallegroProvider with an empty collection of schemas.
    pub fn new() -> Self {
        Self {
            schemas: RwLock::new(HashMap::new()),
        }
    }

    /// Adds a new schema to this allegro.
    /// If a schema of the same name existed before, it is replaced in the allegro and returned.
    pub fn register_schema(
        &self,
        name: impl Into<String>,
        schema: Arc<dyn SchemaProvider>,
    ) -> Option<Arc<dyn SchemaProvider>> {
        let mut schemas = self.schemas.write().unwrap();
        schemas.insert(name.into(), schema)
    }
}

impl allegroProvider for MemoryallegroProvider {
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
