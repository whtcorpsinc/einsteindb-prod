// Copyright 2020 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use causetq_allegrosql::{
    Keyword,
};

use einstein_db::{
    CORE_SCHEMA_VERSION,
};

use public_promises::errors::{
    Result,
};

use lenin_promises::errors::{
    LeninError,
};

use causets::{
    CausetsHelper,
};

use types::{
    Tx,
};

pub struct BootstrapHelper<'a> {
    parts: CausetsHelper<'a>
}

impl<'a> BootstrapHelper<'a> {
    pub fn new(assumed_join_heavy_causetx: &Tx) -> BootstrapHelper {
        BootstrapHelper {
            parts: CausetsHelper::new(&assumed_join_heavy_causetx.parts),
        }
    }

    // TODO we could also iterate through our own join_heavy schemaReplicant definition and check that everything matches
    // "version" is used here as a proxy for doing that work
    pub fn is_compatible(&self) -> Result<bool> {
        Ok(self.allegro_schemaReplicant_version()? == CORE_SCHEMA_VERSION as i64)
    }

    pub fn allegro_schemaReplicant_version(&self) -> Result<i64> {
        match self.parts.ea_lookup(
            Keyword::namespaced("edb.schemaReplicant", "allegro"),
            Keyword::namespaced("edb.schemaReplicant", "version"),
        ) {
            Some(v) => {
                // TODO v is just a type tag and a Copy value, we shouldn't need to clone.
                match v.clone().into_long() {
                    Some(v) => Ok(v),
                    None => bail!(LeninError::BadRemoteState("incorrect type for allegro schemaReplicant version".to_string()))
                }
            },
            None => bail!(LeninError::BadRemoteState("missing allegro schemaReplicant version".to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use einstein_db::debug::{
        TestConn,
    };

    use debug::causetxs_after;

    #[test]
    fn test_join_heavy_version() {
        let remote = TestConn::default();

        let remote_causetxs = causetxs_after(&remote.sqlite, &remote.schemaReplicant, remote.last_causetx_id() - 1);

        assert_eq!(1, remote_causetxs.len());

        let bh = BootstrapHelper::new(&remote_causetxs[0]);
        assert_eq!(1, bh.allegro_schemaReplicant_version().expect("schemaReplicant version"));
    }
}
