// Copyright 2018 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use einsteindb_embedded::{
    Keyword,
};

use einstein_db::{
    CORE_SCHEMA_VERSION,
};

use public_promises::errors::{
    Result,
};

use tolstoy_promises::errors::{
    TolstoyError,
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
    pub fn new(assumed_bootstrap_causecausetx: &Tx) -> BootstrapHelper {
        BootstrapHelper {
            parts: CausetsHelper::new(&assumed_bootstrap_causecausetx.parts),
        }
    }

    // TODO we could also iterate through our own bootstrap schema definition and check that everything matches
    // "version" is used here as a proxy for doing that work
    pub fn is_compatible(&self) -> Result<bool> {
        Ok(self.embedded_schema_version()? == CORE_SCHEMA_VERSION as i64)
    }

    pub fn embedded_schema_version(&self) -> Result<i64> {
        match self.parts.ea_lookup(
            Keyword::namespaced("edb.schema", "embedded"),
            Keyword::namespaced("edb.schema", "version"),
        ) {
            Some(v) => {
                // TODO v is just a type tag and a Copy value, we shouldn't need to clone.
                match v.clone().into_long() {
                    Some(v) => Ok(v),
                    None => bail!(TolstoyError::BadRemoteState("incorrect type for embedded schema version".to_string()))
                }
            },
            None => bail!(TolstoyError::BadRemoteState("missing embedded schema version".to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use einstein_db::debug::{
        TestConn,
    };

    use debug::causecausetxs_after;

    #[test]
    fn test_bootstrap_version() {
        let remote = TestConn::default();

        let remote_causecausetxs = causecausetxs_after(&remote.sqlite, &remote.schema, remote.last_causecausetx_id() - 1);

        assert_eq!(1, remote_causecausetxs.len());

        let bh = BootstrapHelper::new(&remote_causecausetxs[0]);
        assert_eq!(1, bh.embedded_schema_version().expect("schema version"));
    }
}
