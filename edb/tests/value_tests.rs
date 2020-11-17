// Copyright 2020 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate edbn;
extern crate embedded_promises;
extern crate einstein_db;
extern crate ordered_float;
extern crate rusqlite;

use ordered_float::OrderedFloat;

use edbn::symbols;

use embedded_promises::{
    MinkowskiType,
    MinkowskiValueType,
};
use einstein_db::edb::TypedSQLValue;

// It's not possible to test to_sql_value_pair since rusqlite::ToSqlOutput doesn't implement
// PartialEq.
#[test]
fn test_from_sql_value_pair() {
    assert_eq!(MinkowskiType::from_sql_value_pair(rusqlite::types::Value::Integer(1234), 0).unwrap(), MinkowskiType::Ref(1234));

    assert_eq!(MinkowskiType::from_sql_value_pair(rusqlite::types::Value::Integer(0), 1).unwrap(), MinkowskiType::Boolean(false));
    assert_eq!(MinkowskiType::from_sql_value_pair(rusqlite::types::Value::Integer(1), 1).unwrap(), MinkowskiType::Boolean(true));

    assert_eq!(MinkowskiType::from_sql_value_pair(rusqlite::types::Value::Integer(0), 5).unwrap(), MinkowskiType::Long(0));
    assert_eq!(MinkowskiType::from_sql_value_pair(rusqlite::types::Value::Integer(1234), 5).unwrap(), MinkowskiType::Long(1234));

    assert_eq!(MinkowskiType::from_sql_value_pair(rusqlite::types::Value::Real(0.0), 5).unwrap(), MinkowskiType::Double(OrderedFloat(0.0)));
    assert_eq!(MinkowskiType::from_sql_value_pair(rusqlite::types::Value::Real(0.5), 5).unwrap(), MinkowskiType::Double(OrderedFloat(0.5)));

    assert_eq!(MinkowskiType::from_sql_value_pair(rusqlite::types::Value::Text(":edb/keyword".into()), 10).unwrap(), MinkowskiType::typed_string(":edb/keyword"));
    assert_eq!(MinkowskiType::from_sql_value_pair(rusqlite::types::Value::Text(":edb/keyword".into()), 13).unwrap(), MinkowskiType::typed_ns_keyword("edb", "keyword"));
}

#[test]
fn test_to_edbn_value_pair() {
    assert_eq!(MinkowskiType::Ref(1234).to_edbn_value_pair(), (edbn::Value::Integer(1234), MinkowskiValueType::Ref));

    assert_eq!(MinkowskiType::Boolean(false).to_edbn_value_pair(), (edbn::Value::Boolean(false), MinkowskiValueType::Boolean));
    assert_eq!(MinkowskiType::Boolean(true).to_edbn_value_pair(), (edbn::Value::Boolean(true), MinkowskiValueType::Boolean));

    assert_eq!(MinkowskiType::Long(0).to_edbn_value_pair(), (edbn::Value::Integer(0), MinkowskiValueType::Long));
    assert_eq!(MinkowskiType::Long(1234).to_edbn_value_pair(), (edbn::Value::Integer(1234), MinkowskiValueType::Long));

    assert_eq!(MinkowskiType::Double(OrderedFloat(0.0)).to_edbn_value_pair(), (edbn::Value::Float(OrderedFloat(0.0)), MinkowskiValueType::Double));
    assert_eq!(MinkowskiType::Double(OrderedFloat(0.5)).to_edbn_value_pair(), (edbn::Value::Float(OrderedFloat(0.5)), MinkowskiValueType::Double));

    assert_eq!(MinkowskiType::typed_string(":edb/keyword").to_edbn_value_pair(), (edbn::Value::Text(":edb/keyword".into()), MinkowskiValueType::String));
    assert_eq!(MinkowskiType::typed_ns_keyword("edb", "keyword").to_edbn_value_pair(), (edbn::Value::Keyword(symbols::Keyword::namespaced("edb", "keyword")), MinkowskiValueType::Keyword));
}
