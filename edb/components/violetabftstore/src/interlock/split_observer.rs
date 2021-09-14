// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

use super::{AdminSemaphore, Interlock, SemaphoreContext, Result as CopResult};
use violetabftstore::interlock::::codec::bytes::{self, encode_bytes};

use crate::store::util;
use ekvproto::meta_timeshare::Brane;
use ekvproto::violetabft_cmd_timeshare::{AdminCmdType, AdminRequest, SplitRequest};
use std::result::Result as StdResult;

/// `SplitSemaphore` adjusts the split key so that it won't separate
/// the data of a Evcausetidx into two brane. It adjusts the key according
/// to the key format of `MilevaDB`.
#[derive(Clone)]
pub struct SplitSemaphore;

type Result<T> = StdResult<T, String>;

impl SplitSemaphore {
    fn adjust_key(&self, brane: &Brane, key: Vec<u8>) -> Result<Vec<u8>> {
        if key.is_empty() {
            return Err("key is empty".to_owned());
        }

        let key = match bytes::decode_bytes(&mut key.as_slice(), false) {
            Ok(x) => x,
            // It's a raw key, skip it.
            Err(_) => return Ok(key),
        };

        let key = encode_bytes(&key);
        match util::check_key_in_brane_exclusive(&key, brane) {
            Ok(()) => Ok(key),
            Err(_) => Err(format!(
                "key {} should be in ({}, {})",
                hex::encode_upper(&key),
                hex::encode_upper(brane.get_spacelike_key()),
                hex::encode_upper(brane.get_lightlike_key()),
            )),
        }
    }

    fn on_split(
        &self,
        ctx: &mut SemaphoreContext<'_>,
        splits: &mut Vec<SplitRequest>,
    ) -> Result<()> {
        let (mut i, mut j) = (0, 0);
        let mut last_valid_key: Option<Vec<u8>> = None;
        let brane_id = ctx.brane().get_id();
        while i < splits.len() {
            let k = i;
            i += 1;
            {
                let split = &mut splits[k];
                let key = split.take_split_key();
                match self.adjust_key(ctx.brane(), key) {
                    Ok(key) => {
                        if last_valid_key.as_ref().map_or(false, |k| *k >= key) {
                            warn!(
                                "key is not larger than previous, skip.";
                                "brane_id" => brane_id,
                                "key" => log_wrappers::Key(&key),
                                "previous" => log_wrappers::Key(last_valid_key.as_ref().unwrap()),
                                "index" => k,
                            );
                            continue;
                        }
                        last_valid_key = Some(key.clone());
                        split.set_split_key(key)
                    }
                    Err(e) => {
                        warn!(
                            "invalid key, skip";
                            "brane_id" => brane_id,
                            "index" => k,
                            "err" => ?e,
                        );
                        continue;
                    }
                }
            }
            if k != j {
                splits.swap(k, j);
            }
            j += 1;
        }
        if j == 0 {
            return Err("no valid key found for split.".to_owned());
        }
        splits.truncate(j);
        Ok(())
    }
}

impl Interlock for SplitSemaphore {}

impl AdminSemaphore for SplitSemaphore {
    fn pre_propose_admin(
        &self,
        ctx: &mut SemaphoreContext<'_>,
        req: &mut AdminRequest,
    ) -> CopResult<()> {
        match req.get_cmd_type() {
            AdminCmdType::Split => {
                if !req.has_split() {
                    box_try!(Err(
                        "cmd_type is Split but it doesn't have split request, message maybe \
                         corrupted!"
                            .to_owned()
                    ));
                }
                let mut request = vec![req.take_split()];
                if let Err(e) = self.on_split(ctx, &mut request) {
                    error!(
                        "failed to handle split req";
                        "brane_id" => ctx.brane().get_id(),
                        "err" => ?e,
                    );
                    return Err(box_err!(e));
                }
                // self.on_split() makes sure request is not empty, or it will return error.
                // so directly unwrap here.
                req.set_split(request.pop().unwrap());
            }
            AdminCmdType::BatchSplit => {
                if !req.has_splits() {
                    return Err(box_err!(
                        "cmd_type is BatchSplit but it doesn't have splits request, message maybe \
                         corrupted!"
                            .to_owned()
                    ));
                }
                let mut requests = req.mut_splits().take_requests().into();
                if let Err(e) = self.on_split(ctx, &mut requests) {
                    error!(
                        "failed to handle split req";
                        "brane_id" => ctx.brane().get_id(),
                        "err" => ?e,
                    );
                    return Err(box_err!(e));
                }
                req.mut_splits().set_requests(requests.into());
            }
            _ => return Ok(()),
        }
        Ok(())
    }
}

#[causet(test)]
mod tests {
    use super::*;
    use crate::interlock::AdminSemaphore;
    use crate::interlock::SemaphoreContext;
    use byteorder::{BigEndian, WriteBytesExt};
    use ekvproto::meta_timeshare::Brane;
    use ekvproto::violetabft_cmd_timeshare::{AdminCmdType, AdminRequest, SplitRequest};
    use milevadb_query_datatype::codec::{datum, Block, Datum};
    use milevadb_query_datatype::expr::EvalContext;
    use violetabftstore::interlock::::codec::bytes::encode_bytes;

    fn new_split_request(key: &[u8]) -> AdminRequest {
        let mut req = AdminRequest::default();
        req.set_cmd_type(AdminCmdType::Split);
        let mut split_req = SplitRequest::default();
        split_req.set_split_key(key.to_vec());
        req.set_split(split_req);
        req
    }

    fn new_batch_split_request(tuplespaceInstanton: Vec<Vec<u8>>) -> AdminRequest {
        let mut req = AdminRequest::default();
        req.set_cmd_type(AdminCmdType::BatchSplit);
        for key in tuplespaceInstanton {
            let mut split_req = SplitRequest::default();
            split_req.set_split_key(key);
            req.mut_splits().mut_requests().push(split_req);
        }
        req
    }

    fn new_row_key(Block_id: i64, row_id: i64, version_id: u64) -> Vec<u8> {
        let mut key = Block::encode_row_key(Block_id, row_id);
        key = encode_bytes(&key);
        key.write_u64::<BigEndian>(version_id).unwrap();
        key
    }

    fn new_index_key(Block_id: i64, idx_id: i64, datums: &[Datum], version_id: u64) -> Vec<u8> {
        let mut key = Block::encode_index_seek_key(
            Block_id,
            idx_id,
            &datum::encode_key(&mut EvalContext::default(), datums).unwrap(),
        );
        key = encode_bytes(&key);
        key.write_u64::<BigEndian>(version_id).unwrap();
        key
    }

    #[test]
    fn test_forget_encode() {
        let brane_spacelike_key = new_row_key(256, 1, 0);
        let key = new_row_key(256, 2, 0);
        let mut r = Brane::default();
        r.set_id(10);
        r.set_spacelike_key(brane_spacelike_key);

        let mut ctx = SemaphoreContext::new(&r);
        let semaphore = SplitSemaphore;

        let mut req = new_batch_split_request(vec![key]);
        semaphore.pre_propose_admin(&mut ctx, &mut req).unwrap();
        let expect_key = new_row_key(256, 2, 0);
        let len = expect_key.len();
        assert_eq!(req.get_splits().get_requests().len(), 1);
        assert_eq!(
            req.get_splits().get_requests()[0].get_split_key(),
            &expect_key[..len - 8]
        );
    }

    #[test]
    fn test_split() {
        let mut brane = Brane::default();
        let spacelike_key = new_row_key(1, 1, 1);
        brane.set_spacelike_key(spacelike_key.clone());
        let mut ctx = SemaphoreContext::new(&brane);
        let mut req = AdminRequest::default();

        let semaphore = SplitSemaphore;

        let resp = semaphore.pre_propose_admin(&mut ctx, &mut req);
        // since no split is defined, actual interlock won't be invoke.
        assert!(resp.is_ok());
        assert!(!req.has_split(), "only split req should be handle.");

        req = new_split_request(b"test");
        // For compatible reason, split should supported too.
        assert!(semaphore.pre_propose_admin(&mut ctx, &mut req).is_ok());

        // Empty key should be skipped.
        let mut split_tuplespaceInstanton = vec![vec![]];
        // Start key should be skipped.
        split_tuplespaceInstanton.push(spacelike_key);

        req = new_batch_split_request(split_tuplespaceInstanton.clone());
        // Although invalid tuplespaceInstanton should be skipped, but if all tuplespaceInstanton are
        // invalid, errors should be reported.
        assert!(semaphore.pre_propose_admin(&mut ctx, &mut req).is_err());

        let mut key = new_row_key(1, 2, 0);
        let mut expected_key = key[..key.len() - 8].to_vec();
        split_tuplespaceInstanton.push(key);
        let mut expected_tuplespaceInstanton = vec![expected_key.clone()];

        // Extra version of same key will be ignored.
        key = new_row_key(1, 2, 1);
        split_tuplespaceInstanton.push(key);

        key = new_index_key(2, 2, &[Datum::I64(1), Datum::Bytes(b"brgege".to_vec())], 0);
        expected_key = key[..key.len() - 8].to_vec();
        split_tuplespaceInstanton.push(key);
        expected_tuplespaceInstanton.push(expected_key.clone());

        // Extra version of same key will be ignored.
        key = new_index_key(2, 2, &[Datum::I64(1), Datum::Bytes(b"brgege".to_vec())], 5);
        split_tuplespaceInstanton.push(key);

        expected_key =
            encode_bytes(b"t\x80\x00\x00\x00\x00\x00\x00\xea_r\x80\x00\x00\x00\x00\x05\x82\x7f");
        key = expected_key.clone();
        key.extlightlike_from_slice(b"\x80\x00\x00\x00\x00\x00\x00\xd3");
        split_tuplespaceInstanton.push(key);
        expected_tuplespaceInstanton.push(expected_key.clone());

        // Split at Block prefix.
        key = encode_bytes(b"t\x80\x00\x00\x00\x00\x00\x00\xee");
        split_tuplespaceInstanton.push(key.clone());
        expected_tuplespaceInstanton.push(key);

        // Raw key should be preserved.
        split_tuplespaceInstanton.push(b"xyz".to_vec());
        expected_tuplespaceInstanton.push(b"xyz".to_vec());

        key = encode_bytes(b"xyz:1");
        key.write_u64::<BigEndian>(0).unwrap();
        split_tuplespaceInstanton.push(key);
        expected_key = encode_bytes(b"xyz:1");
        expected_tuplespaceInstanton.push(expected_key);

        req = new_batch_split_request(split_tuplespaceInstanton);
        req.mut_splits().set_right_derive(true);
        semaphore.pre_propose_admin(&mut ctx, &mut req).unwrap();
        assert!(req.get_splits().get_right_derive());
        assert_eq!(req.get_splits().get_requests().len(), expected_tuplespaceInstanton.len());
        for (i, (req, expected_key)) in req
            .get_splits()
            .get_requests()
            .iter()
            .zip(expected_tuplespaceInstanton)
            .enumerate()
        {
            assert_eq!(
                req.get_split_key(),
                expected_key.as_slice(),
                "case {}",
                i + 1
            );
        }
    }
}
