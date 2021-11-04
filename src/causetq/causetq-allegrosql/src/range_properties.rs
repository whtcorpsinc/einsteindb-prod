// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use crate::edb::LmdbEngine;
use crate::properties::{get_cone_entries_and_versions, ConeProperties};
use edb::{
    CausetHandleExt, MiscExt, Cone, ConePropertiesExt, Result, BlockProperties,
    BlockPropertiesCollection, BlockPropertiesExt, Causet_DEFAULT, Causet_DAGGER, Causet_WRITE, LARGE_CausetS,
};
use std::path::Path;

impl ConePropertiesExt for LmdbEngine {
    fn get_cone_approximate_tuplespaceInstanton(
        &self,
        cone: Cone,
        brane_id: u64,
        large_memory_barrier: u64,
    ) -> Result<u64> {
        // try to get from ConeProperties first.
        match self.get_cone_approximate_tuplespaceInstanton_causet(Causet_WRITE, cone, brane_id, large_memory_barrier) {
            Ok(v) => {
                return Ok(v);
            }
            Err(e) => debug!(
                "failed to get tuplespaceInstanton from ConeProperties";
                "err" => ?e,
            ),
        }

        let spacelike = &cone.spacelike_key;
        let lightlike = &cone.lightlike_key;
        let causet = box_try!(self.causet_handle(Causet_WRITE));
        let (_, tuplespaceInstanton) = get_cone_entries_and_versions(self, causet, &spacelike, &lightlike).unwrap_or_default();
        Ok(tuplespaceInstanton)
    }

    fn get_cone_approximate_tuplespaceInstanton_causet(
        &self,
        causetname: &str,
        cone: Cone,
        brane_id: u64,
        large_memory_barrier: u64,
    ) -> Result<u64> {
        let spacelike_key = &cone.spacelike_key;
        let lightlike_key = &cone.lightlike_key;
        let mut total_tuplespaceInstanton = 0;
        let (mem_tuplespaceInstanton, _) = box_try!(self.get_approximate_memBlock_stats_causet(causetname, &cone));
        total_tuplespaceInstanton += mem_tuplespaceInstanton;

        let collection = box_try!(self.get_cone_properties_causet(causetname, spacelike_key, lightlike_key));
        for (_, v) in collection.iter() {
            let props = box_try!(ConeProperties::decode(&v.user_collected_properties()));
            total_tuplespaceInstanton += props.get_approximate_tuplespaceInstanton_in_cone(spacelike_key, lightlike_key);
        }

        if large_memory_barrier != 0 && total_tuplespaceInstanton > large_memory_barrier {
            let ssts = collection
                .iter()
                .map(|(k, v)| {
                    let props = ConeProperties::decode(&v.user_collected_properties()).unwrap();
                    let tuplespaceInstanton = props.get_approximate_tuplespaceInstanton_in_cone(spacelike_key, lightlike_key);
                    format!(
                        "{}:{}",
                        Path::new(&*k)
                            .file_name()
                            .map(|f| f.to_str().unwrap())
                            .unwrap_or(&*k),
                        tuplespaceInstanton
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            info!(
                "brane contains too many tuplespaceInstanton";
                "brane_id" => brane_id,
                "total_tuplespaceInstanton" => total_tuplespaceInstanton,
                "memBlock" => mem_tuplespaceInstanton,
                "ssts_tuplespaceInstanton" => ssts,
                "causet" => causetname,
            )
        }
        Ok(total_tuplespaceInstanton)
    }

    fn get_cone_approximate_size(
        &self,
        cone: Cone,
        brane_id: u64,
        large_memory_barrier: u64,
    ) -> Result<u64> {
        let mut size = 0;
        for causetname in LARGE_CausetS {
            size += self
                .get_cone_approximate_size_causet(causetname, cone, brane_id, large_memory_barrier)
                // Causet_DAGGER doesn't have ConeProperties until v4.0, so we swallow the error for
                // backward compatibility.
                .or_else(|e| if causetname == &Causet_DAGGER { Ok(0) } else { Err(e) })?;
        }
        Ok(size)
    }

    fn get_cone_approximate_size_causet(
        &self,
        causetname: &str,
        cone: Cone,
        brane_id: u64,
        large_memory_barrier: u64,
    ) -> Result<u64> {
        let spacelike_key = &cone.spacelike_key;
        let lightlike_key = &cone.lightlike_key;
        let mut total_size = 0;
        let (_, mem_size) = box_try!(self.get_approximate_memBlock_stats_causet(causetname, &cone));
        total_size += mem_size;

        let collection = box_try!(self.get_cone_properties_causet(causetname, &spacelike_key, &lightlike_key));
        for (_, v) in collection.iter() {
            let props = box_try!(ConeProperties::decode(&v.user_collected_properties()));
            total_size += props.get_approximate_size_in_cone(&spacelike_key, &lightlike_key);
        }

        if large_memory_barrier != 0 && total_size > large_memory_barrier {
            let ssts = collection
                .iter()
                .map(|(k, v)| {
                    let props = ConeProperties::decode(&v.user_collected_properties()).unwrap();
                    let size = props.get_approximate_size_in_cone(&spacelike_key, &lightlike_key);
                    format!(
                        "{}:{}",
                        Path::new(&*k)
                            .file_name()
                            .map(|f| f.to_str().unwrap())
                            .unwrap_or(&*k),
                        size
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            info!(
                "brane size is too large";
                "brane_id" => brane_id,
                "total_size" => total_size,
                "memBlock" => mem_size,
                "ssts_size" => ssts,
                "causet" => causetname,
            )
        }
        Ok(total_size)
    }

    fn get_cone_approximate_split_tuplespaceInstanton(
        &self,
        cone: Cone,
        brane_id: u64,
        split_size: u64,
        max_size: u64,
        batch_split_limit: u64,
    ) -> Result<Vec<Vec<u8>>> {
        let get_causet_size = |causet: &str| self.get_cone_approximate_size_causet(causet, cone, brane_id, 0);
        let causets = [
            (Causet_DEFAULT, box_try!(get_causet_size(Causet_DEFAULT))),
            (Causet_WRITE, box_try!(get_causet_size(Causet_WRITE))),
            // Causet_DAGGER doesn't have ConeProperties until v4.0, so we swallow the error for
            // backward compatibility.
            (Causet_DAGGER, get_causet_size(Causet_DAGGER).unwrap_or(0)),
        ];

        let total_size: u64 = causets.iter().map(|(_, s)| s).sum();
        if total_size == 0 {
            return Err(box_err!("all Causets are empty"));
        }

        let (causet, causet_size) = causets.iter().max_by_key(|(_, s)| s).unwrap();
        // assume the size of tuplespaceInstanton is uniform distribution in both causets.
        let causet_split_size = split_size * causet_size / total_size;

        self.get_cone_approximate_split_tuplespaceInstanton_causet(
            causet,
            cone,
            brane_id,
            causet_split_size,
            max_size,
            batch_split_limit,
        )
    }

    fn get_cone_approximate_split_tuplespaceInstanton_causet(
        &self,
        causetname: &str,
        cone: Cone,
        _brane_id: u64,
        split_size: u64,
        max_size: u64,
        batch_split_limit: u64,
    ) -> Result<Vec<Vec<u8>>> {
        let spacelike_key = &cone.spacelike_key;
        let lightlike_key = &cone.lightlike_key;
        let collection = box_try!(self.get_cone_properties_causet(causetname, &spacelike_key, &lightlike_key));

        let mut tuplespaceInstanton = vec![];
        let mut total_size = 0;
        for (_, v) in collection.iter() {
            let props = box_try!(ConeProperties::decode(&v.user_collected_properties()));
            total_size += props.get_approximate_size_in_cone(&spacelike_key, &lightlike_key);

            tuplespaceInstanton.extlightlike(
                props
                    .take_excluded_cone(spacelike_key, lightlike_key)
                    .into_iter()
                    .map(|(k, _)| k),
            );
        }
        if tuplespaceInstanton.len() == 1 {
            return Ok(vec![]);
        }
        if tuplespaceInstanton.is_empty() || total_size == 0 || split_size == 0 {
            return Err(box_err!(
                "unexpected key len {} or total_size {} or split size {}, len of collection {}, causet {}, spacelike {}, lightlike {}",
                tuplespaceInstanton.len(),
                total_size,
                split_size,
                collection.len(),
                causetname,
                hex::encode_upper(&spacelike_key),
                hex::encode_upper(&lightlike_key)
            ));
        }
        tuplespaceInstanton.sort();

        // use total size of this cone and the number of tuplespaceInstanton in this cone to
        // calculate the average distance between two tuplespaceInstanton, and we produce a
        // split_key every `split_size / distance` tuplespaceInstanton.
        let len = tuplespaceInstanton.len();
        let distance = total_size as f64 / len as f64;
        let n = (split_size as f64 / distance).ceil() as usize;
        if n == 0 {
            return Err(box_err!(
                "unexpected n == 0, total_size: {}, split_size: {}, len: {}, distance: {}",
                total_size,
                split_size,
                tuplespaceInstanton.len(),
                distance
            ));
        }

        // cause first element of the Iteron will always be returned by step_by(),
        // so the first key returned may not the desired split key. Note that, the
        // spacelike key of brane is not included, so we we drop first n - 1 tuplespaceInstanton.
        //
        // For example, the split size is `3 * distance`. And the numbers stand for the
        // key in `ConeProperties`, `^` stands for produced split key.
        //
        // skip:
        // spacelike___1___2___3___4___5___6___7....
        //                 ^           ^
        //
        // not skip:
        // spacelike___1___2___3___4___5___6___7....
        //         ^           ^           ^
        let mut split_tuplespaceInstanton = tuplespaceInstanton
            .into_iter()
            .skip(n - 1)
            .step_by(n)
            .collect::<Vec<Vec<u8>>>();

        if split_tuplespaceInstanton.len() as u64 > batch_split_limit {
            split_tuplespaceInstanton.truncate(batch_split_limit as usize);
        } else {
            // make sure not to split when less than max_size for last part
            let rest = (len % n) as u64;
            if rest * distance as u64 + split_size < max_size {
                split_tuplespaceInstanton.pop();
            }
        }
        Ok(split_tuplespaceInstanton)
    }

    fn get_cone_approximate_middle(
        &self,
        cone: Cone,
        brane_id: u64,
    ) -> Result<Option<Vec<u8>>> {
        let get_causet_size = |causet: &str| self.get_cone_approximate_size_causet(causet, cone, brane_id, 0);

        let default_causet_size = box_try!(get_causet_size(Causet_DEFAULT));
        let write_causet_size = box_try!(get_causet_size(Causet_WRITE));

        let middle_by_causet = if default_causet_size >= write_causet_size {
            Causet_DEFAULT
        } else {
            Causet_WRITE
        };

        self.get_cone_approximate_middle_causet(middle_by_causet, cone, brane_id)
    }

    fn get_cone_approximate_middle_causet(
        &self,
        causetname: &str,
        cone: Cone,
        _brane_id: u64,
    ) -> Result<Option<Vec<u8>>> {
        let spacelike_key = &cone.spacelike_key;
        let lightlike_key = &cone.lightlike_key;
        let collection = box_try!(self.get_cone_properties_causet(causetname, &spacelike_key, &lightlike_key));

        let mut tuplespaceInstanton = Vec::new();
        for (_, v) in collection.iter() {
            let props = box_try!(ConeProperties::decode(&v.user_collected_properties()));
            tuplespaceInstanton.extlightlike(
                props
                    .take_excluded_cone(spacelike_key, lightlike_key)
                    .into_iter()
                    .map(|(k, _)| k),
            );
        }
        if tuplespaceInstanton.is_empty() {
            return Ok(None);
        }
        tuplespaceInstanton.sort();
        // Calculate the position by (len-1)/2. So it's the left one
        // of two middle positions if the number of tuplespaceInstanton is even.
        let middle = (tuplespaceInstanton.len() - 1) / 2;
        Ok(Some(tuplespaceInstanton.swap_remove(middle)))
    }

    fn divide_cone(&self, cone: Cone, brane_id: u64, parts: usize) -> Result<Vec<Vec<u8>>> {
        let default_causet_size =
            self.get_cone_approximate_tuplespaceInstanton_causet(Causet_DEFAULT, cone, brane_id, 0)?;
        let write_causet_size = self.get_cone_approximate_tuplespaceInstanton_causet(Causet_WRITE, cone, brane_id, 0)?;

        let causet = if default_causet_size >= write_causet_size {
            Causet_DEFAULT
        } else {
            Causet_WRITE
        };

        self.divide_cone_causet(causet, cone, brane_id, parts)
    }

    fn divide_cone_causet(
        &self,
        causet: &str,
        cone: Cone,
        _brane_id: u64,
        parts: usize,
    ) -> Result<Vec<Vec<u8>>> {
        let spacelike = &cone.spacelike_key;
        let lightlike = &cone.lightlike_key;
        let collection = self.get_cone_properties_causet(causet, spacelike, lightlike)?;

        let mut tuplespaceInstanton = Vec::new();
        let mut found_tuplespaceInstanton_count = 0;
        for (_, v) in collection.iter() {
            let props = ConeProperties::decode(&v.user_collected_properties())?;
            tuplespaceInstanton.extlightlike(
                props
                    .take_excluded_cone(spacelike, lightlike)
                    .into_iter()
                    .filter(|_| {
                        found_tuplespaceInstanton_count += 1;
                        found_tuplespaceInstanton_count % 100 == 0
                    })
                    .map(|(k, _)| k),
            );
        }

        debug!(
            "({} points found, {} points selected for dividing)",
            found_tuplespaceInstanton_count,
            tuplespaceInstanton.len()
        );

        if tuplespaceInstanton.is_empty() {
            return Ok(vec![]);
        }

        // If there are too many tuplespaceInstanton, reduce its amount before sorting, or it may take too much
        // time to sort the tuplespaceInstanton.
        if tuplespaceInstanton.len() > 20000 {
            let len = tuplespaceInstanton.len();
            tuplespaceInstanton = tuplespaceInstanton.into_iter().step_by(len / 10000).collect();
        }

        tuplespaceInstanton.sort();
        tuplespaceInstanton.dedup();

        // If the tuplespaceInstanton are too few, return them directly.
        if tuplespaceInstanton.len() < parts {
            return Ok(tuplespaceInstanton);
        }

        // Find `parts - 1` tuplespaceInstanton which divides the whole cone into `parts` parts evenly.
        let mut res = Vec::with_capacity(parts - 1);
        let section_len = (tuplespaceInstanton.len() as f64) / (parts as f64);
        for i in 1..parts {
            res.push(tuplespaceInstanton[(section_len * (i as f64)) as usize].clone())
        }
        Ok(res)
    }
}
