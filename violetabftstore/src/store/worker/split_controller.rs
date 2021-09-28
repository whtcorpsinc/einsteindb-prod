// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::slice::Iter;
use std::time::{Duration, SystemTime};

use ekvproto::kvrpc_timeshare::KeyCone;
use ekvproto::meta_timeshare::Peer;

use rand::Rng;

use violetabftstore::interlock::::collections::HashMap;
use violetabftstore::interlock::::config::Tracker;

use txn_types::Key;

use crate::store::worker::split_config::DEFAULT_SAMPLE_NUM;
use crate::store::worker::{FlowStatistics, SplitConfig, SplitConfigManager};

pub const TOP_N: usize = 10;

pub struct SplitInfo {
    pub brane_id: u64,
    pub split_key: Vec<u8>,
    pub peer: Peer,
}

pub struct Sample {
    pub key: Vec<u8>,
    pub left: i32,
    pub contained: i32,
    pub right: i32,
}

impl Sample {
    fn new(key: &[u8]) -> Sample {
        Sample {
            key: key.to_owned(),
            left: 0,
            contained: 0,
            right: 0,
        }
    }
}

// It will return prefix sum of iter. `read` is a function to be used to read data from iter.
fn prefix_sum<F, T>(iter: Iter<T>, read: F) -> Vec<usize>
where
    F: Fn(&T) -> usize,
{
    let mut pre_sum = vec![];
    let mut sum = 0;
    for item in iter {
        sum += read(&item);
        pre_sum.push(sum);
    }
    pre_sum
}

// It will return sample_num numbers by sample from lists.
// The list in the lists has the length of N1, N2, N3 ... Np ... NP in turn.
// Their prefix sum is pre_sum and we can get mut list from lists by get_mut.
// Take a random number d from [1, N]. If d < N1, select a data in the first list with an equal probability without replacement;
// If N1 <= d <(N1 + N2), then select a data in the second list with equal probability without replacement;
// and so on, repeat m times, and finally select sample_num pieces of data from lists.
fn sample<F, T>(
    sample_num: usize,
    pre_sum: &[usize],
    mut lists: Vec<T>,
    get_mut: F,
) -> Vec<KeyCone>
where
    F: Fn(&mut T) -> &mut Vec<KeyCone>,
{
    let mut rng = rand::thread_rng();
    let mut key_cones = vec![];
    let high_bound = pre_sum.last().unwrap();
    for _num in 0..sample_num {
        let d = rng.gen_cone(0, *high_bound) as usize;
        let i = match pre_sum.binary_search(&d) {
            Ok(i) => i,
            Err(i) => i,
        };
        let list = get_mut(&mut lists[i]);
        let j = rng.gen_cone(0, list.len()) as usize;
        key_cones.push(list.remove(j)); // Sampling without replacement
    }
    key_cones
}

// BraneInfo will maintain key_cones with sample_num length by reservoir sampling.
// And it will save qps num and peer.
#[derive(Debug, Clone)]
pub struct BraneInfo {
    pub sample_num: usize,
    pub qps: usize,
    pub peer: Peer,
    pub key_cones: Vec<KeyCone>,
}

impl BraneInfo {
    fn new(sample_num: usize) -> BraneInfo {
        BraneInfo {
            sample_num,
            qps: 0,
            key_cones: Vec::with_capacity(sample_num),
            peer: Peer::default(),
        }
    }

    fn get_qps(&self) -> usize {
        self.qps
    }

    fn get_key_cones_mut(&mut self) -> &mut Vec<KeyCone> {
        &mut self.key_cones
    }

    fn add_key_cones(&mut self, key_cones: Vec<KeyCone>) {
        self.qps += key_cones.len();
        for key_cone in key_cones {
            if self.key_cones.len() < self.sample_num {
                self.key_cones.push(key_cone);
            } else {
                let i = rand::thread_rng().gen_cone(0, self.qps) as usize;
                if i < self.sample_num {
                    self.key_cones[i] = key_cone;
                }
            }
        }
    }

    fn fidelio_peer(&mut self, peer: &Peer) {
        if self.peer != *peer {
            self.peer = peer.clone();
        }
    }
}

pub struct Recorder {
    pub detect_num: u64,
    pub peer: Peer,
    pub key_cones: Vec<Vec<KeyCone>>,
    pub times: u64,
    pub create_time: SystemTime,
}

impl Recorder {
    fn new(detect_num: u64) -> Recorder {
        Recorder {
            detect_num,
            peer: Peer::default(),
            key_cones: vec![],
            times: 0,
            create_time: SystemTime::now(),
        }
    }

    fn record(&mut self, key_cones: Vec<KeyCone>) {
        self.times += 1;
        self.key_cones.push(key_cones);
    }

    fn fidelio_peer(&mut self, peer: &Peer) {
        if self.peer != *peer {
            self.peer = peer.clone();
        }
    }

    fn is_ready(&self) -> bool {
        self.times >= self.detect_num
    }

    fn collect(&mut self, config: &SplitConfig) -> Vec<u8> {
        let pre_sum = prefix_sum(self.key_cones.iter(), Vec::len);
        let key_cones = self.key_cones.clone();
        let mut samples = sample(config.sample_num, &pre_sum, key_cones, |x| x)
            .iter()
            .map(|key_cone| Sample::new(&key_cone.spacelike_key))
            .collect();
        for key_cones in &self.key_cones {
            for key_cone in key_cones {
                Recorder::sample(&mut samples, &key_cone);
            }
        }
        Recorder::split_key(
            samples,
            config.split_balance_score,
            config.split_contained_score,
            config.sample_memory_barrier,
        )
    }

    fn sample(samples: &mut Vec<Sample>, key_cone: &KeyCone) {
        for mut sample in samples.iter_mut() {
            let order_spacelike = if key_cone.spacelike_key.is_empty() {
                Ordering::Greater
            } else {
                sample.key.cmp(&key_cone.spacelike_key)
            };

            let order_lightlike = if key_cone.lightlike_key.is_empty() {
                Ordering::Less
            } else {
                sample.key.cmp(&key_cone.lightlike_key)
            };

            if order_spacelike == Ordering::Greater && order_lightlike == Ordering::Less {
                sample.contained += 1;
            } else if order_spacelike != Ordering::Greater {
                sample.right += 1;
            } else {
                sample.left += 1;
            }
        }
    }

    fn split_key(
        samples: Vec<Sample>,
        split_balance_score: f64,
        split_contained_score: f64,
        sample_memory_barrier: i32,
    ) -> Vec<u8> {
        let mut best_index: i32 = -1;
        let mut best_score = 2.0;
        for index in 0..samples.len() {
            let sample = &samples[index];
            let sampled = sample.contained + sample.left + sample.right;
            if (sample.left + sample.right) == 0 || sampled < sample_memory_barrier {
                continue;
            }
            let diff = (sample.left - sample.right) as f64;
            let balance_score = diff.abs() / (sample.left + sample.right) as f64;
            if balance_score >= split_balance_score {
                continue;
            }
            let contained_score = sample.contained as f64 / sampled as f64;
            if contained_score >= split_contained_score {
                continue;
            }
            let final_score = balance_score + contained_score;
            if final_score < best_score {
                best_index = index as i32;
                best_score = final_score;
            }
        }
        if best_index >= 0 {
            return samples[best_index as usize].key.clone();
        }
        return vec![];
    }
}

#[derive(Clone, Debug)]
pub struct ReadStats {
    pub flows: HashMap<u64, FlowStatistics>,
    pub brane_infos: HashMap<u64, BraneInfo>,
    pub sample_num: usize,
}

impl ReadStats {
    pub fn default() -> ReadStats {
        ReadStats {
            sample_num: DEFAULT_SAMPLE_NUM,
            brane_infos: HashMap::default(),
            flows: HashMap::default(),
        }
    }

    pub fn add_qps(&mut self, brane_id: u64, peer: &Peer, key_cone: KeyCone) {
        self.add_qps_batch(brane_id, peer, vec![key_cone]);
    }

    pub fn add_qps_batch(&mut self, brane_id: u64, peer: &Peer, key_cones: Vec<KeyCone>) {
        let num = self.sample_num;
        let brane_info = self
            .brane_infos
            .entry(brane_id)
            .or_insert_with(|| BraneInfo::new(num));
        brane_info.fidelio_peer(peer);
        brane_info.add_key_cones(key_cones);
    }

    pub fn add_flow(&mut self, brane_id: u64, write: &FlowStatistics, data: &FlowStatistics) {
        let flow_stats = self
            .flows
            .entry(brane_id)
            .or_insert_with(FlowStatistics::default);
        flow_stats.add(write);
        flow_stats.add(data);
    }

    pub fn is_empty(&self) -> bool {
        self.brane_infos.is_empty() && self.flows.is_empty()
    }
}

pub struct AutoSplitController {
    pub recorders: HashMap<u64, Recorder>,
    causet: SplitConfig,
    causet_tracker: Tracker<SplitConfig>,
}

impl AutoSplitController {
    pub fn new(config_manager: SplitConfigManager) -> AutoSplitController {
        AutoSplitController {
            recorders: HashMap::default(),
            causet: config_manager.value().clone(),
            causet_tracker: config_manager.0.clone().tracker("split_hub".to_owned()),
        }
    }

    pub fn default() -> AutoSplitController {
        AutoSplitController::new(SplitConfigManager::default())
    }

    pub fn flush(&mut self, others: Vec<ReadStats>) -> (Vec<usize>, Vec<SplitInfo>) {
        let mut split_infos = Vec::default();
        let mut top = BinaryHeap::with_capacity(TOP_N as usize);

        // collect from different thread
        let mut brane_infos_map = HashMap::default(); // braneID-braneInfos
        let capacity = others.len();
        for other in others {
            for (brane_id, brane_info) in other.brane_infos {
                if brane_info.key_cones.len() >= self.causet.sample_num {
                    let brane_infos = brane_infos_map
                        .entry(brane_id)
                        .or_insert_with(|| Vec::with_capacity(capacity));
                    brane_infos.push(brane_info);
                }
            }
        }

        for (brane_id, brane_infos) in brane_infos_map {
            let pre_sum = prefix_sum(brane_infos.iter(), BraneInfo::get_qps);

            let qps = *pre_sum.last().unwrap(); // brane_infos is not empty
            let num = self.causet.detect_times;
            if qps > self.causet.qps_memory_barrier {
                let recorder = self
                    .recorders
                    .entry(brane_id)
                    .or_insert_with(|| Recorder::new(num));

                recorder.fidelio_peer(&brane_infos[0].peer);

                let key_cones = sample(
                    self.causet.sample_num,
                    &pre_sum,
                    brane_infos,
                    BraneInfo::get_key_cones_mut,
                );

                recorder.record(key_cones);
                if recorder.is_ready() {
                    let key = recorder.collect(&self.causet);
                    if !key.is_empty() {
                        let split_info = SplitInfo {
                            brane_id,
                            split_key: Key::from_raw(&key).into_encoded(),
                            peer: recorder.peer.clone(),
                        };
                        split_infos.push(split_info);
                        info!("load base split brane";"brane_id"=>brane_id);
                    }
                    self.recorders.remove(&brane_id);
                }
            } else {
                self.recorders.remove_entry(&brane_id);
            }
            top.push(qps);
        }

        (top.into_vec(), split_infos)
    }

    pub fn clear(&mut self) {
        let interval = Duration::from_secs(self.causet.detect_times * 2);
        self.recorders
            .retain(|_, recorder| recorder.create_time.elapsed().unwrap() < interval);
    }

    pub fn refresh_causet(&mut self) {
        if let Some(incoming) = self.causet_tracker.any_new() {
            self.causet = incoming.clone();
        }
    }
}

#[causet(test)]
mod tests {
    use super::*;
    use crate::store::util::build_key_cone;

    enum Position {
        Left,
        Right,
        Contained,
    }

    impl Sample {
        fn num(&self, pos: Position) -> i32 {
            match pos {
                Position::Left => self.left,
                Position::Right => self.right,
                Position::Contained => self.contained,
            }
        }
    }

    struct SampleCase {
        key: Vec<u8>,
    }

    impl SampleCase {
        fn sample_key(&self, spacelike_key: &[u8], lightlike_key: &[u8], pos: Position) {
            let mut samples = vec![Sample::new(&self.key)];
            let key_cone = build_key_cone(spacelike_key, lightlike_key, false);
            Recorder::sample(&mut samples, &key_cone);
            assert_eq!(
                samples[0].num(pos),
                1,
                "spacelike_key is {:?}, lightlike_key is {:?}",
                String::from_utf8(Vec::from(spacelike_key)).unwrap(),
                String::from_utf8(Vec::from(lightlike_key)).unwrap()
            );
        }
    }

    #[test]
    fn test_pre_sum() {
        let v = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
        let expect = vec![1, 3, 6, 10, 15, 21, 28, 36, 45];
        let pre = prefix_sum(v.iter(), |x| *x);
        for i in 0..v.len() {
            assert_eq!(expect[i], pre[i]);
        }
    }

    #[test]
    fn test_sample() {
        let sc = SampleCase { key: vec![b'c'] };

        // limit scan
        sc.sample_key(b"a", b"b", Position::Left);
        sc.sample_key(b"a", b"c", Position::Left);
        sc.sample_key(b"a", b"d", Position::Contained);
        sc.sample_key(b"c", b"d", Position::Right);
        sc.sample_key(b"d", b"e", Position::Right);

        // point get
        sc.sample_key(b"a", b"a", Position::Left);
        sc.sample_key(b"c", b"c", Position::Right); // when happened 100 times (a,a) and 100 times (c,c), we will split from c.
        sc.sample_key(b"d", b"d", Position::Right);

        // unlimited scan
        sc.sample_key(b"", b"", Position::Contained);
        sc.sample_key(b"a", b"", Position::Contained);
        sc.sample_key(b"c", b"", Position::Right);
        sc.sample_key(b"d", b"", Position::Right);
        sc.sample_key(b"", b"a", Position::Left);
        sc.sample_key(b"", b"c", Position::Left);
        sc.sample_key(b"", b"d", Position::Contained);
    }

    #[test]
    fn test_hub() {
        let mut hub = AutoSplitController::new(SplitConfigManager::default());
        hub.causet.qps_memory_barrier = 1;
        hub.causet.sample_memory_barrier = 0;

        for i in 0..100 {
            let mut qps_stats = ReadStats::default();
            for _ in 0..100 {
                qps_stats.add_qps(1, &Peer::default(), build_key_cone(b"a", b"b", false));
                qps_stats.add_qps(1, &Peer::default(), build_key_cone(b"b", b"c", false));
            }
            let (_, split_infos) = hub.flush(vec![qps_stats]);
            if (i + 1) % hub.causet.detect_times == 0 {
                assert_eq!(split_infos.len(), 1);
                assert_eq!(
                    Key::from_encoded(split_infos[0].split_key.clone())
                        .into_raw()
                        .unwrap(),
                    b"b"
                );
            }
        }
    }

    const REGION_NUM: u64 = 1000;
    const KEY_RANGE_NUM: u64 = 1000;

    fn default_qps_stats() -> ReadStats {
        let mut qps_stats = ReadStats::default();
        for i in 0..REGION_NUM {
            for _j in 0..KEY_RANGE_NUM {
                qps_stats.add_qps(i, &Peer::default(), build_key_cone(b"a", b"b", false))
            }
        }
        qps_stats
    }

    #[bench]
    fn recorder_sample(b: &mut test::Bencher) {
        let mut samples = vec![Sample::new(b"c")];
        let key_cone = build_key_cone(b"a", b"b", false);
        b.iter(|| {
            Recorder::sample(&mut samples, &key_cone);
        });
    }

    #[bench]
    fn hub_flush(b: &mut test::Bencher) {
        let mut other_qps_stats = vec![];
        for _i in 0..10 {
            other_qps_stats.push(default_qps_stats());
        }
        b.iter(|| {
            let mut hub = AutoSplitController::new(SplitConfigManager::default());
            hub.flush(other_qps_stats.clone());
        });
    }

    #[bench]
    fn qps_scan(b: &mut test::Bencher) {
        let mut qps_stats = default_qps_stats();
        let spacelike_key = Key::from_raw(b"a");
        let lightlike_key = Some(Key::from_raw(b"b"));

        b.iter(|| {
            if let Ok(spacelike_key) = spacelike_key.to_owned().into_raw() {
                let mut key = vec![];
                if let Some(lightlike_key) = &lightlike_key {
                    if let Ok(lightlike_key) = lightlike_key.to_owned().into_raw() {
                        key = lightlike_key;
                    }
                }
                qps_stats.add_qps(
                    0,
                    &Peer::default(),
                    build_key_cone(&spacelike_key, &key, false),
                );
            }
        });
    }

    #[bench]
    fn qps_add(b: &mut test::Bencher) {
        let mut qps_stats = default_qps_stats();
        b.iter(|| {
            qps_stats.add_qps(0, &Peer::default(), build_key_cone(b"a", b"b", false));
        });
    }
}
