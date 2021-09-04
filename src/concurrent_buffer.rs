use crate::part_algorithm;
use crate::hashing;
use std::collections::HashMap;
use std::thread;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone, Copy)]
struct Config {
    num_threads: usize,
    hash_bits: usize,
    input_size: usize, 
}

pub struct ConcurrentBuffer {
    config: Config,
    pub data: Vec<(u64,u64)>,
    bins: Vec<Vec<(u64,u64)>>,
    bin_ptrs: Vec<AtomicUsize>,
}

#[derive(Clone, Copy)]
struct Wrapper(NonNull<ConcurrentBuffer>);
unsafe impl std::marker::Send for Wrapper { }
unsafe impl std::marker::Sync for Wrapper { }

impl part_algorithm::PartitioningAlgorithm for ConcurrentBuffer {
    fn new(num_threads: usize, data: Vec<(u64,u64)>, hash_bits: usize) -> Self {
        let input_size = data.len();
        let num_partitions = 2_u32.pow(hash_bits as u32) as usize;
        let partition_size = ((input_size as f64 / num_partitions as f64) as f64 * 1.5) as usize;
        
        let bins = vec![vec![(0,0); partition_size]; num_partitions];
        let bin_ptrs = (0..num_partitions).map(|_| AtomicUsize::new(0)).collect();
        
        let config = Config {num_threads, hash_bits, input_size};
        ConcurrentBuffer {config, data, bins, bin_ptrs}
    }

    fn partition(&mut self) {
        partition_scoped(self);
    }

    fn to_map(&self) -> HashMap<u64,u64> {
        let mut map: HashMap<u64,u64> = HashMap::new();
        for i in 0..self.bins.len() {
            for j in 0..self.bins[0].len() {
                let tuple = self.bins[i][j];
                if tuple.1 > 0 {
                    map.insert(tuple.0, tuple.1);
                }
            }
        }
        map
    }

    fn len(&self) -> usize {
        self.len_partitions().iter().sum()
    }

    fn len_partitions(&self) -> Vec<usize> {
        let mut vec = vec![0; self.bins.len()];
        for i in 0..self.bins.len() {
            for j in 0..self.bins[0].len() {
                if self.bins[i][j].1 > 0 {
                    vec[i] += 1;
                }
            }
        }
        vec
    }

    fn print_stats(&self) {
        // nothing to see here
    }
}


fn partition_scoped(parallel_buffer: &mut ConcurrentBuffer) {
    let mut threads = vec![];
    let conf = parallel_buffer.config.clone();
    let ptr = parallel_buffer as *mut ConcurrentBuffer;
    let sendable_ptr =  Wrapper(NonNull::new(ptr).unwrap());
    for i in 0 .. conf.num_threads {
        let t_index = i;
        threads.push(thread::spawn(move || {
            unsafe {
                let pb = sendable_ptr.0.as_ptr();
                let mut ptr_in = t_index;
                while ptr_in < conf.input_size {
                    let tuple = (*pb).data[ptr_in as usize];
                    let hash = hashing::hash_data(tuple.0, conf.hash_bits);
                    let ptr_out = (*pb).bin_ptrs[hash].fetch_add(1, Ordering::Relaxed);
                    (*pb).bins[hash][ptr_out] = (tuple.0, tuple.1);
                    ptr_in += conf.num_threads;
                }
            }
        }));
    }
    for t in threads {
        let _ = t.join().unwrap();
    }
}
