use crate::part_algorithm;
use crate::hashing;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::cmp::{max, min};
use std::collections::HashMap;
use std::thread;
use std::ptr::NonNull;

#[derive(Clone, Copy)]
struct Config {
    num_threads: usize,
    hash_bits: usize,
    num_partitions: usize,
    input_size: usize, 
    chunk_size_in: usize,
    chunk_size_out: usize,
}

pub struct ParallelBuffer {
    config: Config,
    h_in: AtomicUsize,// number of chunks in use in input data
    h_out: Vec<AtomicUsize>, // number of chunks in use, per partition
    // p_in: Vec<usize>, put into the threads instead
    // p_out: Vec<Vec<usize>>,
    pub data: Vec<(u64,u64)>,
    partition_buffers: Vec<Vec<(u64,u64)>>,
}

#[derive(Clone, Copy)]
struct Wrapper(NonNull<ParallelBuffer>);
unsafe impl std::marker::Send for Wrapper { }
unsafe impl std::marker::Sync for Wrapper { }

impl part_algorithm::PartitioningAlgorithm for ParallelBuffer {
    fn new(num_threads: usize, data: Vec<(u64,u64)>, hash_bits: usize) -> Self {
        let input_size = data.len();
        let num_partitions = 2_u32.pow(hash_bits as u32) as usize;
        let min_part_size = max(input_size / num_partitions, num_threads);

        let chunk_size_in = min(max(input_size / num_threads, 1), 128);
        let chunk_size_out = min(max(((min_part_size) / num_threads) / 2, 1), 128);

        let partition_size = max((min_part_size as f64 * 1.5) as usize, num_threads * 2);
        let h_in = AtomicUsize::new(0);
        let h_out: Vec<AtomicUsize> = (0..num_partitions).map(|_| AtomicUsize::new(0)).collect();
        let partition_buffers = vec![vec![(0,0); partition_size]; num_partitions];
        
        let config = Config {num_threads, hash_bits, num_partitions, 
                             input_size, chunk_size_in, chunk_size_out};
        ParallelBuffer {config, h_in, h_out, data, 
                        partition_buffers}
    }

    fn partition(&mut self) {
        partition_scoped(self);
    }

    fn to_map(&self) -> HashMap<u64,u64> {
        let mut map: HashMap<u64,u64> = HashMap::new();
        for i in 0..self.partition_buffers.len() {
            for j in 0..self.partition_buffers[0].len() {
                let tuple = self.partition_buffers[i][j];
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
        let mut vec = vec![0; self.partition_buffers.len()];
        for i in 0..self.partition_buffers.len() {
            for j in 0..self.partition_buffers[0].len() {
                if self.partition_buffers[i][j].1 > 0 {
                    vec[i] += 1;
                }
            }
        }
        vec
    }

    fn print_stats(&self) {
        let n = self.partition_buffers.len();
        let sizes = self.len_partitions();
        let min = &sizes.iter().min().unwrap();
        let max = &sizes.iter().max().unwrap();
        let sum = *(&sizes.iter().sum::<usize>()) as f64;
        let mu = sum / (n as f64);
        let mut acc = 0.0;
        for x in &sizes {
            let current = *x as f64 - mu;
            acc = current * current + acc;    
        }
        let sigma = acc / (n as f64);
        
        println!("******* STATS ********");
        println!("partitions: {}, elements: {}", n, self.len());
        println!("min: {}, max: {}", min, max);
        println!("StdDev: {}, mean: {}", sigma, mu);
        println!("***********************");
    }
}

fn partition_scoped(parallel_buffer: &mut ParallelBuffer) {
    let mut threads = vec![];
    let conf = parallel_buffer.config.clone();
    let ptr = parallel_buffer as *mut ParallelBuffer;
    let sendable_ptr =  Wrapper(NonNull::new(ptr).unwrap());
    for _ in 0 .. conf.num_threads {
        threads.push(thread::spawn(move || {
            unsafe {
                let pb = sendable_ptr.0.as_ptr();
                let mut ptr_in = (*pb).h_in.fetch_add(1, Ordering::Relaxed) * conf.chunk_size_in;
                let mut ptr_out: Vec<usize> = 
                    (0..conf.num_partitions)
                        .map(|x| (*pb).h_out[x].fetch_add(1, Ordering::Relaxed) * conf.chunk_size_out)
                        .collect();
                let mut p_in = 0;
                let mut p_out = vec![0; conf.num_partitions];
                while ptr_in < conf.input_size {
                    // read data
                    let tuple = (*pb).data[ptr_in];

                    let hash = hashing::hash_data(tuple.0, conf.hash_bits);
                    if p_out[hash] >= conf.chunk_size_out {
                        ptr_out[hash] = (*pb).h_out[hash].fetch_add(1, Ordering::Relaxed) * conf.chunk_size_out;
                        p_out[hash] = 0;
                    }

                    // write data
                    (*pb).partition_buffers[hash][ptr_out[hash]] =
                        (tuple.0, tuple.1);

                    p_out[hash] += 1;
                    p_in += 1;
                    ptr_in += 1;
                    ptr_out[hash] += 1;

                    if p_in >= conf.chunk_size_in {
                        ptr_in = (*pb).h_in.fetch_add(1, Ordering::Relaxed) * conf.chunk_size_in;
                        p_in = 0;
                    }
                }
            }
        }));
    }
    for t in threads {
        let _ = t.join().unwrap();
    }
}
