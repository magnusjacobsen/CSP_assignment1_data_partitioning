use std::collections::HashMap;

pub trait PartitioningAlgorithm {
    fn new(num_threads: usize, data: Vec<(u64,u64)>, hash_bits: usize) -> Self;
    fn partition(&mut self);
    fn to_map(&self) -> HashMap<u64,u64>;
    fn len(&self) -> usize;
    fn len_partitions(&self) -> Vec<usize>;
    fn print_stats(&self);
}
