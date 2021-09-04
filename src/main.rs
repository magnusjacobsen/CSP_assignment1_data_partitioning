pub mod part_algorithm;
pub mod parallel_buffer;
pub mod concurrent_buffer;
mod hashing;

use part_algorithm::PartitioningAlgorithm;
use parallel_buffer::ParallelBuffer;
use concurrent_buffer::ConcurrentBuffer;
use std::env;
use std::str::FromStr;
use std::collections::HashMap;
use rand::prelude::*;
use std::time::{Instant};

fn main() {
    let args: Vec<String> = env::args().map(|x| x.to_string()).collect();
    if args.len() < 6 {
        let error = "Error: invalid args. Need to specify:
    * algorithm(s)   - Single or several, separated by commas (no space pls)
    * data size(s)   - Single or several, separated by commas. In 2^n.
    * hash bits      - Single number or range (e.g. 1-6).
    * num threads    - Single number or range.
    * repeats        - Number of times to repeat each test. 
    * test integrity - 'test' (optional).";
        println!("{}", error);
    } else {
        let algo_names: Vec<String> = get_algorithm_names(&args[1]);
        let data_sizes: Vec<usize> = get_data_sizes(&args[2]);
        let hash_bits: Vec<usize> = get_numeric_range(&args[3]);
        let thread_range: Vec<usize> = get_numeric_range(&args[4]);
        let repeats = usize::from_str(&args[5]).unwrap();
        let run_tests = if args.len() > 6 { &args[6] == "test"} else { false };
        run_experiments(algo_names, data_sizes, hash_bits, thread_range, repeats, run_tests);
    }
}

fn get_algorithm_names(s: &String) -> Vec<String> {
    s.split(",").map(|x| x.to_string()).collect()
}

fn get_data_sizes(s: &String) -> Vec<usize> {
    s.split(",").map(|x| (2_usize).pow(u32::from_str(x).unwrap())).collect()
}

fn get_numeric_range(s: &String) -> Vec<usize> {
    if s.contains("-") {
        let range: Vec<usize> = s.split("-").map(|x| usize::from_str(x).unwrap()).collect();
        (range[0] .. range[1]).collect()
    } else {
        vec![usize::from_str(s).unwrap()]
    }
}

fn run_experiments(algo_names: Vec<String>, data_sizes: Vec<usize>, hash_bits: Vec<usize>, thread_range: Vec<usize>, repeats: usize, run_tests: bool) {
    for algo_name in algo_names {
        for n in &data_sizes {
            for bits in &hash_bits {
                for num_threads in &thread_range {
                    let mut run = 0;
                    let mut times_as_millis = vec![0; repeats];
                    while run < repeats {     
                        let result = run_algorithm(&algo_name, *num_threads, *n, *bits, run as u64, run_tests);
                        if let Some((elapsed, verified)) = result {
                            times_as_millis[run] = elapsed;
                            let elapsed = elapsed as f64;
                            let time_s = elapsed / 1000.;
                            let throughput = (*n as f64 / 1000000.) / time_s;
                            run += 1;
                            if run_tests {
                                println!("time: {}ms, throughput: {} M/s, integrity: {}", elapsed, throughput, verified);
                            } else {
                                println!("time: {}ms, throughput: {} M/s", elapsed, throughput); 
                            }
                        } else {
                            println!("No matching algorithm found for: {}", algo_name);
                            break;
                        }
                    }
                    if repeats > 1 {
                        print_results(times_as_millis, *n as f64);
                    }
                }
            }
        }
    }
}

fn test_integrity(part_map: HashMap<u64,u64>, data: &Vec<(u64,u64)>) -> (bool,bool) {
    let size_out = part_map.len();
    let mut all_present = true;
    for (key,value) in data {
        if let Some(v) = part_map.get(&key) {
            if value == v {
                continue;    
            }
        }
        all_present = false;
        break;
    }

    let equal_size = size_out == data.len();
    (equal_size, all_present)
}

fn _print_integrity(equal_size: bool, all_present: bool, size_out: usize, size_in: usize) {
    println!("");
    println!("********** Integrity test **********");
    println!("all present: {}", all_present);
    println!("equal sizes: {}", equal_size);
    println!("in size: {}", size_in);
    println!("out size: {}", size_out);
}

fn print_results(mut times_as_millis: Vec<u128>, data_size: f64) {
    times_as_millis.sort();
    // filter out outliers, 1 in each direction of the sorted lists
    let filtered: Vec<f64> = times_as_millis[1..times_as_millis.len() - 1]
                                .iter()
                                .map(|x| *x as f64)
                                .collect();
    let n = filtered.len() as f64;
    let total_time: f64 = filtered.iter().sum();
    let throughputs: Vec<f64> = filtered.iter().map(|x| (data_size / 1000000.) / (x / 1000.)).collect();
    let total_throughputs: f64 = throughputs.iter().sum();
    let mu_time = total_time / n;
    let mu_throughput = total_throughputs / n;
    let mut acc_sqr_time = 0.0;
    let mut acc_sqr_throughput = 0.0;
    for i in 0..filtered.len() {
        let time_i = filtered[i] - mu_time;
        let throughput_i = throughputs[i] - mu_throughput;
        acc_sqr_time += time_i * time_i;
        acc_sqr_throughput += throughput_i * throughput_i;
    }
    let sigma_time = (acc_sqr_time / n).sqrt(); // sample standard deviation
    let sigma_throughput = (acc_sqr_throughput / n).sqrt();

    println!("********** RESULTS **********");
    println!("Average time:       {:.2} ms", mu_time);
    println!("Time StdDev:        {:.4}", sigma_time);
    println!("Average throughput: {:.2} M/S", mu_throughput);
    println!("Throughput StdDev:  {:.4}", sigma_throughput);
}

fn run_algorithm(name: &str, num_threads: usize, data_size: usize, hash_bits: usize, seed: u64, run_tests: bool) -> Option<(u128, bool)> {
    let data = generate_data(data_size, seed);
    if name == "parallel" {
        let mut algorithm = ParallelBuffer::new(num_threads, data, hash_bits);
        let start = Instant::now();
        algorithm.partition();
        let elapsed = start.elapsed().as_millis();
        let mut verified = false;
        if run_tests {
            let (equal_size, all_present) = test_integrity(algorithm.to_map(), &algorithm.data);
            verified = equal_size && all_present;
        }
        //algorithm.print_stats();
        Some((elapsed, verified))
    } else if name == "concurrent" {
        let mut algorithm = ConcurrentBuffer::new(num_threads, data, hash_bits);
        let start = Instant::now();
        algorithm.partition();
        let elapsed = start.elapsed().as_millis();
        let mut verified = false;
        if run_tests {
            let (equal_size, all_present) = test_integrity(algorithm.to_map(), &algorithm.data);
            verified = equal_size && all_present;
        }
        Some((elapsed, verified))
    } else {
        None
    }
}

fn generate_data(size: usize, _seed: u64) -> Vec<(u64,u64)> {
    //let mut rng = thread_rng();
    
    let mut rng: StdRng = SeedableRng::from_seed([_seed as u8; 32]);
    let data: Vec<(u64,u64)> = (0..size).map(|i| (i as u64, rng.gen_range(1,1000000) as u64)).collect();
    //println!("rand: {:?}", &data[0]);
    //println!("rand: {:?}", &data[1000]);
    data
}
