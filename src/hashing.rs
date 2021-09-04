    /**
     * Multiplicative hashing based on
     * https://stackoverflow.com/questions/11871245/knuth-multiplicative-hash
     * */
     pub fn hash_data(key: u64, hash_bits: usize) -> usize {
        let key = key as u32;
        let knuth_constant: u32 = 2654435761;
        ((key).overflowing_mul(knuth_constant).0 >> (32 - hash_bits)) as usize
     }