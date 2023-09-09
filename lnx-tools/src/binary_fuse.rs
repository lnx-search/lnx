use std::collections::HashSet;
use std::hash::{Hash, Hasher};

use anyhow::{bail, Result};
use rkyv::{Archive, Deserialize, Serialize};
use crate::hashers::NoOpRandomState;

// probability of success should always be > 0.5 so 100 iterations is highly unlikely.
const XOR_MAX_ITERATIONS: usize = 100;

type FuseHasher = cityhasher::CityHasher;

#[derive(Default)]
/// A builder for constructing binary fuse filters using
/// 16 bit masks.
pub struct Fuse16Builder {
    keys: HashSet<u64, NoOpRandomState>,
}

impl Fuse16Builder {
    #[inline]
    /// Inserts a new key into the builder.
    ///
    /// Returns if the key was inserted or not, since the filter does not
    /// accept duplicate keys, it is important to be aware if you have duplicates or not.
    pub fn insert<K: Hash>(&mut self, key: &K) -> bool {
        let mut hasher = FuseHasher::default();
        key.hash(&mut hasher);
        let key = hasher.finish();
        self.keys.insert(key)
    }

    #[inline]
    /// Inserts multiple keys into the builder.
    pub fn extend<'a, I, K>(&mut self, keys: I)
    where
        I: IntoIterator<Item = &'a K> + 'a,
        K: Hash + 'a,
    {
        for key in keys {
            self.insert(key);
        }
    }

    /// Build a new binary fuse filter.
    pub fn build(self) -> Result<Fuse16> {
        let mut fuse = Fuse16::new(self.keys.len() as u32);
        let keys = self.keys.into_iter().collect::<Vec<u64>>();
        fuse.populate_with_keys(keys)?;
        Ok(fuse)
    }
}

#[derive(Archive, Serialize, Deserialize)]
/// A binary fuse filter is a probabilistic data-structure to test membership of an element in a set.
pub struct Fuse16 {
    seed: u64,
    segment_length: u32,
    segment_length_mask: u32,
    segment_count: u32,
    segment_count_length: u32,
    #[with(rkyv::with::Raw)]
    fingerprints: Vec<u16>,
}

impl Fuse16 {
    #[inline]
    /// Contains tell you whether the key is likely part of the set, with a false
    /// positive rate.
    pub fn contains<K: Hash>(&self, key: &K) -> bool {
        let key = {
            let mut hasher = FuseHasher::default();
            key.hash(&mut hasher);
            hasher.finish()
        };

        self.contains_digest(key)
    }

    #[inline]
    /// Contains tell you whether the key, as pre-computed digest form, is likely
    /// part of the set, with a false positive rate.
    pub fn contains_digest(&self, digest: u64) -> bool {
        let hash = binary_fuse_mix_split(digest, self.seed);
        let mut f = binary_fuse16_fingerprint(hash) as u16;
        let BinaryHashes { h0, h1, h2 } = self.binary_fuse16_hash_batch(hash);
        f ^= self.fingerprints[h0 as usize]
            ^ self.fingerprints[h1 as usize]
            ^ self.fingerprints[h2 as usize];
        f == 0
    }

    #[inline]
    /// A reference to the internal fuse fingerprints.
    pub fn fingerprints(&self) -> &[u16] {
        &self.fingerprints
    }

    fn new(size: u32) -> Self {
        use std::cmp;

        let arity = 3_u32;

        let segment_length = match size {
            0 => 4,
            size => cmp::min(binary_fuse_calculate_segment_length(arity, size), 262144),
        };

        let segment_length_mask = segment_length - 1;
        let mut array_length = {
            let size_factor = binary_fuse_calculate_size_factor(arity, size);
            let cap = match size {
                0 | 1 => 0,
                size => ((size as f64) * size_factor).round() as u32,
            };
            let n =
                ((cap + segment_length - 1) / segment_length).wrapping_sub(arity - 1);
            (n.wrapping_add(arity) - 1) * segment_length
        };

        let mut segment_count = (array_length + segment_length - 1) / segment_length;
        segment_count = if segment_count <= (arity - 1) {
            1
        } else {
            segment_count - (arity - 1)
        };

        array_length = (segment_count + arity - 1) * segment_length;
        let segment_count_length = segment_count * segment_length;

        Self {
            seed: u64::default(),
            segment_length,
            segment_length_mask,
            segment_count,
            segment_count_length,
            fingerprints: vec![0; array_length as usize],
        }
    }

    fn populate_with_keys(&mut self, keys: Vec<u64>) -> Result<()> {
        let mut rng_counter = 0x726b2b9d438b9d4d_u64;
        let capacity = self.fingerprints.len();
        let size = keys.len();

        self.seed = binary_fuse_rng_splitmix64(&mut rng_counter);

        let mut reverse_order: Vec<u64> = vec![0; size + 1];
        let mut reverse_h: Vec<u8> = vec![0; size];
        let mut alone: Vec<u32> = vec![0; capacity];
        let mut t2count: Vec<u8> = vec![0; capacity];
        let mut t2hash: Vec<u64> = vec![0; capacity];

        let mut block_bits: u32 = 1;
        while (1_u32 << block_bits) < self.segment_count {
            block_bits += 1;
        }
        let block = 1_u32 << block_bits;

        let mut start_pos: Vec<u32> = vec![0; 1 << block_bits];

        let mut h012 = [0_u32; 5];

        reverse_order[size] = 1; // sentinel
        let mut iter = 0..=XOR_MAX_ITERATIONS;
        loop {
            if iter.next().is_none() {
                bail!("Too many iterations. Are all your keys unique?");
            }

            for i in 0_u32..block {
                // important : i * size would overflow as a 32-bit number in some
                // cases.
                start_pos[i as usize] =
                    (((i as u64) * (size as u64)) >> block_bits) as u32;
            }

            let mask_block = (block - 1) as u64;
            for digest in keys.iter().take(size) {
                let hash: u64 = binary_fuse_murmur64(digest.wrapping_add(self.seed));
                let mut segment_index: u64 = hash >> (64 - block_bits);
                while reverse_order[start_pos[segment_index as usize] as usize] != 0 {
                    segment_index += 1;
                    segment_index &= mask_block;
                }
                reverse_order[start_pos[segment_index as usize] as usize] = hash;
                start_pos[segment_index as usize] += 1;
            }

            let mut error: usize = 0;
            for rev_order in reverse_order.iter().take(size) {
                let hash: u64 = *rev_order;

                let h0: usize = self.binary_fuse16_hash(0, hash) as usize;
                t2count[h0] = t2count[h0].wrapping_add(4);
                t2hash[h0] ^= hash;

                let h1: usize = self.binary_fuse16_hash(1, hash) as usize;
                t2count[h1] = t2count[h1].wrapping_add(4);
                t2count[h1] ^= 1;
                t2hash[h1] ^= hash;

                let h2: usize = self.binary_fuse16_hash(2, hash) as usize;
                t2count[h2] = t2count[h2].wrapping_add(4);
                t2hash[h2] ^= hash;
                t2count[h2] ^= 2;

                error = if t2count[h0] < 4 { 1 } else { error };
                error = if t2count[h1] < 4 { 1 } else { error };
                error = if t2count[h2] < 4 { 1 } else { error };
            }

            if error > 0 {
                reverse_order.fill(0);
                reverse_order[size] = 1; // sentinel
                t2count.fill(0);
                t2hash.fill(0);
                self.seed = binary_fuse_rng_splitmix64(&mut rng_counter);
                continue;
            }

            let mut q_size = 0_usize; // End of key addition

            // Add sets with one key to the queue.
            for (i, x) in t2count.iter().enumerate().take(capacity) {
                alone[q_size] = i as u32;
                q_size += if (x >> 2) == 1 { 1 } else { 0 };
            }

            let mut stack_size = 0_usize;

            while q_size > 0 {
                q_size -= 1;
                let index = alone[q_size] as usize;
                if (t2count[index] >> 2) == 1 {
                    let hash: u64 = t2hash[index];

                    //h012[0] = binary_fuse16_hash(0, hash, self);
                    h012[1] = self.binary_fuse16_hash(1, hash);
                    h012[2] = self.binary_fuse16_hash(2, hash);
                    h012[3] = self.binary_fuse16_hash(0, hash); // == h012[0];
                    h012[4] = h012[1];

                    let found: u8 = t2count[index] & 3;
                    reverse_h[stack_size] = found;
                    reverse_order[stack_size] = hash;
                    stack_size += 1;

                    let other_index1: u32 = h012[(found + 1) as usize];
                    alone[q_size] = other_index1;
                    q_size += if (t2count[other_index1 as usize] >> 2) == 2 {
                        1
                    } else {
                        0
                    };

                    t2count[other_index1 as usize] -= 4;
                    t2count[other_index1 as usize] ^= binary_fuse_mod3(found + 1);
                    t2hash[other_index1 as usize] ^= hash;

                    let other_index2: u32 = h012[(found + 2) as usize];
                    alone[q_size] = other_index2;
                    q_size += if (t2count[other_index2 as usize] >> 2) == 2 {
                        1
                    } else {
                        0
                    };
                    t2count[other_index2 as usize] -= 4;
                    t2count[other_index2 as usize] ^= binary_fuse_mod3(found + 2);
                    t2hash[other_index2 as usize] ^= hash;
                }
            }

            if stack_size == size {
                break; // success
            }

            reverse_order.fill(0);
            reverse_order[size] = 1; // sentinel
            t2count.fill(0);
            t2hash.fill(0);

            self.seed = binary_fuse_rng_splitmix64(&mut rng_counter);
        }

        for i in (0_usize..size).rev() {
            // the hash of the key we insert next
            let hash: u64 = reverse_order[i];
            let xor2: u16 = binary_fuse16_fingerprint(hash) as u16;
            let found: usize = reverse_h[i] as usize;
            h012[0] = self.binary_fuse16_hash(0, hash);
            h012[1] = self.binary_fuse16_hash(1, hash);
            h012[2] = self.binary_fuse16_hash(2, hash);
            h012[3] = h012[0];
            h012[4] = h012[1];

            self.fingerprints[h012[found] as usize] = xor2
                ^ self.fingerprints[h012[found + 1] as usize]
                ^ self.fingerprints[h012[found + 2] as usize];
        }

        Ok(())
    }

    #[inline]
    fn binary_fuse16_hash_batch(&self, hash: u64) -> BinaryHashes {
        let mut ans = BinaryHashes::default();

        ans.h0 = binary_fuse_mulhi(hash, self.segment_count_length.into()) as u32;
        ans.h1 = ans.h0 + self.segment_length;
        ans.h2 = ans.h1 + self.segment_length;
        ans.h1 ^= ((hash >> 18) as u32) & self.segment_length_mask;
        ans.h2 ^= (hash as u32) & self.segment_length_mask;
        ans
    }

    #[inline]
    fn binary_fuse16_hash(&self, index: u32, hash: u64) -> u32 {
        let mut h = binary_fuse_mulhi(hash, self.segment_count_length.into());
        h += (index * self.segment_length) as u64;
        // keep the lower 36 bits
        let hh = hash & ((1_u64 << 36) - 1);
        // index 0: right shift by 36; index 1: right shift by 18; index 2: no shift
        h ^= (hh >> (36 - 18 * index)) & (self.segment_length_mask as u64);

        h as u32
    }
}

#[inline]
fn binary_fuse16_fingerprint(hash: u64) -> u64 {
    hash ^ (hash >> 32)
}

#[inline]
fn binary_fuse_murmur64(mut h: u64) -> u64 {
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51afd7ed558ccd_u64);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ceb9fe1a85ec53_u64);
    h ^= h >> 33;
    h
}

#[inline]
fn binary_fuse_mix_split(key: u64, seed: u64) -> u64 {
    binary_fuse_murmur64(key.wrapping_add(seed))
}

#[inline]
// returns random number, modifies the seed
fn binary_fuse_rng_splitmix64(seed: &mut u64) -> u64 {
    *seed = seed.wrapping_add(0x9E3779B97F4A7C15_u64);
    let mut z = *seed;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9_u64);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB_u64);
    z ^ (z >> 31)
}

#[inline]
fn binary_fuse_mulhi(a: u64, b: u64) -> u64 {
    (((a as u128) * (b as u128)) >> 64) as u64
}

#[inline]
fn binary_fuse_calculate_segment_length(arity: u32, size: u32) -> u32 {
    let ln_size = (size as f64).ln();

    // These parameters are very sensitive. Replacing 'floor' by 'round' can
    // substantially affect the construction time.
    match arity {
        3 => 1_u32 << ((ln_size / 3.33_f64.ln() + 2.25).floor() as u32),
        4 => 1_u32 << ((ln_size / 2.91_f64.ln() - 0.50).floor() as u32),
        _ => 65536,
    }
}

#[inline]
fn binary_fuse8_max(a: f64, b: f64) -> f64 {
    if a < b {
        b
    } else {
        a
    }
}

#[inline]
fn binary_fuse_calculate_size_factor(arity: u32, size: u32) -> f64 {
    let ln_size = (size as f64).ln();
    match arity {
        3 => binary_fuse8_max(1.125, 0.875 + 0.250 * 1000000.0_f64.ln() / ln_size),
        4 => binary_fuse8_max(1.075, 0.770 + 0.305 * 0600000.0_f64.ln() / ln_size),
        _ => 2.0,
    }
}

#[inline]
fn binary_fuse_mod3(x: u8) -> u8 {
    if x > 2 {
        x - 3
    } else {
        x
    }
}

#[derive(Default)]
struct BinaryHashes {
    pub(crate) h0: u32,
    pub(crate) h1: u32,
    pub(crate) h2: u32,
}

#[cfg(test)]
mod tests {
    use rand::prelude::random;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use super::*;

    fn generate_unique_keys(rng: &mut StdRng, size: usize) -> Vec<u64> {
        let mut keys: Vec<u64> = Vec::with_capacity(size);
        keys.resize(size, u64::default());

        for key in keys.iter_mut() {
            *key = rng.gen();
        }
        keys.sort_unstable();
        keys.dedup();

        for _i in 0..(size - keys.len()) {
            let key = rng.gen::<u64>();
            if !keys.contains(&key) {
                keys.push(key)
            }
        }

        keys
    }

    fn test_fuse16_build(name: &str, seed: u64, size: u32) {
        let x = (size / 2) as usize;

        println!("test_fuse16_build<{}> size:{}", name, size);
        let mut rng = StdRng::seed_from_u64(seed);

        let keys = generate_unique_keys(&mut rng, size as usize);
        let (keys1, keys2) = (&keys[0..x], &keys[x..]);

        let mut builder = Fuse16Builder::default();
        builder.extend(keys1);

        for key in keys2 {
            builder.insert(key);
        }

        let filter = builder.build().expect("failed to build fuse16 filter");

        // contains api
        for key in keys.iter() {
            assert!(filter.contains(key), "key {} not present", key);
        }
        // contains_key api
        for key in keys.iter() {
            let digest = {
                let mut hasher = FuseHasher::default();
                key.hash(&mut hasher);
                hasher.finish()
            };
            assert!(filter.contains_digest(digest), "key {} not present", key);
        }

        // print some statistics
        let (falsesize, mut matches) = (10_000_000, 0_f64);
        let bpv = ((filter.fingerprints.len() * 2) as f64) * 8.0 / (keys.len() as f64);
        println!("test_fuse16_build<{}> bits per entry {} bits", name, bpv);
        if size > 100000 {
            assert!(bpv < 20.0, "bpv({}) >= 20.0", bpv);
        }

        for _ in 0..falsesize {
            if filter.contains(&rng.gen::<u64>()) {
                matches += 1_f64;
            }
        }

        let fpp = matches * 100.0 / (falsesize as f64);
        println!("test_fuse16_build<{}> false positive rate {}%", name, fpp);
        assert!(fpp < 0.40, "fpp({}) >= 0.40", fpp);
    }

    macro_rules! fuse_test {
        ($name:ident, $size:expr) => {
            #[test]
            fn $name() {
                let mut seed: u64 = random();
                println!("test_fuse16 seed:{},size:{}", seed, $size);

                let start = std::time::Instant::now();
                let size: u32 = $size;
                seed = seed.wrapping_add(size as u64);
                test_fuse16_build("Standard", seed, size);
                println!("Took: {:?}", start.elapsed());
            }
        };
    }

    fuse_test!(test_fuse16_size_1, 1);
    fuse_test!(test_fuse16_size_2, 2);
    fuse_test!(test_fuse16_size_10, 10);
    fuse_test!(test_fuse16_size_1_000, 1_000);
    fuse_test!(test_fuse16_size_10_000, 10_000);
    fuse_test!(test_fuse16_size_100_000, 100_000);
    fuse_test!(test_fuse16_size_1_000_000, 1_000_000);
    fuse_test!(test_fuse16_size_10_000_000, 10_000_000);

    #[test]
    #[ignore]
    fn test_fuse16_billion() {
        let seed: u64 = random();
        println!("test_fuse16_billion seed:{}", seed);

        let size = 1_000_000_000;
        test_fuse16_build("Standard 1B", seed, size);
    }
}
