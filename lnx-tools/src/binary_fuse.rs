//! A binary fuse filter implementation
//!
//! This is taken from the https://github.com/prataprc/xorfilter repository but adds
//! support for rkyv.

use std::hash::Hash;
use std::hash::Hasher;

use anyhow::{bail, Result};
use cityhasher::CityHasher;
use rkyv::{Archive, Serialize, Deserialize};

// Probability of success should always be > 0.5 so 100 iterations is highly unlikely.
const XOR_MAX_ITERATIONS: usize = 100;

#[inline]
pub(crate) fn binary_fuse_murmur64(mut h: u64) -> u64 {
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51afd7ed558ccd_u64);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ceb9fe1a85ec53_u64);
    h ^= h >> 33;
    h
}

#[inline]
pub(crate) fn binary_fuse_mix_split(key: u64, seed: u64) -> u64 {
    binary_fuse_murmur64(key.wrapping_add(seed))
}

#[allow(dead_code)]
#[inline]
fn binary_fuse_rotl64(n: u64, c: u32) -> u64 {
    n.rotate_left(c)
}

#[allow(dead_code)]
#[inline]
fn binary_fuse_reduce(hash: u32, n: u32) -> u32 {
    // http://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
    (((hash as u64) * (n as u64)) >> 32) as u32
}

#[inline]
fn binary_fuse8_fingerprint(hash: u64) -> u64 {
    hash ^ (hash >> 32)
}

// returns random number, modifies the seed
pub(crate) fn binary_fuse_rng_splitmix64(seed: &mut u64) -> u64 {
    *seed = seed.wrapping_add(0x9E3779B97F4A7C15_u64);
    let mut z = *seed;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9_u64);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB_u64);
    z ^ (z >> 31)
}

#[inline]
pub(crate) fn binary_fuse_mulhi(a: u64, b: u64) -> u64 {
    (((a as u128) * (b as u128)) >> 64) as u64
}

#[inline]
pub(crate) fn binary_fuse_calculate_segment_length(arity: u32, size: u32) -> u32 {
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
pub(crate) fn binary_fuse_calculate_size_factor(arity: u32, size: u32) -> f64 {
    let ln_size = (size as f64).ln();
    match arity {
        3 => binary_fuse8_max(1.125, 0.875 + 0.250 * 1000000.0_f64.ln() / ln_size),
        4 => binary_fuse8_max(1.075, 0.770 + 0.305 * 0600000.0_f64.ln() / ln_size),
        _ => 2.0,
    }
}

#[inline]
pub(crate) fn binary_fuse_mod3(x: u8) -> u8 {
    if x > 2 {
        x - 3
    } else {
        x
    }
}

#[derive(Default, Archive, Serialize, Deserialize)]
pub struct Keys(#[with(rkyv::with::Raw)] Vec<u64>);

#[derive(Archive, Serialize, Deserialize)]
/// Type Fuse8 is probabilistic data-structure to test membership of an element in a set.
///
/// IMPORTANT: Fuse8 filter can only tolerate few duplicates in a given data-set.
/// So make sure to supply a hasher that is capable of generating unique digests,
/// _(with allowed tolerance of duplicates)_ and while supplying the digests directly
/// via `populate_keys()` and `build_keys()` make sure they don't have more than few
/// duplicates.
pub struct Fuse8 {
    keys: Option<Keys>,
    pub seed: u64,
    pub num_keys: Option<usize>,
    pub segment_length: u32,
    pub segment_length_mask: u32,
    pub segment_count: u32,
    pub segment_count_length: u32,
    #[with(rkyv::with::Raw)]
    pub finger_prints: Vec<u8>,
}

#[derive(Default)]
pub(crate) struct BinaryHashes {
    pub(crate) h0: u32,
    pub(crate) h1: u32,
    pub(crate) h2: u32,
}

impl Clone for Fuse8 {
    fn clone(&self) -> Self {
        Fuse8 {
            keys: Some(Keys::default()),
            seed: self.seed,
            num_keys: self.num_keys,
            segment_length: self.segment_length,
            segment_length_mask: self.segment_length_mask,
            segment_count: self.segment_count,
            segment_count_length: self.segment_count_length,
            finger_prints:self.finger_prints.clone(),
        }
    }
}

impl Fuse8 {
    #[inline]
    fn binary_fuse8_hash_batch(&self, hash: u64) -> BinaryHashes {
        let mut ans = BinaryHashes::default();

        ans.h0 = binary_fuse_mulhi(hash, self.segment_count_length.into()) as u32;
        ans.h1 = ans.h0 + self.segment_length;
        ans.h2 = ans.h1 + self.segment_length;
        ans.h1 ^= ((hash >> 18) as u32) & self.segment_length_mask;
        ans.h2 ^= (hash as u32) & self.segment_length_mask;
        ans
    }

    #[inline]
    fn binary_fuse8_hash(&self, index: u32, hash: u64) -> u32 {
        let mut h = binary_fuse_mulhi(hash, self.segment_count_length.into());
        h += (index * self.segment_length) as u64;
        // keep the lower 36 bits
        let hh = hash & ((1_u64 << 36) - 1);
        // index 0: right shift by 36; index 1: right shift by 18; index 2: no shift
        h ^= (hh >> (36 - 18 * index)) & (self.segment_length_mask as u64);

        h as u32
    }
}

impl Fuse8 {
    /// New Fuse8 instance that can index size number of keys. Internal data-structures
    /// are pre-allocated for `size`.  `size` should be at least 2.
    pub fn new(size: u32) -> Self {
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
            let n = ((cap + segment_length - 1) / segment_length).wrapping_sub(arity - 1);
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

        Fuse8 {
            keys: Some(Keys::default()),
            seed: u64::default(),
            num_keys: None,
            segment_length,
            segment_length_mask,
            segment_count,
            segment_count_length,
            finger_prints: vec![0; array_length as usize],
        }
    }
}

impl Fuse8 {
    /// Return the size of index.
    #[inline]
    pub fn size_of(&self) -> usize {
        std::mem::size_of::<Self>() + self.finger_prints.len()
    }

    /// Insert 64-bit digest of a single key. Digest for the key shall be generated
    /// using the default-hasher or via hasher supplied via [Fuse8::with_hasher] method.
    pub fn insert<K: ?Sized + Hash>(&mut self, key: &K) {
        let digest = {
            let mut hasher = CityHasher::new();
            key.hash(&mut hasher);
            hasher.finish()
        };
        if let Some(x) = self.num_keys.as_mut() {
            *x += 1
        }
        self.keys.as_mut().unwrap().0.push(digest);
    }

    /// Populate with 64-bit digests for a collection of keys of type `K`. Digest for
    /// key shall be generated using the default-hasher or via hasher supplied
    /// via [Fuse8::with_hasher] method.
    pub fn populate<K: Hash>(&mut self, keys: &[K]) {
        if let Some(x) = self.num_keys.as_mut() {
            *x += keys.len()
        }
        keys.iter().for_each(|key| {
            let mut hasher = CityHasher::new();
            key.hash(&mut hasher);
            self.keys.as_mut().unwrap().0.push(hasher.finish());
        })
    }

    /// Populate with pre-compute collection of 64-bit digests.
    pub fn populate_keys(&mut self, digests: &[u64]) {
        if let Some(x) = self.num_keys.as_mut() {
            *x += digests.len()
        }
        self.keys.as_mut().unwrap().0.extend_from_slice(digests);
    }

    // construct the filter, returns true on success, false on failure.
    // most likely, a failure is due to too high a memory usage
    // size is the number of keys
    // The caller is responsable for calling binary_fuse8_allocate(size,filter)
    // before. The caller is responsible to ensure that there are no duplicated
    // keys. The inner loop will run up to XOR_MAX_ITERATIONS times (default on
    // 100), it should never fail, except if there are duplicated keys. If it fails,
    // a return value of false is provided.
    /// Build bitmap for keys that where previously inserted using [Fuse8::insert],
    /// [Fuse8::populate] and [Fuse8::populate_keys] method.
    pub fn build(&mut self) -> Result<()> {
        match self.keys.take() {
            Some(keys) => self.build_keys(&keys.0),
            None => Ok(()),
        }
    }

    /// Build a bitmap for pre-computed 64-bit digests for keys. If keys where
    /// previously inserted using [Fuse8::insert] or [Fuse8::populate] or
    /// [Fuse8::populate_keys] methods, they shall be ignored.
    ///
    /// It is upto the caller to ensure that digests are unique, that there no
    /// duplicates.
    pub fn build_keys(&mut self, digests: &[u64]) -> Result<()> {
        let mut rng_counter = 0x726b2b9d438b9d4d_u64;
        let capacity = self.finger_prints.len();
        let size = digests.len();

        self.num_keys = Some(digests.len());
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
                bail!("Too many iterations when building filter. Are all your keys unique?");
            }

            for i in 0_u32..block {
                // important : i * size would overflow as a 32-bit number in some
                // cases.
                start_pos[i as usize] =
                    (((i as u64) * (size as u64)) >> block_bits) as u32;
            }

            let mask_block = (block - 1) as u64;
            for (_, digest) in digests.iter().enumerate().take(size) {
                let hash: u64 = binary_fuse_murmur64(digest.wrapping_add(self.seed));
                let mut segment_index: u64 = hash >> (64 - block_bits);
                while reverse_order[start_pos[segment_index as usize] as usize] != 0 {
                    segment_index += 1;
                    segment_index &= mask_block;
                }
                reverse_order[start_pos[segment_index as usize] as usize] = hash;
                start_pos[segment_index as usize] += 1;
            }

            let mut error: isize = 0;
            let mut duplicates = 0;
            for (_, rev_order) in reverse_order.iter().enumerate().take(size) {
                let hash: u64 = *rev_order;

                let h0: usize = self.binary_fuse8_hash(0, hash) as usize;
                t2count[h0] = t2count[h0].wrapping_add(4);
                t2hash[h0] ^= hash;

                let h1: usize = self.binary_fuse8_hash(1, hash) as usize;
                t2count[h1] = t2count[h1].wrapping_add(4);
                t2count[h1] ^= 1;
                t2hash[h1] ^= hash;

                let h2: usize = self.binary_fuse8_hash(2, hash) as usize;
                t2count[h2] = t2count[h2].wrapping_add(4);
                t2hash[h2] ^= hash;
                t2count[h2] ^= 2;

                // If we have duplicated hash values, then it is likely that
                // the next comparison is true
                if (t2hash[h0] & t2hash[h1] & t2hash[h2]) == 0 {
                    // next we do the actual test
                    if ((t2hash[h0] == 0) && (t2count[h0] == 8))
                        || ((t2hash[h1] == 0) && (t2count[h1] == 8))
                        || ((t2hash[h2] == 0) && (t2count[h2] == 8))
                    {
                        duplicates += 1;
                        t2count[h0] = t2count[h0].wrapping_sub(4);
                        t2hash[h0] ^= hash;
                        t2count[h1] = t2count[h1].wrapping_sub(4);
                        t2count[h1] ^= 1;
                        t2hash[h1] ^= hash;
                        t2count[h2] = t2count[h2].wrapping_sub(4);
                        t2hash[h2] ^= hash;
                        t2count[h2] ^= 2;
                    }
                }

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

                    //h012[0] = self.binary_fuse8_hash(0, hash);
                    h012[1] = self.binary_fuse8_hash(1, hash);
                    h012[2] = self.binary_fuse8_hash(2, hash);
                    h012[3] = self.binary_fuse8_hash(0, hash); // == h012[0];
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

            if (stack_size + duplicates) == size {
                break; // success
            }

            reverse_order.fill(0);
            reverse_order[size] = 1; // sentinel
            t2count.fill(0);
            t2hash.fill(0);

            self.seed = binary_fuse_rng_splitmix64(&mut rng_counter);
        }

        if size == 0 {
            return Ok(());
        }

        for i in (0_usize..size).rev() {
            // the hash of the key we insert next
            let hash: u64 = reverse_order[i];
            let xor2: u8 = binary_fuse8_fingerprint(hash) as u8;
            let found: usize = reverse_h[i] as usize;
            h012[0] = self.binary_fuse8_hash(0, hash);
            h012[1] = self.binary_fuse8_hash(1, hash);
            h012[2] = self.binary_fuse8_hash(2, hash);
            h012[3] = h012[0];
            h012[4] = h012[1];
            self.finger_prints[h012[found] as usize] = xor2
                ^ self.finger_prints[h012[found + 1] as usize]
                ^ self.finger_prints[h012[found + 2] as usize];
        }

        Ok(())
    }
}

impl Fuse8 {
    #[inline]
    /// Return the number of keys added/built into the bitmap index.
    pub fn len(&self) -> Option<usize> {
        self.num_keys
    }

    #[inline]
    /// Returns if the filter is empty or not.
    pub fn is_empty(&self) -> bool {
        self.num_keys.map(|v| v == 0).unwrap_or_default()
    }

    /// Contains tell you whether the key is likely part of the set, with false
    /// positive rate.
    pub fn contains<K: ?Sized + Hash>(&self, key: &K) -> bool {
        let digest = {
            let mut hasher = CityHasher::new();
            key.hash(&mut hasher);
            hasher.finish()
        };
        self.contains_key(digest)
    }

    /// Contains tell you whether the key, as pre-computed digest form, is likely
    /// part of the set, with false positive rate.
    pub fn contains_key(&self, digest: u64) -> bool {
        let hash = binary_fuse_mix_split(digest, self.seed);
        let mut f = binary_fuse8_fingerprint(hash) as u8;
        let BinaryHashes { h0, h1, h2 } = self.binary_fuse8_hash_batch(hash);
        f ^= self.finger_prints[h0 as usize]
            ^ self.finger_prints[h1 as usize]
            ^ self.finger_prints[h2 as usize];
        f == 0
    }
}


#[cfg(test)]
mod tests {
    use std::collections::hash_map::RandomState;
    use std::hash::BuildHasher;

    use rand::distributions::Distribution;
    use rand::distributions::Standard;
    use rand::prelude::random;
    use rand::rngs::StdRng;
    use rand::Rng;
    use rand::SeedableRng;

    use super::*;

    fn generate_unique_keys<K>(prefix: &str, rng: &mut StdRng, size: usize) -> Vec<K>
    where
        K: Clone + Default + Ord,
        Standard: Distribution<K>,
    {
        let mut keys: Vec<K> = Vec::with_capacity(size);
        keys.resize(size, K::default());

        for key in keys.iter_mut() {
            *key = rng.gen();
        }
        keys.sort_unstable();

        let mut ks = keys.clone();
        ks.dedup();
        println!("{} number of duplicates {}", prefix, size - ks.len());

        keys
    }

    fn test_fuse8_build<K>(name: &str, seed: u64, size: u32)
    where
        K: Clone + Default + Ord + Hash + std::fmt::Display,
        Standard: Distribution<K>,
    {
        use std::cmp;

        let mut rng = StdRng::seed_from_u64(seed);

        let keys = generate_unique_keys(name, &mut rng, size as usize);

        let size = keys.len() as u32;
        let (x, y) = {
            let size = size as usize;
            (size / 3, size / 3)
        };
        let (keys1, keys2, keys3) = (&keys[0..x], &keys[x..x + y], &keys[x + y..]);

        println!("test_fuse8_build<{}> size:{}", name, size);

        let mut filter = Fuse8::new(size);

        // populate api
        filter.populate(keys1);
        // populate_keys api
        let digests: Vec<u64> = keys2
            .iter()
            .map(|k| {
                let mut hasher = CityHasher::new();
                k.hash(&mut hasher);
                hasher.finish()
            })
            .collect();
        filter.populate_keys(&digests);
        // insert api
        keys3.iter().for_each(|key| filter.insert(key));

        filter.build().expect("failed to build fuse16 filter");

        // contains api
        for key in keys.iter() {
            assert!(filter.contains(key), "key {} not present", key);
        }
        // contains_key api
        for key in keys.iter() {
            let digest = {
                let mut hasher = CityHasher::new();
                key.hash(&mut hasher);
                hasher.finish()
            };
            assert!(filter.contains_key(digest), "key {} not present", key);
        }

        // print some statistics
        let (falsesize, mut matches) = (cmp::min(size * 10, 10_000_000), 0_f64);
        let bpv = (filter.finger_prints.len() as f64) * 8.0 / (keys.len() as f64);
        println!("test_fuse8_build<{}> bits per entry {} bits", name, bpv);

        for _ in 0..falsesize {
            let k = rng.gen::<K>();
            let ok = filter.contains(&k);
            match keys.binary_search(&k) {
                Ok(_) if !ok => panic!("false negative {}", k),
                Ok(_) => (),
                Err(_) if ok => matches += 1_f64,
                Err(_) => (),
            }
        }

        let fpp = matches * 100.0 / (falsesize as f64);
        println!("test_fuse8_build<{}> false positive rate {}%", name, fpp);

        if size > 100_000 {
            assert!(bpv < 12.0, "bpv({}) >= 12.0", bpv);
            assert!(fpp < 0.4, "fpp({}) >= 0.4", fpp);
        }
    }

    fn test_fuse8_build_keys<H, K>(name: &str, seed: u64, size: u32)
    where
        H: Default + BuildHasher,
        K: Clone + Default + Ord + Hash + std::fmt::Display,
        Standard: Distribution<K>,
    {
        use std::cmp;

        let mut rng = StdRng::seed_from_u64(seed);

        let keys = generate_unique_keys(name, &mut rng, size as usize);
        let size = keys.len() as u32;

        println!("test_fuse8_build_keys<{}> size:{}", name, size);

        let mut filter = Fuse8::new(size);

        // build_keys api
        let digests: Vec<u64> = keys
            .iter()
            .map(|k| {
                let mut hasher = CityHasher::new();
                k.hash(&mut hasher);
                hasher.finish()
            })
            .collect();

        filter.build_keys(&digests).expect("failed to build fuse16 filter");

        // contains api
        for key in keys.iter() {
            assert!(filter.contains(key), "key {} not present", key);
        }
        // contains_key api
        for digest in digests.into_iter() {
            assert!(filter.contains_key(digest), "digest {} not present", digest);
        }

        // print some statistics
        let (falsesize, mut matches) = (cmp::min(size * 10, 10_000_000), 0_f64);
        let bpv = (filter.finger_prints.len() as f64) * 8.0 / (keys.len() as f64);
        println!(
            "test_fuse8_build_keys<{}> bits per entry {} bits",
            name, bpv
        );

        for _ in 0..falsesize {
            let k = rng.gen::<K>();
            let ok = filter.contains(&k);
            match keys.binary_search(&k) {
                Ok(_) if !ok => panic!("false negative {}", k),
                Ok(_) => (),
                Err(_) if ok => matches += 1_f64,
                Err(_) => (),
            }
        }

        let fpp = matches * 100.0 / (falsesize as f64);
        println!(
            "test_fuse8_build_keys<{}> false positive rate {}%",
            name, fpp
        );

        if size > 100_000 {
            assert!(bpv < 12.0, "bpv({}) >= 12.0", bpv);
            assert!(fpp < 0.4, "fpp({}) >= 0.4", fpp);
        }
    }

    #[test]
    fn test_fuse8_u8() {
        let mut seed: u64 = [6509898893809465102_u64, random()][random::<usize>() % 2];
        println!("test_fuse8_u8 seed:{}", seed);

        for size in [0, 1, 2, 10, 100].iter() {
            seed = seed.wrapping_add(*size as u64);
            test_fuse8_build::<u8>("RandomState,u8", seed, *size);
            test_fuse8_build_keys::<u8>("RandomState,u8", seed, *size);
        }
    }

    #[test]
    fn test_fuse8_u16() {
        let mut seed: u64 = random();
        println!("test_fuse8_u16 seed:{}", seed);

        for size in [0, 1, 2, 10, 100, 500].iter() {
            seed = seed.wrapping_add(*size as u64);
            test_fuse8_build::<u16>("RandomState,16", seed, *size);
            test_fuse8_build_keys::<u16>("RandomState,16", seed, *size);
        }
    }

    #[test]
    fn test_fuse8_u64() {
        let mut seed: u64 = random();
        println!("test_fuse8_u64 seed:{}", seed);

        for size in [0, 1, 2, 10, 1000, 10_000, 100_000, 1_000_000, 10_000_000].iter() {
            seed = seed.wrapping_add(*size as u64);
            test_fuse8_build::<u64>("RandomState,64", seed, *size);
            test_fuse8_build_keys::<u64>("RandomState,64", seed, *size);
        }
    }

    #[test]
    fn test_fuse8_duplicates() {
        println!("test_fuse8_duplicates");

        let keys = vec![102, 123, 1242352, 12314, 124235, 1231234, 12414, 1242352];

        let mut filter = Fuse8::new(keys.len() as u32);

        filter.build_keys(&keys).expect("build with duplicate keys failed");

        // contains api
        for key in keys.iter() {
            assert!(filter.contains_key(*key), "key {} not present", key);
        }
    }

    #[test]
    #[ignore]
    fn test_fuse8_billion() {
        let seed: u64 = random();
        println!("test_fuse8_billion seed:{}", seed);

        let size = 1_000_000_000;
        test_fuse8_build::<u64>("RandomState,u64", seed, size);
        test_fuse8_build_keys::<u64>("RandomState,u64", seed, size);
    }
}