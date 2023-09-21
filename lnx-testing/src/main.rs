use std::hash::{Hash, Hasher};

use anyhow::Result;
use cityhasher::CityHasher;
use lnx_tools::binary_fuse::{Fuse16, Fuse16Builder};
use mimalloc::MiMalloc;
use rand::rngs::StdRng;
use rand::{random, Rng, SeedableRng};

#[global_allocator]
static ALLOC: MiMalloc = MiMalloc;

const NUM_THREADS: usize = 8;

fn main() -> Result<()> {
    let mut seed: u64 = random();

    let start = std::time::Instant::now();
    let size: u32 = 10_000_000;
    seed = seed.wrapping_add(size as u64);
    test_fuse16_build("Standard", seed, size);
    println!("Total Took: {:?}", start.elapsed());

    Ok(())
}

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
    println!("test_fuse16_build<{}> size:{}", name, size);
    let mut rng = StdRng::seed_from_u64(seed);

    let keys = generate_unique_keys(&mut rng, size as usize);

    let start = std::time::Instant::now();
    let mut builder = Fuse16Builder::default();
    builder.extend(keys.iter());

    let filter = builder.build().expect("failed to build fuse16 filter");
    println!("Building took: {:?}", start.elapsed());

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
        assert!(filter.contains_digest(digest), "key {} not present", key);
    }

    // print some statistics
    let (falsesize, mut matches) = (10_000_000, 0_f64);
    let bpv = ((filter.fingerprints().len() * 2) as f64) * 8.0 / (keys.len() as f64);
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
