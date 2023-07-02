pub mod files;
pub mod supervisor;

const CITY_SEED_1: u64 = 7_000_993_739_526_508_814;

#[inline(always)]
pub fn cityhash<H: AsRef<[u8]>>(v: H) -> u64 {
    cityhasher::hash_with_seed(v, CITY_SEED_1)
}
