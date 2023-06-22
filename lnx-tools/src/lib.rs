const CITY_SEED_1: u64 = 7_000_993_739_526_508_814;
const CITY_SEED_2: u64 = 11_782_341_770_808_795_856;

#[inline]
pub fn cityhash<H: AsRef<[u8]>>(v: H) -> u64 {
    cityhash_sys::city_hash_64_with_seeds(v.as_ref(), CITY_SEED_1, CITY_SEED_2)
}
