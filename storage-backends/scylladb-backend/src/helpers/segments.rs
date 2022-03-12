use lnx_common::configuration::SEGMENT_SIZE;

pub fn get_range(segment: i64) -> (i64, i64) {
    let start = if segment < 0 { i64::MIN + 1 } else { 0 };

    let start = start + (segment * SEGMENT_SIZE);
    (start, start + SEGMENT_SIZE)
}
