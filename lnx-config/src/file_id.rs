//! The file_ids refer to a unique identifier of a file in the VFS.
//!
//! We reserve certain ranges for different systems to allow us to optimise certain storage
//! patterns.
//!
//! In particular, we split the 64-bit integer into:
//!
//! - `10` bits are the "family" of the value.
//! - The remaining `54` bits are up to the group to use.
//!

const FAMILY_ID_CLEAR_MASK: u64 = 0x00_3F_FF_FF_FF_FF_FF_FF;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
/// The "family" of files the ID belongs to.
pub enum Family {
    /// System information files.
    System,
    /// Index specific files.
    Index,
}

mod assignments {
    pub const FAMILY_SYSTEM: u64 = 0;
    pub const FAMILY_INDEX: u64 = 10;
}

/// Unpack the given file ID, returning the family and actual value
/// stored within the ID if applicable.
///
/// Returns `None` if the ID is invalid.
pub fn unpack_id(file_id: u64) -> Option<(Family, u64)> {
    let raw_family_id = file_id >> 54;
    let raw_value = file_id & FAMILY_ID_CLEAR_MASK;
    map_raw_to_family(raw_family_id).map(|family| (family, raw_value))
}

/// Pack the given family and value together forming the file ID.
///
/// Panics if the value exceeds the 54 bit allowance.
pub fn pack_id(family: Family, value: u64) -> u64 {
    assert!(value <= (1 << 53), "value too large");
    let raw_family_id = map_family_to_raw(family);
    raw_family_id << 54 | value
}

fn map_raw_to_family(raw_id: u64) -> Option<Family> {
    match raw_id {
        assignments::FAMILY_SYSTEM => Some(Family::System),
        assignments::FAMILY_INDEX => Some(Family::Index),
        _ => None,
    }
}

fn map_family_to_raw(family: Family) -> u64 {
    match family {
        Family::System => assignments::FAMILY_SYSTEM,
        Family::Index => assignments::FAMILY_INDEX,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[rstest::rstest]
    fn test_pack_and_unpack_ids(
        #[values(Family::System, Family::Index)] family: Family,
        #[values(0, 10, 1 << 32, 1 << 53)] value: u64,
    ) {
        let packed = pack_id(family, value);
        let (retrieved_family, retrieved_value) =
            unpack_id(packed).expect("family should be unpacked");
        assert_eq!(retrieved_family, family);
        assert_eq!(retrieved_value, value);
    }

    #[should_panic(expected = "value too large")]
    #[test]
    fn test_pack_and_unpack_ids_too_large() {
        pack_id(Family::Index, 1 << 54);
    }
}
