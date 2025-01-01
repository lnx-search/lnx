use std::cmp::Ordering;

#[derive(Debug, Clone)]
pub struct Suggestion {
    pub term: Vec<u8>,
    pub distance: usize,
    pub count: usize,
}

impl Suggestion {
    pub fn empty() -> Suggestion {
        Suggestion {
            term: Vec::new(),
            distance: 0,
            count: 0,
        }
    }

    pub fn new(term: impl AsRef<[u8]>, distance: usize, count: usize) -> Suggestion {
        Suggestion {
            term: term.as_ref().to_vec(),
            distance,
            count,
        }
    }
}

impl Ord for Suggestion {
    fn cmp(&self, other: &Suggestion) -> Ordering {
        let distance_cmp = self.distance.cmp(&other.distance);
        if distance_cmp == Ordering::Equal {
            return self.count.cmp(&other.count);
        }
        distance_cmp
    }
}

impl PartialOrd for Suggestion {
    fn partial_cmp(&self, other: &Suggestion) -> Option<Ordering> {
        Some(self.cmp(other)) 
    }
}

impl PartialEq for Suggestion {
    fn eq(&self, other: &Suggestion) -> bool {
        // self.term == other.term
        self.distance == other.distance && self.count == other.count
    }
}

impl Eq for Suggestion {}
