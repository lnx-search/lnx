use std::hash::Hash;
use tantivy::schema::Field;

#[derive(Debug, Clone)]
pub struct Filter {
    pub field: Field,
    pub op: FilterOp,

    pub and: Option<Vec<Filter>>,
    pub or: Option<Vec<Filter>>,
}

impl Filter {
    pub fn new(field: Field, op: FilterOp) -> Self {
        Self {
            field,
            op,

            and: None,
            or: None,
        }
    }

    pub fn and(&mut self, other: Vec<Filter>) {
        self.and = Some(other);
    }

    pub fn or(&mut self, other: Vec<Filter>) {
        self.or = Some(other)
    }
}


#[derive(Debug, Clone)]
pub enum FilterOp {
    Eq(u64),
    Ne(u64),
    Le(u64),
    Lt(u64),
    Ge(u64),
    Gt(u64),
    In(AdaptableSet),
}

#[derive(Debug, Clone)]
pub enum AdaptableSet {
    Small(Vec<u64>),
    Large(hashbrown::HashSet<u64>),
}