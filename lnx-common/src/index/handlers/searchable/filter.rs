use std::hash::Hash;
use tantivy::schema::Field;

#[derive(Debug, Clone)]
pub struct Filter {
    pub field: Field,
    pub op: FilterOp
}


#[derive(Debug, Clone)]
pub enum FilterOp {
    Eq(f64),
    Ne(f64),
    Le(f64),
    Lt(f64),
    Ge(f64),
    Gt(f64),
    In(AdaptableSet),
}

#[derive(Debug, Clone)]
pub enum AdaptableSet {
    Small(Vec<f64>),
    Large(hashbrown::HashSet<u64>),
}