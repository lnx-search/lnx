use tantivy::schema::Field;

#[derive(Debug, Clone)]
pub struct Filter {
    pub field: Field,
    pub op: FilterOp,

    pub and: Option<Box<Filter>>,
    pub or: Option<Box<Filter>>,
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

    pub fn and(&mut self, other: Filter) {
        self.and = Some(Box::new(other));
    }

    pub fn or(&mut self, other: Filter) {
        self.or = Some(Box::new(other))
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