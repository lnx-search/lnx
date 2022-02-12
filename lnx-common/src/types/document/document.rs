use hashbrown::HashMap;

use super::field::DocField;

#[derive(Debug, Clone)]
pub struct Document(pub HashMap<String, DocField>);