use hashbrown::HashMap;

use super::field::DocField;


pub struct Document(pub HashMap<String, DocField>);