use std::borrow::Cow;


#[derive(Clone)]
pub struct IndexContext {
    name: Cow<'static, String>,
}

impl IndexContext {
    #[inline]
    pub fn id(&self) -> u64 {
        crc32fast::hash(self.name.as_ref()) as u64
    }
}
