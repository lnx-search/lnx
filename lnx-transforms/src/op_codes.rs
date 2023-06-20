use crate::type_cast::TypeCast;

pub enum TransformOp {
    /// Loads a value with a key from the object on the stack.
    ///
    /// If the key does not exist an error is returned.
    Load { key: String, keep_object: bool },
    /// Loads a value with a key from the object on the stack.
    ///
    /// If the value does not exist,the system will load a `null`
    /// inplace of the missing value.
    LoadOptional { key: String, keep_object: bool },
    /// Stores the value as a given entry in the resulting fields with a given key.
    Store { as_key: String },
    /// Rejects a field if it's value is `null`.
    RejectNull,
    /// Casts the value to a desired type.
    Cast { ty: TypeCast },
}

impl TransformOp {
    /// Load a value in the stack with a given key.
    ///
    /// If the key does not exist the syste will reject the doc.
    pub fn load(key: impl Into<String>) -> Self {
        Self::Load {
            key: key.into(),
            keep_object: true,
        }
    }

    /// Load a value in the stack with a given key.
    ///
    /// If the key does not exist the system treats it as `missing`
    /// and will skip any ops on the value.
    pub fn load_opt(key: impl Into<String>) -> Self {
        Self::LoadOptional {
            key: key.into(),
            keep_object: true,
        }
    }

    /// Stores the top value in the stack into the output vec with a given key.
    pub fn store(as_key: impl Into<String>) -> Self {
        Self::Store {
            as_key: as_key.into(),
        }
    }

    /// Reject the top value in the stack if it is null.
    pub fn reject_null() -> Self {
        Self::RejectNull
    }

    /// Casts the top value in the stack to a given type or reject the doc.
    pub fn cast(ty: TypeCast) -> Self {
        Self::Cast { ty }
    }

    /// Don't re-add the object to the stack if it's a load op.
    pub fn drop_object(mut self) -> Self {
        match &mut self {
            Self::Load { keep_object, .. } => {
                (*keep_object) = false;
            },
            Self::LoadOptional { keep_object, .. } => {
                (*keep_object) = false;
            },
            _ => {},
        };

        self
    }
}
