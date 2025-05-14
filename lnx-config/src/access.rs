use std::ops::Deref;

#[macro_export]
/// Get a config parameter.
macro_rules! get {
    ($cfg:ident) => {
        $crate::get_config::<$cfg>()
            .unwrap_or_else(|| panic!("config {} has not be initialised yet", stringify!($cfg)))
    };
    ($cfg:ident.$attr:ident) => {
        $crate::get_config::<$cfg>()
            .map(|c| unsafe {
                Accessed::__with_value_and_parent(c.clone(), &c.$attr)
            })
            .unwrap_or_else(|| panic!("config {} has not be initialised yet", stringify!($cfg)))
    };    
}

/// A wrapper around a config value and an access value.
/// 
/// This is to allow it to hold onto the parent value which owns the data.
pub struct Accessed<C, T> 
where 
    C: 'static,
    T: 'static,
{
    #[allow(unused)]
    inner: C,
    value: &'static T, 
}

impl<C, T> Accessed<C, T> {
    #[doc(hidden)]
    /// DO NOT USE!!
    pub unsafe fn __with_value_and_parent(parent: C, value: &T) -> Accessed<C, T> {
        Self {
            inner: parent,
            value: unsafe { std::mem::transmute::<&T, &'static T>(value) },
        }
    }
}

impl<C, T> Deref for Accessed<C, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.value
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    
    struct Test {
        test: String,
    }
    
    #[test]
    fn foo() {
        let x = get!(Test);
        let foo = get!(Test.test);
    }
}