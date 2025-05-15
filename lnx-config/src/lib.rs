use std::any::{Any, TypeId};
use std::sync::Arc;

#[doc(hidden)]
pub mod access;
mod state;

#[derive(Debug, thiserror::Error)]
#[error("provided config type is already initialised")]
/// The provided config is already initialised in the global state.
pub struct ConfigAlreadyExists;

#[derive(Debug, thiserror::Error)]
#[error("provided config type is not initialised")]
/// The provided config is not initialised in the global state.
pub struct ConfigNotInitialisedExists;

/// Initialise a config in the global or thread local state for dependants to pull.
///
/// Errors if the config already exists within the state.
pub fn init<T>(config: T) -> Result<(), ConfigAlreadyExists>
where
    T: Any + Send + Sync + 'static,
{
    let type_id = TypeId::of::<T>();
    if state::exists(type_id) {
        return Err(ConfigAlreadyExists);
    }
    state::set_auto(type_id, Arc::new(config));
    Ok(())
}

#[cfg(feature = "thread-local")]
/// Initialise a config in the _global state only_ for dependants to pull.
///
/// Errors if the config already exists within the state.
pub fn init_global<T>(config: T) -> Result<(), ConfigAlreadyExists>
where
    T: Any + Send + Sync + 'static,
{
    let type_id = TypeId::of::<T>();
    if state::exists(type_id) {
        return Err(ConfigAlreadyExists);
    }
    state::set_global(type_id, Arc::new(config));
    Ok(())
}

/// Update an existing config.
///
/// This will trigger any watching callbacks for the given config if they are using [WatchMode::Live].
pub fn update<T>(config: T) -> Result<(), ConfigNotInitialisedExists>
where
    T: Any + Send + Sync + 'static,
{
    let type_id = TypeId::of::<T>();
    if !state::exists(type_id) {
        return Err(ConfigNotInitialisedExists);
    }
    state::set_auto(type_id, Arc::new(config));
    Ok(())
}

#[cfg(feature = "thread-local")]
/// Update an existing config in the thread-local state.
///
/// This will trigger any watching callbacks for the given config if they are using [WatchMode::Live].
pub fn update_global<T>(config: T) -> Result<(), ConfigNotInitialisedExists>
where
    T: Any + Send + Sync + 'static,
{
    let type_id = TypeId::of::<T>();
    if !state::exists(type_id) {
        return Err(ConfigNotInitialisedExists);
    }
    state::set_global(type_id, Arc::new(config));
    Ok(())
}

#[doc(hidden)]
/// Get an existing config if it is initialised.
pub fn get_config<T>() -> Option<Arc<T>>
where
    T: Any + Send + Sync + 'static,
{
    state::get(TypeId::of::<T>()).and_then(|v| v.downcast().ok())
}
