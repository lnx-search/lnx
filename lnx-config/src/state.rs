use std::any::{Any, TypeId};
use std::sync::Arc;
use parking_lot::RwLock;

type DynConfig = Arc<dyn Any + Send + Sync>;
type State = RwLock<ahash::HashMap<TypeId, DynConfig>>;


mod global_state {
    use std::sync::OnceLock;
    use super::*;

    static GLOBAL_STATE: OnceLock<State> = OnceLock::new();
    
    pub(super) fn set(cfg: DynConfig) {
        let type_id = cfg.type_id();
        let state = GLOBAL_STATE.get_or_init(State::default);
        state.write().insert(type_id, cfg);
    }
    
    pub(super) fn exists(type_id: TypeId) -> bool {
        let state = GLOBAL_STATE.get_or_init(State::default);
        state.read().contains_key(&type_id)        
    }
    
    pub(super) fn get(type_id: TypeId) -> Option<DynConfig> {
        let state = GLOBAL_STATE.get_or_init(State::default);
        state.read().get(&type_id).cloned()
    }
}

#[cfg(feature = "thread-local")]
mod thread_local_state {
    use std::sync::OnceLock;
    use super::*;

    thread_local! {
        static GLOBAL_STATE: OnceLock<State> = OnceLock::new();
    }
    
    pub(super) fn set(cfg: DynConfig) {
        let type_id = cfg.type_id();
        GLOBAL_STATE.with(|s| {
            let state = s.get_or_init(State::default);
            state.write().insert(type_id, cfg);
        })
    }

    pub(super) fn exists(type_id: TypeId) -> bool {
        GLOBAL_STATE.with(|s| {
            let state = s.get_or_init(State::default);
            state.read().contains_key(&type_id)
        })
    }

    pub(super) fn get(type_id: TypeId) -> Option<DynConfig> {
        GLOBAL_STATE.with(|s| {
            let state = s.get_or_init(State::default);
            state.read().get(&type_id).cloned()
        })
    }
}

pub(crate) fn set_auto(cfg: DynConfig) {
    #[cfg(feature = "thread-local")]
    thread_local_state::set(cfg);
    #[cfg(not(feature = "thread-local"))]
    set_global(cfg);
}

pub(crate) fn set_global(cfg: DynConfig) {
    global_state::set(cfg)
}

pub(crate) fn exists(type_id: TypeId) -> bool {
    #[cfg(feature = "thread-local")]
    {
        let exists_local = thread_local_state::exists(type_id);
        let exists_global = global_state::exists(type_id);
        exists_local || exists_global
    }

    #[cfg(not(feature = "thread-local"))]
    {
        global_state::exists(type_id)
    }
}

pub(crate) fn get(type_id: TypeId) -> Option<DynConfig> {
    #[cfg(feature = "thread-local")]
    {
        thread_local_state::get(type_id)
            .or_else(|| global_state::get(type_id))
    }
    
    #[cfg(not(feature = "thread-local"))]
    global_state::get(type_id)
}