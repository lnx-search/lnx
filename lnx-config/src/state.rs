use std::any::{Any, TypeId};
use std::sync::Arc;

use parking_lot::RwLock;

type DynConfig = Arc<dyn Any + Send + Sync>;

struct StateEntry {
    entry: DynConfig,
}

#[derive(Default)]
struct State(RwLock<foldhash::HashMap<TypeId, StateEntry>>);

impl State {
    fn exists(&self, type_id: TypeId) -> bool {
        self.0.read().contains_key(&type_id)
    }

    fn set(&self, type_id: TypeId, cfg: DynConfig) {
        self.0
            .write()
            .entry(type_id)
            .and_modify(|e| {
                e.entry = cfg.clone();
            })
            .or_insert_with(|| StateEntry { entry: cfg });
    }

    fn get(&self, type_id: TypeId) -> Option<DynConfig> {
        self.0.read().get(&type_id).map(|e| e.entry.clone())
    }
}

mod global_state {
    use std::sync::OnceLock;

    use super::*;

    static GLOBAL_STATE: OnceLock<State> = OnceLock::new();

    pub(super) fn set(type_id: TypeId, cfg: DynConfig) {
        let state = GLOBAL_STATE.get_or_init(State::default);
        state.set(type_id, cfg)
    }

    pub(super) fn exists(type_id: TypeId) -> bool {
        let state = GLOBAL_STATE.get_or_init(State::default);
        state.exists(type_id)
    }

    pub(super) fn get(type_id: TypeId) -> Option<DynConfig> {
        let state = GLOBAL_STATE.get_or_init(State::default);
        state.get(type_id)
    }
}

#[cfg(feature = "thread-local")]
mod thread_local_state {
    use std::sync::OnceLock;

    use super::*;

    thread_local! {
        static GLOBAL_STATE: OnceLock<State> = const { OnceLock::new() };
    }

    pub(super) fn set(type_id: TypeId, cfg: DynConfig) {
        GLOBAL_STATE.with(|s| {
            let state = s.get_or_init(State::default);
            state.set(type_id, cfg)
        })
    }

    pub(super) fn exists(type_id: TypeId) -> bool {
        GLOBAL_STATE.with(|s| {
            let state = s.get_or_init(State::default);
            state.exists(type_id)
        })
    }

    pub(super) fn get(type_id: TypeId) -> Option<DynConfig> {
        GLOBAL_STATE.with(|s| {
            let state = s.get_or_init(State::default);
            state.get(type_id)
        })
    }
}

pub(crate) fn set_auto(type_id: TypeId, cfg: DynConfig) {
    #[cfg(feature = "thread-local")]
    thread_local_state::set(type_id, cfg);
    #[cfg(not(feature = "thread-local"))]
    set_global(type_id, cfg);
}

pub(crate) fn set_global(type_id: TypeId, cfg: DynConfig) {
    global_state::set(type_id, cfg)
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
        thread_local_state::get(type_id).or_else(|| global_state::get(type_id))
    }

    #[cfg(not(feature = "thread-local"))]
    global_state::get(type_id)
}
