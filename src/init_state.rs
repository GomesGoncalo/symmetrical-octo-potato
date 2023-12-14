use std::collections::HashSet;

use crate::{Init, Initable};

pub struct InitState<T: Initable> {
    init: Init,
    state: T,
    neighborhood: HashSet<String>,
}

impl<T: Initable> InitState<T> {
    #[must_use]
    pub fn from_init(init: Init) -> Self {
        Self {
            init: init.clone(),
            state: T::with_init(init.clone()),
            neighborhood: init.node_ids,
        }
    }

    pub fn get_neighbors(&self) -> &HashSet<String> {
        &self.neighborhood
    }

    pub fn update_neighbor(&mut self, val: &str) {
        self.neighborhood.insert(val.to_string());
    }

    pub fn get_init(&self) -> &Init {
        &self.init
    }

    pub fn get_inner_mut(&mut self) -> &mut T {
        &mut self.state
    }

    pub fn get_inner(&self) -> &T {
        &self.state
    }
}
