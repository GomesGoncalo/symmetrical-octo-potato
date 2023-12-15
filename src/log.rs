use std::collections::{hash_map::Values, HashMap};

use crate::{Init, Initable};

#[derive(Clone, Debug)]
pub struct Log<T> {
    node: String,
    counter: usize,
    values: HashMap<String, T>,
}

impl<T> Log<T> {
    pub(crate) fn contains_key(&self, key: &str) -> bool {
        self.values.contains_key(key)
    }

    pub(crate) fn insert_with_key(&mut self, key: &str, val: T) {
        self.values.insert(key.to_string(), val);
    }

    pub fn insert(&mut self, val: T) {
        self.values
            .insert(format!("{}-{}", self.node, self.counter), val);
        self.counter += 1;
    }

    #[must_use]
    pub fn values(&self) -> Values<String, T> {
        self.values.values()
    }
}

impl<T> IntoIterator for Log<T> {
    type Item = <HashMap<String, T> as IntoIterator>::Item;

    type IntoIter = <HashMap<String, T> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.values.into_iter()
    }
}

impl<T> Initable for Log<T> {
    fn with_init(init: Init) -> Self {
        Self {
            node: init.node_id,
            counter: 0,
            values: HashMap::new(),
        }
    }
}
