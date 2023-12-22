use crate::init_state::{Init, Initable};
use std::collections::{hash_map::Values, HashMap};

#[derive(Clone, Debug)]
pub struct Log<T> {
    node: String,
    counter: usize,
    values: HashMap<String, T>,
}

impl<T: Clone> Log<T> {
    #[must_use]
    pub(crate) fn insert_with_key<'b>(&mut self, key: &str, val: &'b T) -> Option<&'b T> {
        let inserted = !self.values.contains_key(key);
        self.values.insert(key.to_string(), val.clone());
        if inserted {
            Some(val)
        } else {
            None
        }
    }

    #[must_use]
    pub fn insert<'b>(&mut self, val: &'b T) -> Option<&'b T> {
        let inserted = self.insert_with_key(&format!("{}-{}", self.node, self.counter), val);
        self.counter += 1;
        inserted
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
