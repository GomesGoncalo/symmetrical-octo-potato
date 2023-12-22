use crate::log::Log;
use std::ops::DerefMut;

pub trait Store<T>: DerefMut<Target = Log<T>> {
    fn new_value(&mut self, _new: &T) {}
}
