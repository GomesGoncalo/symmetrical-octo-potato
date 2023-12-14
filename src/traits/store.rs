use crate::log::Log;

pub trait Store<T> {
    fn get_log(&self) -> &Log<T>;
    fn get_log_mut(&mut self) -> &mut Log<T>;
}
