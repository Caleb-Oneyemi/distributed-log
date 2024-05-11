use crate::models::Log;
use std::sync::Mutex;

pub struct AppState<'a> {
    pub log: Mutex<Log<'a>>,
}
