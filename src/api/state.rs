use crate::config::Config;
use crate::store::Store;
use crate::sync::scan::EventBus;

pub struct AppState {
    pub store: Store,
    pub bus: EventBus,
    pub cfg: Config,
}
