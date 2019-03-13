extern crate flatmanager;
extern crate dotenv;
extern crate env_logger;

use dotenv::dotenv;
use std::env;
use std::path::PathBuf;

fn main() {
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "info");
    }
    env_logger::init();
    let sys = actix::System::new("repo-manage");

    dotenv().ok();

    let config_path = PathBuf::from(env::var("REPO_CONFIG").unwrap_or ("config.json".to_string()));

    let config = flatmanager::load_config(&config_path);

    let _server = flatmanager::start(&config);

    let _ = sys.run();
}
