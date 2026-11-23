use dotenv::dotenv;
use std::env;
use std::path::PathBuf;

fn main() {
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "info");
    }
    env_logger::init();
    let sys = actix::System::new();

    dotenv().ok();

    let config_path =
        PathBuf::from(env::var("REPO_CONFIG").unwrap_or_else(|_| "config.json".to_string()));

    let config = flatmanager::load_config(&config_path);
    sys.block_on(async {
        let (server, stopped) = flatmanager::start(&config);
        let _ = server.await;
        let _ = stopped.await;
    });
}
