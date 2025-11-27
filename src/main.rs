use std::env;
use tokio::io;

use redust::run_server;

#[tokio::main]
async fn main() -> io::Result<()> {
    let bind_addr = env::var("REDUST_ADDR").unwrap_or_else(|_| "127.0.0.1:6379".to_string());

    run_server(&bind_addr, std::future::pending()).await
}
