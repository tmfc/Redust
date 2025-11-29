use std::env;
use tokio::io;
use tokio::signal; // Import the signal module

use redust::run_server;

#[tokio::main]
async fn main() -> io::Result<()> {
    let bind_addr = env::var("REDUST_ADDR").unwrap_or_else(|_| "127.0.0.1:6379".to_string());

    // Create a future that resolves when Ctrl+C is received
    let shutdown_future = async {
        signal::ctrl_c().await.expect("Failed to listen for Ctrl+C");
        println!("Ctrl+C received, shutting down gracefully...");
    };

    run_server(&bind_addr, shutdown_future).await
}
