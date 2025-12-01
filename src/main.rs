use std::env;
use tokio::io;
use tokio::signal; // Import the signal module

use log::info;
use redust::run_server;

#[tokio::main]
async fn main() -> io::Result<()> {
    // 初始化日志（仅在 main 中调用一次），默认 info 级别，可被 RUST_LOG 覆盖
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    // 解析简单的命令行参数：支持 --bind 和 --maxmemory-bytes
    let args: Vec<String> = env::args().skip(1).collect();

    let mut bind_from_cli: Option<String> = None;
    let mut maxmemory_from_cli: Option<String> = None;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--bind" => {
                if i + 1 < args.len() {
                    bind_from_cli = Some(args[i + 1].clone());
                    i += 1;
                }
            }
            "--maxmemory-bytes" => {
                if i + 1 < args.len() {
                    maxmemory_from_cli = Some(args[i + 1].clone());
                    i += 1;
                }
            }
            _ => {}
        }
        i += 1;
    }

    // 命令行优先覆盖环境变量
    if let Some(b) = bind_from_cli {
        env::set_var("REDUST_ADDR", &b);
    }
    if let Some(m) = maxmemory_from_cli {
        env::set_var("REDUST_MAXMEMORY_BYTES", &m);
    }

    let bind_addr = env::var("REDUST_ADDR").unwrap_or_else(|_| "127.0.0.1:6379".to_string());

    // Create a future that resolves when Ctrl+C is received
    let shutdown_future = async {
        signal::ctrl_c().await.expect("Failed to listen for Ctrl+C");
        info!("Ctrl+C received, shutting down gracefully...");
    };

    run_server(&bind_addr, shutdown_future).await
}
