use std::env;
use tokio::io;
use tokio::signal;

use log::info;
use redust::{run_server, run_server_tls};

#[tokio::main]
async fn main() -> io::Result<()> {
    // 初始化日志（仅在 main 中调用一次），默认 info 级别，可被 RUST_LOG 覆盖
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    // 解析简单的命令行参数
    let args: Vec<String> = env::args().skip(1).collect();

    let mut bind_from_cli: Option<String> = None;
    let mut maxmemory_from_cli: Option<String> = None;
    let mut rdb_load_mode_from_cli: Option<String> = None;
    let mut tls_cert_from_cli: Option<String> = None;
    let mut tls_key_from_cli: Option<String> = None;

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
            "--rdb-load-mode" => {
                if i + 1 < args.len() {
                    rdb_load_mode_from_cli = Some(args[i + 1].clone());
                    i += 1;
                }
            }
            "--tls-cert" => {
                if i + 1 < args.len() {
                    tls_cert_from_cli = Some(args[i + 1].clone());
                    i += 1;
                }
            }
            "--tls-key" => {
                if i + 1 < args.len() {
                    tls_key_from_cli = Some(args[i + 1].clone());
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
    if let Some(m) = rdb_load_mode_from_cli {
        env::set_var("REDUST_RDB_LOAD_MODE", &m);
    }

    let bind_addr = env::var("REDUST_ADDR").unwrap_or_else(|_| "127.0.0.1:6379".to_string());

    // TLS 配置：命令行参数优先，其次环境变量
    let tls_cert = tls_cert_from_cli.or_else(|| env::var("REDUST_TLS_CERT").ok());
    let tls_key = tls_key_from_cli.or_else(|| env::var("REDUST_TLS_KEY").ok());

    // Create a future that resolves when Ctrl+C is received
    let shutdown_future = async {
        signal::ctrl_c().await.expect("Failed to listen for Ctrl+C");
        info!("Ctrl+C received, shutting down gracefully...");
    };

    // 如果同时提供了证书和私钥，启用 TLS
    match (tls_cert, tls_key) {
        (Some(cert), Some(key)) => {
            info!("TLS enabled with cert: {}, key: {}", cert, key);
            run_server_tls(&bind_addr, &cert, &key, shutdown_future).await
        }
        (Some(_), None) | (None, Some(_)) => {
            eprintln!("Error: Both --tls-cert and --tls-key must be provided for TLS");
            std::process::exit(1);
        }
        (None, None) => run_server(&bind_addr, shutdown_future).await,
    }
}
