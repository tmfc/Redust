use std::future::Future;
use std::sync::{Arc, atomic::{AtomicU64, AtomicUsize, Ordering}};
use std::time::Instant;

use tokio::io::{self, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

use crate::command::{read_command, Command, CommandError}; // Import CommandError
use crate::resp::respond_bulk_string;
use crate::storage::Storage;

struct Metrics {
    start_time: Instant,
    connected_clients: AtomicUsize,
    total_commands: AtomicU64,
    tcp_port: u16,
}

async fn handle_connection(stream: TcpStream, storage: Storage, metrics: Arc<Metrics>) -> io::Result<()> {
    let peer_addr = stream.peer_addr().ok();
    println!("[conn] new connection from {:?}", peer_addr);

    metrics.connected_clients.fetch_add(1, Ordering::Relaxed);

    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    loop {
        let cmd_result = read_command(&mut reader).await;
        let cmd = match cmd_result {
            Ok(Some(command)) => command,
            Ok(None) => break, // Connection closed or no more data
            Err(CommandError::Io(e)) => {
                // Log the IO error and break
                eprintln!("[conn] IO error reading command: {}", e);
                break;
            }
            Err(CommandError::RedisError(msg)) => {
                // Send the Redis-like error message to the client
                let response = format!("-{}\r\n", msg);
                write_half.write_all(response.as_bytes()).await?;
                continue; // Continue to read next command
            }
        };

        metrics.total_commands.fetch_add(1, Ordering::Relaxed);

        println!("[conn] received command: {:?}", cmd);

        match cmd {
            Command::Ping => write_half.write_all(b"+PONG\r\n").await?,
            Command::Echo(value) => respond_bulk_string(&mut write_half, &value).await?,
            Command::Quit => {
                write_half.write_all(b"+OK\r\n").await?;
                break;
            }
            Command::Set { key, value, expire_millis } => {
                storage.set(key.clone(), value);
                if let Some(ms) = expire_millis {
                    storage.expire_millis(&key, ms);
                }
                write_half.write_all(b"+OK\r\n").await?;
            }
            Command::Get { key } => {
                if let Some(value) = storage.get(&key) {
                    respond_bulk_string(&mut write_half, &value).await?;
                } else {
                    write_half.write_all(b"$-1\r\n").await?;
                }
            }
            Command::Incr { key } => {
                match storage.incr(&key) {
                    Ok(value) => {
                        let response = format!(":{}\r\n", value);
                        write_half.write_all(response.as_bytes()).await?;
                    }
                    Err(_) => {
                        write_half
                            .write_all(b"-ERR value is not an integer or out of range\r\n")
                            .await?;
                    }
                }
            }
            Command::Decr { key } => {
                match storage.decr(&key) {
                    Ok(value) => {
                        let response = format!(":{}\r\n", value);
                        write_half.write_all(response.as_bytes()).await?;
                    }
                    Err(_) => {
                        write_half
                            .write_all(b"-ERR value is not an integer or out of range\r\n")
                            .await?;
                    }
                }
            }
            Command::Del { keys } => {
                let removed = storage.del(&keys);
                let response = format!(":{}\r\n", removed);
                write_half.write_all(response.as_bytes()).await?;
            }
            Command::Exists { keys } => {
                let count = storage.exists(&keys);
                let response = format!(":{}\r\n", count);
                write_half.write_all(response.as_bytes()).await?;
            }
            Command::Lpush { key, values } => {
                let len = storage.lpush(&key, &values);
                let response = format!(":{}\r\n", len);
                write_half.write_all(response.as_bytes()).await?;
            }
            Command::Rpush { key, values } => {
                let len = storage.rpush(&key, &values);
                let response = format!(":{}\r\n", len);
                write_half.write_all(response.as_bytes()).await?;
            }
            Command::Lrange { key, start, stop } => {
                let items = storage.lrange(&key, start, stop);
                let mut response = format!("*{}\r\n", items.len());
                for item in items {
                    response.push_str(&format!("${}\r\n{}\r\n", item.len(), item));
                }
                write_half.write_all(response.as_bytes()).await?;
            }
            Command::Lpop { key } => {
                if let Some(value) = storage.lpop(&key) {
                    respond_bulk_string(&mut write_half, &value).await?;
                } else {
                    write_half.write_all(b"$-1\r\n").await?;
                }
            }
            Command::Rpop { key } => {
                if let Some(value) = storage.rpop(&key) {
                    respond_bulk_string(&mut write_half, &value).await?;
                } else {
                    write_half.write_all(b"$-1\r\n").await?;
                }
            }
            Command::Sadd { key, members } => {
                let added = storage.sadd(&key, &members);
                let response = format!(":{}\r\n", added);
                write_half.write_all(response.as_bytes()).await?;
            }
            Command::Srem { key, members } => {
                let removed = storage.srem(&key, &members);
                let response = format!(":{}\r\n", removed);
                write_half.write_all(response.as_bytes()).await?;
            }
            Command::Smembers { key } => {
                let members = storage.smembers(&key);
                let mut response = format!("*{}\r\n", members.len());
                for m in members {
                    response.push_str(&format!("${}\r\n{}\r\n", m.len(), m));
                }
                write_half.write_all(response.as_bytes()).await?;
            }
            Command::Scard { key } => {
                let card = storage.scard(&key);
                let response = format!(":{}\r\n", card);
                write_half.write_all(response.as_bytes()).await?;
            }
            Command::Sismember { key, member } => {
                let is_member = storage.sismember(&key, &member);
                let response = if is_member { ":1\r\n" } else { ":0\r\n" };
                write_half.write_all(response.as_bytes()).await?;
            }
            Command::Sunion { keys } => {
                let members = storage.sunion(&keys);
                let mut response = format!("*{}\r\n", members.len());
                for m in members {
                    response.push_str(&format!("${}\r\n{}\r\n", m.len(), m));
                }
                write_half.write_all(response.as_bytes()).await?;
            }
            Command::Sinter { keys } => {
                let members = storage.sinter(&keys);
                let mut response = format!("*{}\r\n", members.len());
                for m in members {
                    response.push_str(&format!("${}\r\n{}\r\n", m.len(), m));
                }
                write_half.write_all(response.as_bytes()).await?;
            }
            Command::Sdiff { keys } => {
                let members = storage.sdiff(&keys);
                let mut response = format!("*{}\r\n", members.len());
                for m in members {
                    response.push_str(&format!("${}\r\n{}\r\n", m.len(), m));
                }
                write_half.write_all(response.as_bytes()).await?;
            }
            Command::Expire { key, seconds } => {
                let res = storage.expire_seconds(&key, seconds);
                let response = if res { ":1\r\n" } else { ":0\r\n" };
                write_half.write_all(response.as_bytes()).await?;
            }
            Command::Pexpire { key, millis } => {
                let res = storage.expire_millis(&key, millis);
                let response = if res { ":1\r\n" } else { ":0\r\n" };
                write_half.write_all(response.as_bytes()).await?;
            }
            Command::Ttl { key } => {
                let ttl = storage.ttl_seconds(&key);
                let response = format!(":{}\r\n", ttl);
                write_half.write_all(response.as_bytes()).await?;
            }
            Command::Pttl { key } => {
                let ttl = storage.pttl_millis(&key);
                let response = format!(":{}\r\n", ttl);
                write_half.write_all(response.as_bytes()).await?;
            }
            Command::Persist { key } => {
                let changed = storage.persist(&key);
                let response = if changed { ":1\r\n" } else { ":0\r\n" };
                write_half.write_all(response.as_bytes()).await?;
            }
            Command::Info => {
                let uptime = Instant::now().duration_since(metrics.start_time).as_secs();
                let connected = metrics.connected_clients.load(Ordering::Relaxed);
                let total_cmds = metrics.total_commands.load(Ordering::Relaxed);
                let keys = storage.keys("*").len();

                let mut info = String::new();
                info.push_str("# Server\r\n");
                info.push_str(&format!("redust_version:0.1.0\r\n"));
                info.push_str(&format!("tcp_port:{}\r\n", metrics.tcp_port));
                info.push_str(&format!("uptime_in_seconds:{}\r\n", uptime));
                info.push_str("\r\n# Clients\r\n");
                info.push_str(&format!("connected_clients:{}\r\n", connected));
                info.push_str("\r\n# Stats\r\n");
                info.push_str(&format!("total_commands_processed:{}\r\n", total_cmds));
                info.push_str("\r\n# Keyspace\r\n");
                info.push_str(&format!("db0:keys={}\r\n", keys));
                info.push_str("\r\n");

                respond_bulk_string(&mut write_half, &info).await?;
            }
            Command::Type { key } => {
                let t = storage.type_of(&key);
                let response = format!("+{}\r\n", t);
                write_half.write_all(response.as_bytes()).await?;
            }
            Command::Keys { pattern } => {
                let keys = storage.keys(&pattern);
                let mut response = format!("*{}\r\n", keys.len());
                for k in keys {
                    response.push_str(&format!("${}\r\n{}\r\n", k.len(), k));
                }
                write_half.write_all(response.as_bytes()).await?;
            }
            Command::Unknown(parts) => {
                let joined = parts.join(" ");
                let response = format!("-ERR unknown command '{}'\r\n", joined);
                write_half.write_all(response.as_bytes()).await?;
            }
            Command::Error(msg) => { // Handle Command::Error variant
                let response = format!("-{}\r\n", msg);
                write_half.write_all(response.as_bytes()).await?;
            }
        }
    }

    metrics.connected_clients.fetch_sub(1, Ordering::Relaxed);

    Ok(())
}

pub async fn serve(
    listener: TcpListener,
    shutdown: impl Future<Output = ()> + Send,
) -> io::Result<()> {
    let local_addr = listener.local_addr()?;
    let port = local_addr.port();

    let storage = Storage::default();
    storage.spawn_expiration_task();

    let metrics = Arc::new(Metrics {
        start_time: Instant::now(),
        connected_clients: AtomicUsize::new(0),
        total_commands: AtomicU64::new(0),
        tcp_port: port,
    });
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            res = listener.accept() => {
                let (stream, addr) = res?;
                let storage = storage.clone();
                let metrics = metrics.clone();
                println!("Accepted connection from {}", addr);
                tokio::spawn(async move {
                    if let Err(err) = handle_connection(stream, storage, metrics).await {
                        eprintln!("Connection error: {}", err);
                    }
                });
            }
            _ = &mut shutdown => {
                break;
            }
        }
    }

    Ok(())
}

pub async fn run_server(
    bind_addr: &str,
    shutdown: impl Future<Output = ()> + Send,
) -> io::Result<()> {
    let listener = TcpListener::bind(bind_addr).await?;
    let local_addr = listener.local_addr()?;

    println!("Redust listening on {}", local_addr);

    serve(listener, shutdown).await
}
