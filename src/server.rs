use std::future::Future;
use std::sync::{Arc, atomic::{AtomicU64, AtomicUsize, Ordering}};
use std::time::Instant;

use tokio::io::{self, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

use crate::command::{read_command, Command, CommandError}; // Import CommandError
use crate::resp::{
    respond_bulk_string,
    respond_simple_string,
    respond_error,
    respond_integer,
    respond_null_bulk,
};
use crate::storage::Storage;

struct Metrics {
    start_time: Instant,
    connected_clients: AtomicUsize,
    total_commands: AtomicU64,
    tcp_port: u16,
}

async fn handle_string_command(
    cmd: Command,
    storage: &Storage,
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
) -> io::Result<()> {
    match cmd {
        Command::Ping => {
            respond_simple_string(writer, "PONG").await?;
        }
        Command::Echo(value) => {
            respond_bulk_string(writer, &value).await?;
        }
        Command::Quit => {
            respond_simple_string(writer, "OK").await?;
        }
        Command::Set { key, value, expire_millis } => {
            storage.set(key.clone(), value);
            if let Some(ms) = expire_millis {
                storage.expire_millis(&key, ms);
            }
            respond_simple_string(writer, "OK").await?;
        }
        Command::Get { key } => {
            if let Some(value) = storage.get(&key) {
                respond_bulk_string(writer, &value).await?;
            } else {
                respond_null_bulk(writer).await?;
            }
        }
        Command::Incr { key } => {
            match storage.incr(&key) {
                Ok(value) => {
                    respond_integer(writer, value).await?;
                }
                Err(_) => {
                    respond_error(writer, "ERR value is not an integer or out of range").await?;
                }
            }
        }
        Command::Decr { key } => {
            match storage.decr(&key) {
                Ok(value) => {
                    respond_integer(writer, value).await?;
                }
                Err(_) => {
                    respond_error(writer, "ERR value is not an integer or out of range").await?;
                }
            }
        }
        Command::Incrby { key, delta } => {
            match storage.incr_by(&key, delta) {
                Ok(value) => {
                    respond_integer(writer, value).await?;
                }
                Err(_) => {
                    respond_error(writer, "ERR value is not an integer or out of range").await?;
                }
            }
        }
        Command::Decrby { key, delta } => {
            match storage.incr_by(&key, -delta) {
                Ok(value) => {
                    respond_integer(writer, value).await?;
                }
                Err(_) => {
                    respond_error(writer, "ERR value is not an integer or out of range").await?;
                }
            }
        }
        Command::Del { keys } => {
            let removed = storage.del(&keys);
            respond_integer(writer, removed as i64).await?;
        }
        Command::Exists { keys } => {
            let count = storage.exists(&keys);
            respond_integer(writer, count as i64).await?;
        }
        Command::Mget { keys } => {
            let values = storage.mget(&keys);
            let mut response = format!("*{}\r\n", values.len());
            for v in values {
                match v {
                    Some(s) => {
                        response.push_str(&format!("${}\r\n{}\r\n", s.len(), s));
                    }
                    None => {
                        response.push_str("$-1\r\n");
                    }
                }
            }
            writer.write_all(response.as_bytes()).await?;
        }
        Command::Mset { pairs } => {
            storage.mset(&pairs);
            respond_simple_string(writer, "OK").await?;
        }
        Command::Setnx { key, value } => {
            let inserted = storage.setnx(&key, value);
            let v = if inserted { 1 } else { 0 };
            respond_integer(writer, v).await?;
        }
        Command::Setex { key, seconds, value } => {
            storage.set_with_expire_seconds(key, value, seconds);
            respond_simple_string(writer, "OK").await?;
        }
        Command::Psetex { key, millis, value } => {
            storage.set_with_expire_millis(key, value, millis);
            respond_simple_string(writer, "OK").await?;
        }
        _ => {}
    }

    Ok(())
}

async fn handle_list_command(
    cmd: Command,
    storage: &Storage,
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
) -> io::Result<()> {
    match cmd {
        Command::Lpush { key, values } => {
            let len = storage.lpush(&key, &values);
            respond_integer(writer, len as i64).await?;
        }
        Command::Rpush { key, values } => {
            let len = storage.rpush(&key, &values);
            respond_integer(writer, len as i64).await?;
        }
        Command::Lrange { key, start, stop } => {
            let items = storage.lrange(&key, start, stop);
            let mut response = format!("*{}\r\n", items.len());
            for item in items {
                response.push_str(&format!("${}\r\n{}\r\n", item.len(), item));
            }
            writer.write_all(response.as_bytes()).await?;
        }
        Command::Lpop { key } => {
            if let Some(value) = storage.lpop(&key) {
                respond_bulk_string(writer, &value).await?;
            } else {
                respond_null_bulk(writer).await?;
            }
        }
        Command::Rpop { key } => {
            if let Some(value) = storage.rpop(&key) {
                respond_bulk_string(writer, &value).await?;
            } else {
                respond_null_bulk(writer).await?;
            }
        }
        _ => {}
    }

    Ok(())
}

async fn handle_set_command(
    cmd: Command,
    storage: &Storage,
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
) -> io::Result<()> {
    match cmd {
        Command::Sadd { key, members } => {
            let added = storage.sadd(&key, &members);
            respond_integer(writer, added as i64).await?;
        }
        Command::Srem { key, members } => {
            let removed = storage.srem(&key, &members);
            respond_integer(writer, removed as i64).await?;
        }
        Command::Smembers { key } => {
            let members = storage.smembers(&key);
            let mut response = format!("*{}\r\n", members.len());
            for m in members {
                response.push_str(&format!("${}\r\n{}\r\n", m.len(), m));
            }
            writer.write_all(response.as_bytes()).await?;
        }
        Command::Scard { key } => {
            let card = storage.scard(&key);
            respond_integer(writer, card as i64).await?;
        }
        Command::Sismember { key, member } => {
            let is_member = storage.sismember(&key, &member);
            let v = if is_member { 1 } else { 0 };
            respond_integer(writer, v).await?;
        }
        Command::Sunion { keys } => {
            let members = storage.sunion(&keys);
            let mut response = format!("*{}\r\n", members.len());
            for m in members {
                response.push_str(&format!("${}\r\n{}\r\n", m.len(), m));
            }
            writer.write_all(response.as_bytes()).await?;
        }
        Command::Sinter { keys } => {
            let members = storage.sinter(&keys);
            let mut response = format!("*{}\r\n", members.len());
            for m in members {
                response.push_str(&format!("${}\r\n{}\r\n", m.len(), m));
            }
            writer.write_all(response.as_bytes()).await?;
        }
        Command::Sdiff { keys } => {
            let members = storage.sdiff(&keys);
            let mut response = format!("*{}\r\n", members.len());
            for m in members {
                response.push_str(&format!("${}\r\n{}\r\n", m.len(), m));
            }
            writer.write_all(response.as_bytes()).await?;
        }
        _ => {}
    }

    Ok(())
}

async fn handle_hash_command(
    cmd: Command,
    storage: &Storage,
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
) -> io::Result<()> {
    match cmd {
        Command::Hset { key, field, value } => {
            let added = storage.hset(&key, &field, value);
            respond_integer(writer, added as i64).await?;
        }
        Command::Hget { key, field } => {
            if let Some(value) = storage.hget(&key, &field) {
                respond_bulk_string(writer, &value).await?;
            } else {
                respond_null_bulk(writer).await?;
            }
        }
        Command::Hdel { key, fields } => {
            let removed = storage.hdel(&key, &fields);
            respond_integer(writer, removed as i64).await?;
        }
        Command::Hexists { key, field } => {
            let exists = storage.hexists(&key, &field);
            let v = if exists { 1 } else { 0 };
            respond_integer(writer, v).await?;
        }
        Command::Hgetall { key } => {
            let entries = storage.hgetall(&key);
            let mut response = format!("*{}\r\n", entries.len() * 2);
            for (field, value) in entries {
                response.push_str(&format!("${}\r\n{}\r\n", field.len(), field));
                response.push_str(&format!("${}\r\n{}\r\n", value.len(), value));
            }
            writer.write_all(response.as_bytes()).await?;
        }
        _ => {}
    }

    Ok(())
}

async fn handle_key_meta_command(
    cmd: Command,
    storage: &Storage,
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
) -> io::Result<()> {
    match cmd {
        Command::Expire { key, seconds } => {
            let res = storage.expire_seconds(&key, seconds);
            let v = if res { 1 } else { 0 };
            respond_integer(writer, v).await?;
        }
        Command::Pexpire { key, millis } => {
            let res = storage.expire_millis(&key, millis);
            let v = if res { 1 } else { 0 };
            respond_integer(writer, v).await?;
        }
        Command::Ttl { key } => {
            let ttl = storage.ttl_seconds(&key);
            respond_integer(writer, ttl).await?;
        }
        Command::Pttl { key } => {
            let ttl = storage.pttl_millis(&key);
            respond_integer(writer, ttl).await?;
        }
        Command::Persist { key } => {
            let changed = storage.persist(&key);
            let v = if changed { 1 } else { 0 };
            respond_integer(writer, v).await?;
        }
        Command::Type { key } => {
            let t = storage.type_of(&key);
            respond_simple_string(writer, &t).await?;
        }
        Command::Keys { pattern } => {
            let keys = storage.keys(&pattern);
            let mut response = format!("*{}\r\n", keys.len());
            for k in keys {
                response.push_str(&format!("${}\r\n{}\r\n", k.len(), k));
            }
            writer.write_all(response.as_bytes()).await?;
        }
        _ => {}
    }

    Ok(())
}

async fn handle_info_command(
    storage: &Storage,
    metrics: &Metrics,
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
) -> io::Result<()> {
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

    respond_bulk_string(writer, &info).await
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
            // string / generic key-value 命令
            Command::Ping
            | Command::Echo(_)
            | Command::Set { .. }
            | Command::Get { .. }
            | Command::Incr { .. }
            | Command::Decr { .. }
            | Command::Incrby { .. }
            | Command::Decrby { .. }
            | Command::Del { .. }
            | Command::Exists { .. }
            | Command::Mget { .. }
            | Command::Mset { .. }
            | Command::Setnx { .. }
            | Command::Setex { .. }
            | Command::Psetex { .. } => {
                // Quit 需要在外面单独处理连接关闭语义
                handle_string_command(cmd, &storage, &mut write_half).await?;
            }
            Command::Quit => {
                handle_string_command(cmd, &storage, &mut write_half).await?;
                break;
            }

            // list 命令
            Command::Lpush { .. }
            | Command::Rpush { .. }
            | Command::Lrange { .. }
            | Command::Lpop { .. }
            | Command::Rpop { .. } => {
                handle_list_command(cmd, &storage, &mut write_half).await?;
            }

            // set 命令
            Command::Sadd { .. }
            | Command::Srem { .. }
            | Command::Smembers { .. }
            | Command::Scard { .. }
            | Command::Sismember { .. }
            | Command::Sunion { .. }
            | Command::Sinter { .. }
            | Command::Sdiff { .. } => {
                handle_set_command(cmd, &storage, &mut write_half).await?;
            }

            // hash 命令
            Command::Hset { .. }
            | Command::Hget { .. }
            | Command::Hdel { .. }
            | Command::Hexists { .. }
            | Command::Hgetall { .. } => {
                handle_hash_command(cmd, &storage, &mut write_half).await?;
            }

            // key 元信息、过期相关命令
            Command::Expire { .. }
            | Command::Pexpire { .. }
            | Command::Ttl { .. }
            | Command::Pttl { .. }
            | Command::Persist { .. }
            | Command::Type { .. }
            | Command::Keys { .. } => {
                handle_key_meta_command(cmd, &storage, &mut write_half).await?;
            }

            // info
            Command::Info => {
                handle_info_command(&storage, &metrics, &mut write_half).await?;
            }

            // 解析阶段构造的错误命令
            Command::Error(msg) => {
                respond_error(&mut write_half, &msg).await?;
            }

            // 未知命令
            Command::Unknown(parts) => {
                let joined = parts.join(" ");
                let msg = format!("ERR unknown command '{}'", joined);
                respond_error(&mut write_half, &msg).await?;
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
