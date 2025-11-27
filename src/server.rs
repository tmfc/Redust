use std::future::Future;

use tokio::io::{self, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

use crate::command::{read_command, Command};
use crate::resp::respond_bulk_string;
use crate::storage::Storage;

async fn handle_connection(stream: TcpStream, storage: Storage) -> io::Result<()> {
    let peer_addr = stream.peer_addr().ok();
    println!("[conn] new connection from {:?}", peer_addr);

    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    while let Some(cmd) = read_command(&mut reader).await? {
        println!("[conn] received command: {:?}", cmd);

        match cmd {
            Command::Ping => write_half.write_all(b"+PONG\r\n").await?,
            Command::Echo(value) => respond_bulk_string(&mut write_half, &value).await?,
            Command::Quit => {
                write_half.write_all(b"+OK\r\n").await?;
                break;
            }
            Command::Set { key, value } => {
                storage.set(key, value);
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
        }
    }

    Ok(())
}

pub async fn serve(
    listener: TcpListener,
    shutdown: impl Future<Output = ()> + Send,
) -> io::Result<()> {
    let storage = Storage::default();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            res = listener.accept() => {
                let (stream, addr) = res?;
                let storage = storage.clone();
                println!("Accepted connection from {}", addr);
                tokio::spawn(async move {
                    if let Err(err) = handle_connection(stream, storage).await {
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
