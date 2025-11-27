use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use tokio::io::{self, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

#[derive(Debug)]
enum Command {
    Ping,
    Echo(String),
    Set(String, String),
    Get(String),
    Quit,
    Unknown(Vec<String>),
}

type SharedStore = Arc<Mutex<HashMap<String, String>>>;

#[tokio::main]
async fn main() -> io::Result<()> {
    let bind_addr = env::var("REDUST_ADDR").unwrap_or_else(|_| "127.0.0.1:6379".to_string());
    let listener = TcpListener::bind(&bind_addr).await?;
    let store: SharedStore = Arc::new(Mutex::new(HashMap::new()));

    println!("Redust listening on {}", bind_addr);

    loop {
        let (stream, addr) = listener.accept().await?;
        println!("Accepted connection from {}", addr);
        let store = store.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_connection(stream, store).await {
                eprintln!("Connection error: {}", err);
            }
        });
    }
}

async fn handle_connection(stream: TcpStream, store: SharedStore) -> io::Result<()> {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    while let Some(cmd) = read_command(&mut reader).await? {
        match cmd {
            Command::Ping => write_half.write_all(b"+PONG\r\n").await?,
            Command::Echo(value) => respond_bulk_string(&mut write_half, &value).await?,
            Command::Set(key, value) => {
                {
                    let mut store = store.lock().await;
                    store.insert(key, value);
                }
                write_half.write_all(b"+OK\r\n").await?;
            }
            Command::Get(key) => {
                let value = {
                    let store = store.lock().await;
                    store.get(&key).cloned()
                };

                match value {
                    Some(value) => respond_bulk_string(&mut write_half, &value).await?,
                    None => respond_null_bulk(&mut write_half).await?,
                }
            }
            Command::Quit => {
                write_half.write_all(b"+OK\r\n").await?;
                break;
            }
            Command::Unknown(parts) => {
                let joined = parts.join(" ");
                let response = format!("-ERR unknown command '{}'\\r\\n", joined);
                write_half.write_all(response.as_bytes()).await?;
            }
        }
    }

    Ok(())
}

async fn read_command(
    reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>,
) -> io::Result<Option<Command>> {
    let Some(parts) = read_resp_array(reader).await? else {
        return Ok(None);
    };

    let mut iter = parts.into_iter();
    let Some(command) = iter.next() else {
        return Ok(None);
    };

    let upper = command.to_ascii_uppercase();
    let cmd = match upper.as_str() {
        "PING" => Command::Ping,
        "ECHO" => Command::Echo(iter.collect::<Vec<_>>().join(" ")),
        "SET" => {
            let Some(key) = iter.next() else {
                return Ok(Some(Command::Unknown(vec![command])));
            };

            let Some(value_first) = iter.next() else {
                return Ok(Some(Command::Unknown(vec![command, key])));
            };

            let mut value_parts = vec![value_first];
            value_parts.extend(iter);
            Command::Set(key, value_parts.join(" "))
        }
        "GET" => match iter.next() {
            Some(key) => Command::Get(key),
            None => Command::Unknown(vec![command]),
        },
        "QUIT" => Command::Quit,
        _ => Command::Unknown(std::iter::once(command).chain(iter).collect()),
    };

    Ok(Some(cmd))
}

async fn read_resp_array(
    reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>,
) -> io::Result<Option<Vec<String>>> {
    let mut header = String::new();
    let read = reader.read_line(&mut header).await?;
    if read == 0 {
        return Ok(None);
    }

    let header = header.trim_end();
    if !header.starts_with('*') {
        return Ok(Some(vec![header.to_string()]));
    }

    let array_len: usize = header[1..].parse().unwrap_or(0);
    let mut parts = Vec::with_capacity(array_len);

    for _ in 0..array_len {
        let mut bulk_header = String::new();
        if reader.read_line(&mut bulk_header).await? == 0 {
            return Ok(None);
        }

        let bulk_header = bulk_header.trim_end();
        let Some(stripped) = bulk_header.strip_prefix('$') else {
            return Ok(None);
        };

        let bulk_len: usize = stripped.parse().unwrap_or(0);
        let mut buf = vec![0u8; bulk_len];
        reader.read_exact(&mut buf).await?;

        // Consume trailing CRLF after the bulk string
        let mut crlf = [0u8; 2];
        reader.read_exact(&mut crlf).await?;

        parts.push(String::from_utf8_lossy(&buf).into_owned());
    }

    Ok(Some(parts))
}

async fn respond_bulk_string(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    value: &str,
) -> io::Result<()> {
    let response = format!("${}\r\n{}\r\n", value.len(), value);
    writer.write_all(response.as_bytes()).await
}

async fn respond_null_bulk(writer: &mut tokio::net::tcp::OwnedWriteHalf) -> io::Result<()> {
    writer.write_all(b"$-1\r\n").await
}
