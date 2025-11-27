use std::env;
use std::future::Future;
use tokio::io::{self, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

#[derive(Debug)]
enum Command {
    Ping,
    Echo(String),
    Quit,
    Unknown(Vec<String>),
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let bind_addr = env::var("REDUST_ADDR").unwrap_or_else(|_| "127.0.0.1:6379".to_string());
    let listener = TcpListener::bind(&bind_addr).await?;
    let local_addr = listener.local_addr()?;

    println!("Redust listening on {}", local_addr);

    serve(listener, std::future::pending()).await
}

async fn handle_connection(stream: TcpStream) -> io::Result<()> {
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
            Command::Unknown(parts) => {
                let joined = parts.join(" ");
                let response = format!("-ERR unknown command '{}'\r\n", joined);
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
    println!("[resp] header line: {:?}", header);
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
        println!("[resp] bulk header line: {:?}", bulk_header);
        let Some(stripped) = bulk_header.strip_prefix('$') else {
            return Ok(None);
        };

        let bulk_len: usize = stripped.parse().unwrap_or(0);
        let mut buf = vec![0u8; bulk_len];
        reader.read_exact(&mut buf).await?;

        // Consume trailing CRLF after the bulk string
        let mut crlf = [0u8; 2];
        reader.read_exact(&mut crlf).await?;

        let value = String::from_utf8_lossy(&buf).into_owned();
        println!("[resp] bulk value: {:?}", value);
        parts.push(value);
    }

    println!("[resp] parsed array parts: {:?}", parts);
    Ok(Some(parts))
}

async fn respond_bulk_string(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    value: &str,
) -> io::Result<()> {
    let response = format!("${}\r\n{}\r\n", value.len(), value);
    writer.write_all(response.as_bytes()).await
}

async fn serve(
    listener: TcpListener,
    shutdown: impl Future<Output = ()> + Send,
) -> io::Result<()> {
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            res = listener.accept() => {
                let (stream, addr) = res?;
                println!("Accepted connection from {}", addr);
                tokio::spawn(async move {
                    if let Err(err) = handle_connection(stream).await {
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::sync::oneshot;
    use tokio::time::{Duration, Instant};

    async fn spawn_server() -> (std::net::SocketAddr, oneshot::Sender<()>, tokio::task::JoinHandle<io::Result<()>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind server");
        let addr = listener.local_addr().expect("local addr");
        let (tx, rx) = oneshot::channel();
        let handle = tokio::spawn(async move {
            serve(listener, async move {
                let _ = rx.await;
            })
            .await
        });
        (addr, tx, handle)
    }

    #[tokio::test]
    async fn responds_to_basic_commands() {
        let (addr, shutdown, handle) = spawn_server().await;
        let stream = TcpStream::connect(addr).await.unwrap();
        let (read_half, mut write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);

        write_half
            .write_all(b"*1\r\n$4\r\nPING\r\n")
            .await
            .unwrap();
        let mut pong = String::new();
        reader.read_line(&mut pong).await.unwrap();
        assert_eq!(pong, "+PONG\r\n");

        write_half
            .write_all(b"*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n")
            .await
            .unwrap();
        let mut bulk_header = String::new();
        reader.read_line(&mut bulk_header).await.unwrap();
        assert_eq!(bulk_header, "$5\r\n");
        let mut bulk_value = String::new();
        reader.read_line(&mut bulk_value).await.unwrap();
        assert_eq!(bulk_value, "hello\r\n");

        shutdown.send(()).unwrap();
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn continues_after_unknown_command() {
        let (addr, shutdown, handle) = spawn_server().await;
        let stream = TcpStream::connect(addr).await.unwrap();
        let (read_half, mut write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);

        write_half
            .write_all(b"*2\r\n$7\r\nCOMMAND\r\n$4\r\nDOCS\r\n")
            .await
            .unwrap();

        let mut error_line = String::new();
        reader.read_line(&mut error_line).await.unwrap();
        assert_eq!(error_line, "-ERR unknown command 'COMMAND DOCS'\r\n");

        write_half
            .write_all(b"*1\r\n$4\r\nPING\r\n")
            .await
            .unwrap();
        let mut pong = String::new();
        reader.read_line(&mut pong).await.unwrap();
        assert_eq!(pong, "+PONG\r\n");

        shutdown.send(()).unwrap();
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn handles_quit_and_connection_close() {
        let (addr, shutdown, handle) = spawn_server().await;
        let stream = TcpStream::connect(addr).await.unwrap();
        let (read_half, mut write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);

        write_half
            .write_all(b"*1\r\n$4\r\nQUIT\r\n")
            .await
            .unwrap();
        let mut ok = String::new();
        reader.read_line(&mut ok).await.unwrap();
        assert_eq!(ok, "+OK\r\n");

        shutdown.send(()).unwrap();
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn performance_ping_round_trips() {
        let (addr, shutdown, handle) = spawn_server().await;
        let stream = TcpStream::connect(addr).await.unwrap();
        let (read_half, mut write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);

        let iterations = 200;
        let start = Instant::now();
        for _ in 0..iterations {
            write_half
                .write_all(b"*1\r\n$4\r\nPING\r\n")
                .await
                .unwrap();
            let mut line = String::new();
            reader.read_line(&mut line).await.unwrap();
            assert_eq!(line, "+PONG\r\n");
            line.clear();
        }
        let elapsed = start.elapsed();
        assert!(elapsed < Duration::from_secs(2), "Ping loop took too long: {:?}", elapsed);

        shutdown.send(()).unwrap();
        handle.await.unwrap().unwrap();
    }
}
