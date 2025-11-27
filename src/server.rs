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
            Command::Unknown(parts) => {
                let joined = parts.join(" ");
                let response = format!("-ERR unknown command '{}'\r\n", joined);
                write_half.write_all(response.as_bytes()).await?;
            }
        }
    }

    Ok(())
}

async fn serve(
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

        // SET key
        write_half
            .write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
            .await
            .unwrap();
        let mut ok = String::new();
        reader.read_line(&mut ok).await.unwrap();
        assert_eq!(ok, "+OK\r\n");

        // GET key
        write_half
            .write_all(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n")
            .await
            .unwrap();
        let mut get_header = String::new();
        reader.read_line(&mut get_header).await.unwrap();
        assert_eq!(get_header, "$3\r\n");
        let mut get_value = String::new();
        reader.read_line(&mut get_value).await.unwrap();
        assert_eq!(get_value, "bar\r\n");

        // EXISTS key
        write_half
            .write_all(b"*2\r\n$6\r\nEXISTS\r\n$3\r\nfoo\r\n")
            .await
            .unwrap();
        let mut exists_resp = String::new();
        reader.read_line(&mut exists_resp).await.unwrap();
        assert_eq!(exists_resp, ":1\r\n");

        // DEL key
        write_half
            .write_all(b"*2\r\n$3\r\nDEL\r\n$3\r\nfoo\r\n")
            .await
            .unwrap();
        let mut del_resp = String::new();
        reader.read_line(&mut del_resp).await.unwrap();
        assert_eq!(del_resp, ":1\r\n");

        // GET missing key -> nil bulk string
        write_half
            .write_all(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n")
            .await
            .unwrap();
        let mut nil_resp = String::new();
        reader.read_line(&mut nil_resp).await.unwrap();
        assert_eq!(nil_resp, "$-1\r\n");

        shutdown.send(()).unwrap();
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn multiple_clients_share_storage() {
        let (addr, shutdown, handle) = spawn_server().await;

        // client 1: SET foo bar
        let stream1 = TcpStream::connect(addr).await.unwrap();
        let (_r1, mut w1) = stream1.into_split();
        w1.write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
            .await
            .unwrap();

        // consume +OK from client1 (not strictly necessary to assert here)
        // but we open a separate client for GET to ensure shared state

        // client 2: GET foo, should see bar
        let stream2 = TcpStream::connect(addr).await.unwrap();
        let (r2, mut w2) = stream2.into_split();
        let mut reader2 = BufReader::new(r2);
        w2.write_all(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n")
            .await
            .unwrap();

        let mut header = String::new();
        let mut value = String::new();
        reader2.read_line(&mut header).await.unwrap();
        reader2.read_line(&mut value).await.unwrap();
        assert_eq!(header, "$3\r\n");
        assert_eq!(value, "bar\r\n");

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
