use std::net::SocketAddr;
use std::sync::{Mutex, OnceLock};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

use redust::server::serve;

fn persistence_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
}

async fn spawn_server() -> (
    SocketAddr,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<tokio::io::Result<()>>,
) {
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

struct TestClient {
    reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
    writer: tokio::net::tcp::OwnedWriteHalf,
}

impl TestClient {
    async fn connect(addr: SocketAddr) -> Self {
        let stream = TcpStream::connect(addr).await.unwrap();
        let (read_half, write_half) = stream.into_split();
        let reader = BufReader::new(read_half);
        TestClient {
            reader,
            writer: write_half,
        }
    }

    async fn send_array(&mut self, parts: &[&str]) {
        let mut buf = String::new();
        buf.push_str(&format!("*{}\r\n", parts.len()));
        for p in parts {
            buf.push_str(&format!("${}\r\n{}\r\n", p.len(), p));
        }
        self.writer.write_all(buf.as_bytes()).await.unwrap();
    }

    async fn read_simple_line(&mut self) -> String {
        let mut line = String::new();
        self.reader.read_line(&mut line).await.unwrap();
        line
    }
}

#[tokio::test]
async fn save_and_load_snapshot_via_rdb_commands() {
    let _guard = persistence_lock();
    let tmp = std::env::temp_dir();
    let path = tmp.join(format!("redust_save_{}.rdb", rand::random::<u64>()));
    let path_str = path.to_string_lossy().to_string();
    let _ = std::fs::remove_file(&path);
    std::env::set_var("REDUST_RDB_PATH", &path_str);
    std::env::remove_var("REDUST_AOF_ENABLED");

    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    client.send_array(&["SET", "pkey", "pval"]).await;
    let _ = client.read_simple_line().await;

    client.send_array(&["SAVE"]).await;
    let resp = client.read_simple_line().await;
    assert_eq!(resp, "+OK\r\n");

    client.send_array(&["LASTSAVE"]).await;
    let last = client.read_simple_line().await;
    assert!(last.starts_with(':'));

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();

    // restart and ensure key is loaded from snapshot
    let (addr2, shutdown2, handle2) = spawn_server().await;
    let mut client2 = TestClient::connect(addr2).await;
    client2.send_array(&["GET", "pkey"]).await;
    let mut line = String::new();
    client2.reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, "$4\r\n");
    let mut buf = [0u8; 4];
    client2.reader.read_exact(&mut buf).await.unwrap();
    let mut crlf = [0u8; 2];
    client2.reader.read_exact(&mut crlf).await.unwrap();
    assert_eq!(String::from_utf8(buf.to_vec()).unwrap(), "pval");

    shutdown2.send(()).unwrap();
    handle2.await.unwrap().unwrap();

    let _ = std::fs::remove_file(&path);
    std::env::remove_var("REDUST_RDB_PATH");
}

#[tokio::test]
async fn aof_everysec_persists_state() {
    let _guard = persistence_lock();
    let tmp = std::env::temp_dir();
    let path = tmp.join(format!("redust_aof_{}.aof", rand::random::<u64>()));
    let path_str = path.to_string_lossy().to_string();
    let _ = std::fs::remove_file(&path);
    std::env::set_var("REDUST_AOF_ENABLED", "1");
    std::env::set_var("REDUST_AOF_PATH", &path_str);
    std::env::remove_var("REDUST_RDB_PATH");

    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;
    client.send_array(&["SET", "akey", "aval"]).await;
    let _ = client.read_simple_line().await;

    // force snapshot to disk
    client.send_array(&["SAVE"]).await;
    let resp = client.read_simple_line().await;
    assert_eq!(resp, "+OK\r\n");

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();

    // snapshot file should exist after shutdown
    let meta = std::fs::metadata(&path).expect("snapshot file should exist");
    assert!(meta.len() > 0);

    // restart with same AOF path
    let (addr2, shutdown2, handle2) = spawn_server().await;
    let mut client2 = TestClient::connect(addr2).await;
    client2.send_array(&["GET", "akey"]).await;
    let mut header = String::new();
    client2.reader.read_line(&mut header).await.unwrap();
    assert_eq!(header, "$4\r\n");
    let mut buf = [0u8; 4];
    client2.reader.read_exact(&mut buf).await.unwrap();
    let mut crlf = [0u8; 2];
    client2.reader.read_exact(&mut crlf).await.unwrap();
    assert_eq!(String::from_utf8(buf.to_vec()).unwrap(), "aval");

    shutdown2.send(()).unwrap();
    handle2.await.unwrap().unwrap();

    let _ = std::fs::remove_file(&path);
    std::env::remove_var("REDUST_AOF_ENABLED");
    std::env::remove_var("REDUST_AOF_PATH");
}

#[tokio::test]
async fn corrupted_rdb_is_quarantined_and_ignored() {
    let _guard = persistence_lock();
    let tmp = std::env::temp_dir();
    let base = format!("redust_corrupt_{}.rdb", rand::random::<u64>());
    let path = tmp.join(&base);
    let path_str = path.to_string_lossy().to_string();
    // 写入明显损坏的内容
    std::fs::write(&path, b"BAD").unwrap();
    std::env::set_var("REDUST_RDB_PATH", &path_str);
    std::env::remove_var("REDUST_AOF_ENABLED");
    std::env::remove_var("REDUST_AOF_PATH");

    let (addr, shutdown, handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    client.send_array(&["PING"]).await;
    let pong = client.read_simple_line().await;
    assert_eq!(pong, "+PONG\r\n");

    let parent = path.parent().unwrap();
    let base_prefix = format!("{}.corrupt", base);
    let mut quarantined: Option<std::path::PathBuf> = None;
    for entry in std::fs::read_dir(parent).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if name_str.starts_with(&base_prefix) {
            quarantined = Some(entry.path());
            break;
        }
    }

    assert!(
        quarantined.is_some(),
        "expected corrupt RDB to be quarantined next to {:?}",
        path
    );
    assert!(
        !path.exists(),
        "original corrupt file should be moved away from {:?}",
        path
    );

    shutdown.send(()).unwrap();
    handle.await.unwrap().unwrap();

    if let Some(q) = quarantined {
        let _ = std::fs::remove_file(q);
    }
    let _ = std::fs::remove_file(&path);
    std::env::remove_var("REDUST_RDB_PATH");
}
