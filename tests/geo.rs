use std::net::SocketAddr;

use redust::server::serve;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::oneshot;

async fn spawn_server() -> (
    SocketAddr,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<tokio::io::Result<()>>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = oneshot::channel();
    let shutdown = async move {
        let _ = rx.await;
    };
    let handle = tokio::spawn(async move { serve(listener, shutdown).await });
    (addr, tx, handle)
}

struct TestClient {
    reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
    writer: tokio::net::tcp::OwnedWriteHalf,
}

impl TestClient {
    async fn connect(addr: SocketAddr) -> Self {
        let stream = TcpStream::connect(addr).await.expect("Failed to connect");
        let (read_half, write_half) = stream.into_split();
        Self {
            reader: BufReader::new(read_half),
            writer: write_half,
        }
    }

    async fn send_command(&mut self, args: &[&str]) {
        let mut cmd = format!("*{}\r\n", args.len());
        for arg in args {
            cmd.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
        }
        self.writer.write_all(cmd.as_bytes()).await.unwrap();
    }

    async fn read_integer(&mut self) -> i64 {
        let mut line = String::new();
        self.reader.read_line(&mut line).await.unwrap();
        assert!(line.starts_with(':'), "Expected integer, got: {}", line);
        line.trim_start_matches(':').trim().parse().unwrap()
    }

    async fn read_bulk_string(&mut self) -> Option<String> {
        let mut line = String::new();
        self.reader.read_line(&mut line).await.unwrap();
        if line.starts_with("$-1") {
            return None;
        }
        assert!(line.starts_with('$'), "Expected bulk string, got: {}", line);
        let len: usize = line.trim_start_matches('$').trim().parse().unwrap();
        let mut buf = vec![0u8; len + 2];
        tokio::io::AsyncReadExt::read_exact(&mut self.reader, &mut buf)
            .await
            .unwrap();
        Some(String::from_utf8_lossy(&buf[..len]).to_string())
    }

    async fn read_array_len(&mut self) -> Option<usize> {
        let mut line = String::new();
        self.reader.read_line(&mut line).await.unwrap();
        if line.starts_with("*-1") {
            return None;
        }
        assert!(line.starts_with('*'), "Expected array, got: {}", line);
        Some(line.trim_start_matches('*').trim().parse().unwrap())
    }

    async fn read_error(&mut self) -> String {
        let mut line = String::new();
        self.reader.read_line(&mut line).await.unwrap();
        assert!(line.starts_with('-'), "Expected error, got: {}", line);
        line.trim_start_matches('-').trim().to_string()
    }
}

#[tokio::test]
async fn test_geoadd_and_geopos() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // GEOADD
    client
        .send_command(&[
            "GEOADD",
            "sicily",
            "13.361389",
            "38.115556",
            "Palermo",
            "15.087269",
            "37.502669",
            "Catania",
        ])
        .await;
    assert_eq!(client.read_integer().await, 2);

    // GEOPOS
    client
        .send_command(&["GEOPOS", "sicily", "Palermo", "Catania", "NonExisting"])
        .await;
    let len = client.read_array_len().await.unwrap();
    assert_eq!(len, 3);

    // Palermo position
    let palermo_len = client.read_array_len().await.unwrap();
    assert_eq!(palermo_len, 2);
    let lon = client.read_bulk_string().await.unwrap();
    let lat = client.read_bulk_string().await.unwrap();
    let lon_f: f64 = lon.parse().unwrap();
    let lat_f: f64 = lat.parse().unwrap();
    assert!((lon_f - 13.361389).abs() < 0.01);
    assert!((lat_f - 38.115556).abs() < 0.01);

    // Catania position
    let catania_len = client.read_array_len().await.unwrap();
    assert_eq!(catania_len, 2);
    let _ = client.read_bulk_string().await.unwrap();
    let _ = client.read_bulk_string().await.unwrap();

    // NonExisting returns null bulk (not null array)
    let non_existing = client.read_bulk_string().await;
    assert!(non_existing.is_none());

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_geodist() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // GEOADD
    client
        .send_command(&[
            "GEOADD",
            "sicily",
            "13.361389",
            "38.115556",
            "Palermo",
            "15.087269",
            "37.502669",
            "Catania",
        ])
        .await;
    assert_eq!(client.read_integer().await, 2);

    // GEODIST in meters (default)
    client
        .send_command(&["GEODIST", "sicily", "Palermo", "Catania"])
        .await;
    let dist = client.read_bulk_string().await.unwrap();
    let dist_f: f64 = dist.parse().unwrap();
    // Distance should be around 166 km
    assert!(dist_f > 160000.0 && dist_f < 170000.0);

    // GEODIST in km
    client
        .send_command(&["GEODIST", "sicily", "Palermo", "Catania", "km"])
        .await;
    let dist_km = client.read_bulk_string().await.unwrap();
    let dist_km_f: f64 = dist_km.parse().unwrap();
    assert!(dist_km_f > 160.0 && dist_km_f < 170.0);

    // GEODIST with non-existing member
    client
        .send_command(&["GEODIST", "sicily", "Palermo", "NonExisting"])
        .await;
    let result = client.read_bulk_string().await;
    assert!(result.is_none());

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_geohash() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // GEOADD
    client
        .send_command(&[
            "GEOADD",
            "sicily",
            "13.361389",
            "38.115556",
            "Palermo",
            "15.087269",
            "37.502669",
            "Catania",
        ])
        .await;
    assert_eq!(client.read_integer().await, 2);

    // GEOHASH
    client
        .send_command(&["GEOHASH", "sicily", "Palermo", "Catania", "NonExisting"])
        .await;
    let len = client.read_array_len().await.unwrap();
    assert_eq!(len, 3);

    // Palermo hash (should be 11 characters)
    let palermo_hash = client.read_bulk_string().await.unwrap();
    assert_eq!(palermo_hash.len(), 11);

    // Catania hash
    let catania_hash = client.read_bulk_string().await.unwrap();
    assert_eq!(catania_hash.len(), 11);

    // NonExisting returns null
    let non_existing = client.read_bulk_string().await;
    assert!(non_existing.is_none());

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_geo_wrongtype() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // Set a string key
    client.send_command(&["SET", "mykey", "hello"]).await;
    let mut line = String::new();
    client.reader.read_line(&mut line).await.unwrap();

    // GEOADD on string key should fail
    client
        .send_command(&["GEOADD", "mykey", "13.361389", "38.115556", "Palermo"])
        .await;
    let err = client.read_error().await;
    assert!(err.contains("WRONGTYPE"));

    // GEOPOS on string key should fail
    client.send_command(&["GEOPOS", "mykey", "Palermo"]).await;
    let err = client.read_error().await;
    assert!(err.contains("WRONGTYPE"));

    // GEODIST on string key should fail
    client
        .send_command(&["GEODIST", "mykey", "Palermo", "Catania"])
        .await;
    let err = client.read_error().await;
    assert!(err.contains("WRONGTYPE"));

    // GEOHASH on string key should fail
    client.send_command(&["GEOHASH", "mykey", "Palermo"]).await;
    let err = client.read_error().await;
    assert!(err.contains("WRONGTYPE"));

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_geo_missing_key() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // GEOPOS on missing key
    client
        .send_command(&["GEOPOS", "nonexistent", "member"])
        .await;
    let len = client.read_array_len().await.unwrap();
    assert_eq!(len, 1);
    // Returns null bulk for missing member
    let result = client.read_bulk_string().await;
    assert!(result.is_none());

    // GEODIST on missing key
    client
        .send_command(&["GEODIST", "nonexistent", "m1", "m2"])
        .await;
    let result = client.read_bulk_string().await;
    assert!(result.is_none());

    // GEOHASH on missing key
    client
        .send_command(&["GEOHASH", "nonexistent", "member"])
        .await;
    let len = client.read_array_len().await.unwrap();
    assert_eq!(len, 1);
    let result = client.read_bulk_string().await;
    assert!(result.is_none());

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_geoadd_invalid_coords() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // Invalid longitude (> 180)
    client
        .send_command(&["GEOADD", "geo", "200", "38.0", "invalid"])
        .await;
    let err = client.read_error().await;
    assert!(err.contains("invalid longitude"));

    // Invalid latitude (> 85.05112878)
    client
        .send_command(&["GEOADD", "geo", "13.0", "90.0", "invalid"])
        .await;
    let err = client.read_error().await;
    assert!(err.contains("invalid longitude"));

    // Valid coords should work
    client
        .send_command(&["GEOADD", "geo", "13.0", "38.0", "valid"])
        .await;
    assert_eq!(client.read_integer().await, 1);

    let _ = shutdown.send(());
}

// ==================== GEOSEARCH Tests ====================

#[tokio::test]
async fn test_geosearch_byradius_fromlonlat() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // Add some cities
    client
        .send_command(&["GEOADD", "cities", "13.361389", "52.519444", "Berlin"])
        .await;
    assert_eq!(client.read_integer().await, 1);

    client
        .send_command(&["GEOADD", "cities", "2.349014", "48.864716", "Paris"])
        .await;
    assert_eq!(client.read_integer().await, 1);

    client
        .send_command(&["GEOADD", "cities", "-0.127758", "51.507351", "London"])
        .await;
    assert_eq!(client.read_integer().await, 1);

    // Search within 500km of Berlin
    client
        .send_command(&["GEOSEARCH", "cities", "FROMLONLAT", "13.361389", "52.519444", "BYRADIUS", "500", "KM"])
        .await;
    let arr_len = client.read_array_len().await.unwrap();
    assert_eq!(arr_len, 1); // Only Berlin within 500km of itself
    // Consume the member name
    let _ = client.read_bulk_string().await;

    // Search within 1000km of Berlin
    client
        .send_command(&["GEOSEARCH", "cities", "FROMLONLAT", "13.361389", "52.519444", "BYRADIUS", "1000", "KM"])
        .await;
    let arr_len = client.read_array_len().await.unwrap();
    assert!(arr_len >= 1); // At least Berlin
    // Consume all member names
    for _ in 0..arr_len {
        let _ = client.read_bulk_string().await;
    }

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_geosearch_frommember() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // Add some cities
    client
        .send_command(&["GEOADD", "cities", "13.361389", "52.519444", "Berlin"])
        .await;
    assert_eq!(client.read_integer().await, 1);

    client
        .send_command(&["GEOADD", "cities", "2.349014", "48.864716", "Paris"])
        .await;
    assert_eq!(client.read_integer().await, 1);

    // Search from Berlin
    client
        .send_command(&["GEOSEARCH", "cities", "FROMMEMBER", "Berlin", "BYRADIUS", "1500", "KM"])
        .await;
    let arr_len = client.read_array_len().await.unwrap();
    assert!(arr_len >= 1); // At least Berlin itself

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_geosearch_withdist() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // Add a city
    client
        .send_command(&["GEOADD", "cities", "13.361389", "52.519444", "Berlin"])
        .await;
    assert_eq!(client.read_integer().await, 1);

    // Search with WITHDIST
    client
        .send_command(&["GEOSEARCH", "cities", "FROMLONLAT", "13.361389", "52.519444", "BYRADIUS", "100", "KM", "WITHDIST"])
        .await;
    let arr_len = client.read_array_len().await.unwrap();
    assert_eq!(arr_len, 1);

    // Each result is [member, dist]
    let sub_len = client.read_array_len().await.unwrap();
    assert_eq!(sub_len, 2);
    let member = client.read_bulk_string().await.unwrap();
    assert_eq!(member, "Berlin");
    let dist = client.read_bulk_string().await.unwrap();
    // Distance should be very small (near 0)
    let dist_f: f64 = dist.parse().unwrap();
    assert!(dist_f < 1.0);

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_geosearch_withcoord() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // Add a city
    client
        .send_command(&["GEOADD", "cities", "13.361389", "52.519444", "Berlin"])
        .await;
    assert_eq!(client.read_integer().await, 1);

    // Search with WITHCOORD
    client
        .send_command(&["GEOSEARCH", "cities", "FROMLONLAT", "13.361389", "52.519444", "BYRADIUS", "100", "KM", "WITHCOORD"])
        .await;
    let arr_len = client.read_array_len().await.unwrap();
    assert_eq!(arr_len, 1);

    // Each result is [member, [lon, lat]]
    let sub_len = client.read_array_len().await.unwrap();
    assert_eq!(sub_len, 2);
    let member = client.read_bulk_string().await.unwrap();
    assert_eq!(member, "Berlin");
    let coord_len = client.read_array_len().await.unwrap();
    assert_eq!(coord_len, 2);
    let lon = client.read_bulk_string().await.unwrap();
    let lat = client.read_bulk_string().await.unwrap();
    let lon_f: f64 = lon.parse().unwrap();
    let lat_f: f64 = lat.parse().unwrap();
    // Should be close to original coords
    assert!((lon_f - 13.361389).abs() < 0.01);
    assert!((lat_f - 52.519444).abs() < 0.01);

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_geosearch_count() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // Add multiple cities
    client
        .send_command(&["GEOADD", "cities", "13.361389", "52.519444", "Berlin"])
        .await;
    assert_eq!(client.read_integer().await, 1);

    client
        .send_command(&["GEOADD", "cities", "2.349014", "48.864716", "Paris"])
        .await;
    assert_eq!(client.read_integer().await, 1);

    client
        .send_command(&["GEOADD", "cities", "-0.127758", "51.507351", "London"])
        .await;
    assert_eq!(client.read_integer().await, 1);

    // Search with COUNT 1
    client
        .send_command(&["GEOSEARCH", "cities", "FROMLONLAT", "0", "50", "BYRADIUS", "2000", "KM", "COUNT", "1"])
        .await;
    let arr_len = client.read_array_len().await.unwrap();
    assert_eq!(arr_len, 1);

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_geosearch_bybox() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // Add a city
    client
        .send_command(&["GEOADD", "cities", "13.361389", "52.519444", "Berlin"])
        .await;
    assert_eq!(client.read_integer().await, 1);

    // Search with BYBOX
    client
        .send_command(&["GEOSEARCH", "cities", "FROMLONLAT", "13.0", "52.0", "BYBOX", "200", "200", "KM"])
        .await;
    let arr_len = client.read_array_len().await.unwrap();
    assert_eq!(arr_len, 1);
    let member = client.read_bulk_string().await.unwrap();
    assert_eq!(member, "Berlin");

    let _ = shutdown.send(());
}

#[tokio::test]
async fn test_geosearch_missing_key() {
    let (addr, shutdown, _handle) = spawn_server().await;
    let mut client = TestClient::connect(addr).await;

    // Search on non-existent key should return empty array
    client
        .send_command(&["GEOSEARCH", "nokey", "FROMLONLAT", "0", "0", "BYRADIUS", "100", "KM"])
        .await;
    let arr_len = client.read_array_len().await.unwrap();
    assert_eq!(arr_len, 0);

    let _ = shutdown.send(());
}
