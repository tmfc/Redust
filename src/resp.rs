use tokio::io::{self, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

// Redis's default max bulk string size is 512MB.
const MAX_BULK_STRING_SIZE: usize = 512 * 1024 * 1024;
// We'll also limit array sizes to something reasonable, e.g., 1MB elements for an array.
const MAX_ARRAY_SIZE: usize = 1024 * 1024;

pub async fn read_resp_array(
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

    let array_len: usize = header[1..]
        .parse()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("Invalid array length: {}", e)))?;

    if array_len > MAX_ARRAY_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "array length exceeds limit: {} > {}",
                array_len, MAX_ARRAY_SIZE
            ),
        ));
    }

    let mut parts = Vec::with_capacity(array_len);

    for _ in 0..array_len {
        let mut bulk_header = String::new();
        if reader.read_line(&mut bulk_header).await? == 0 {
            return Ok(None);
        }

        let bulk_header = bulk_header.trim_end();
        println!("[resp] bulk header line: {:?}", bulk_header);
        let Some(stripped) = bulk_header.strip_prefix('$') else {
            // This case handles non-bulk string elements in the array, which is not valid RESP
            // for the commands we are parsing. We'll treat it as an error.
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "expected bulk string in array",
            ));
        };

        let bulk_len: usize = stripped
            .parse()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("Invalid bulk string length: {}", e)))?;

        if bulk_len > MAX_BULK_STRING_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "bulk string length exceeds limit: {} > {}",
                    bulk_len, MAX_BULK_STRING_SIZE
                ),
            ));
        }

        let mut buf = vec![0u8; bulk_len];
        reader.read_exact(&mut buf).await?;

        // Consume trailing CRLF after the bulk string
        let mut crlf = [0u8; 2];
        reader.read_exact(&mut crlf).await?;
        if &crlf != b"\r\n" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "expected CRLF after bulk string",
            ));
        }

        let value = String::from_utf8_lossy(&buf).into_owned();
        println!("[resp] bulk value: {:?}", value);
        parts.push(value);
    }

    println!("[resp] parsed array parts: {:?}", parts);
    Ok(Some(parts))
}

pub async fn respond_bulk_string(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    value: &str,
) -> io::Result<()> {
    let response = format!("${}\r\n{}\r\n", value.len(), value);
    writer.write_all(response.as_bytes()).await
}

pub async fn respond_simple_string(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    value: &str,
) -> io::Result<()> {
    let response = format!("+{}\r\n", value);
    writer.write_all(response.as_bytes()).await
}

pub async fn respond_error(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    message: &str,
) -> io::Result<()> {
    let response = format!("-{}\r\n", message);
    writer.write_all(response.as_bytes()).await
}

pub async fn respond_integer(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    value: i64,
) -> io::Result<()> {
    let response = format!(":{}\r\n", value);
    writer.write_all(response.as_bytes()).await
}

pub async fn respond_null_bulk(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
) -> io::Result<()> {
    writer.write_all(b"$-1\r\n").await
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncWriteExt, BufReader};
    use tokio::net::TcpListener;
    use crate::resp::{MAX_ARRAY_SIZE, MAX_BULK_STRING_SIZE};

    async fn setup_test_client(data: Vec<u8>) -> (BufReader<tokio::net::tcp::OwnedReadHalf>, TcpListener) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client = tokio::spawn(async move {
            let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            stream.write_all(&data).await.unwrap();
            stream.shutdown().await.unwrap(); // Close the client stream after writing
        });

        let (stream, _) = listener.accept().await.unwrap();
        let (read_half, _write_half) = stream.into_split();
        let reader = BufReader::new(read_half);
        client.await.unwrap();
        (reader, listener)
    }

    #[tokio::test]
    async fn parses_simple_resp_array() {
        let (mut reader, _listener) = setup_test_client(b"*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n".to_vec()).await;
        let parts = read_resp_array(&mut reader).await.unwrap().unwrap();
        assert_eq!(parts, vec!["ECHO".to_string(), "hello".to_string()]);
    }

    #[tokio::test]
    async fn handles_empty_stream() {
        let (mut reader, _listener) = setup_test_client(Vec::new()).await;
        let parts = read_resp_array(&mut reader).await.unwrap();
        assert!(parts.is_none());
    }

    #[tokio::test]
    async fn parses_non_array_as_single_element() {
        let (mut reader, _listener) = setup_test_client(b"+OK\r\n".to_vec()).await;
        let parts = read_resp_array(&mut reader).await.unwrap().unwrap();
        assert_eq!(parts, vec!["+OK".to_string()]);
    }

    #[tokio::test]
    async fn parses_error_cases() {
        // Non-numeric array length
        let (mut reader, _listener) = setup_test_client(b"*abc\r\n$4\r\nECHO\r\n".to_vec()).await;
        let err = read_resp_array(&mut reader).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("Invalid array length"));

        // Excessively large array length
        let oversized_array_header = format!("*{}\r\n", MAX_ARRAY_SIZE + 1);
        let (mut reader, _listener) = setup_test_client(oversized_array_header.into_bytes()).await;
        let err = read_resp_array(&mut reader).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("array length exceeds limit"));

        // Non-numeric bulk string length
        let (mut reader, _listener) = setup_test_client(b"*1\r\n$abc\r\nhello\r\n".to_vec()).await;
        let err = read_resp_array(&mut reader).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("Invalid bulk string length"));

        // Excessively large bulk string length
        let oversized_bulk_header = format!("*1\r\n${}\r\n", MAX_BULK_STRING_SIZE + 1);
        let (mut reader, _listener) = setup_test_client(oversized_bulk_header.into_bytes()).await;
        let err = read_resp_array(&mut reader).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("bulk string length exceeds limit"));

        // Missing '$' prefix for bulk string
        let (mut reader, _listener) = setup_test_client(b"*1\r\n4\r\nECHO\r\n".to_vec()).await;
        let err = read_resp_array(&mut reader).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("expected bulk string in array"));

        // Malformed CRLF after bulk string (missing second byte)
        let (mut reader, _listener) = setup_test_client(b"*1\r\n$5\r\nhello\r".to_vec()).await;
        let err = read_resp_array(&mut reader).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);

        // Malformed CRLF after bulk string (wrong bytes)
        let (mut reader, _listener) = setup_test_client(b"*1\r\n$5\r\nhello\n\n".to_vec()).await;
        let err = read_resp_array(&mut reader).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("expected CRLF after bulk string"));
    }
}
