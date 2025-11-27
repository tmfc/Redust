use tokio::io::{self, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

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

pub async fn respond_bulk_string(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    value: &str,
) -> io::Result<()> {
    let response = format!("${}\r\n{}\r\n", value.len(), value);
    writer.write_all(response.as_bytes()).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncWriteExt, BufReader};
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn parses_simple_resp_array() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client = tokio::spawn(async move {
            let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            stream
                .write_all(b"*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n")
                .await
                .unwrap();
        });

        let (stream, _) = listener.accept().await.unwrap();
        let (read_half, _write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);

        let parts = read_resp_array(&mut reader).await.unwrap().unwrap();
        assert_eq!(parts, vec!["ECHO".to_string(), "hello".to_string()]);

        client.await.unwrap();
    }
}
