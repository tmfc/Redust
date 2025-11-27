use tokio::io::{self, BufReader};

use crate::resp::read_resp_array;

#[derive(Debug)]
pub enum Command {
    Ping,
    Echo(String),
    Quit,
    Set { key: String, value: String },
    Get { key: String },
    Del { keys: Vec<String> },
    Exists { keys: Vec<String> },
    Incr { key: String },
    Decr { key: String },
    Type { key: String },
    Keys { pattern: String },
    Unknown(Vec<String>),
}

pub async fn read_command(
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
        "SET" => {
            let Some(key) = iter.next() else {
                return Ok(Some(Command::Unknown(std::iter::once(command).chain(iter).collect())));
            };
            let Some(value) = iter.next() else {
                return Ok(Some(Command::Unknown(std::iter::once(command).chain(std::iter::once(key)).chain(iter).collect())));
            };
            Command::Set { key, value }
        }
        "GET" => {
            let Some(key) = iter.next() else {
                return Ok(Some(Command::Unknown(std::iter::once(command).chain(iter).collect())));
            };
            Command::Get { key }
        }
        "INCR" => {
            let Some(key) = iter.next() else {
                return Ok(Some(Command::Unknown(std::iter::once(command).chain(iter).collect())));
            };
            Command::Incr { key }
        }
        "DECR" => {
            let Some(key) = iter.next() else {
                return Ok(Some(Command::Unknown(std::iter::once(command).chain(iter).collect())));
            };
            Command::Decr { key }
        }
        "DEL" => {
            let keys: Vec<String> = iter.collect();
            if keys.is_empty() {
                Command::Unknown(vec![command])
            } else {
                Command::Del { keys }
            }
        }
        "EXISTS" => {
            let keys: Vec<String> = iter.collect();
            if keys.is_empty() {
                Command::Unknown(vec![command])
            } else {
                Command::Exists { keys }
            }
        }
        "TYPE" => {
            let Some(key) = iter.next() else {
                return Ok(Some(Command::Unknown(std::iter::once(command).chain(iter).collect())));
            };
            Command::Type { key }
        }
        "KEYS" => {
            let Some(pattern) = iter.next() else {
                return Ok(Some(Command::Unknown(std::iter::once(command).chain(iter).collect())));
            };
            Command::Keys { pattern }
        }
        _ => Command::Unknown(std::iter::once(command).chain(iter).collect()),
    };

    Ok(Some(cmd))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncWriteExt, BufReader};
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn parses_basic_commands() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client = tokio::spawn(async move {
            let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            stream
                .write_all(b"*1\r\n$4\r\nPING\r\n")
                .await
                .unwrap();
            stream
                .write_all(b"*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n")
                .await
                .unwrap();
            stream
                .write_all(b"*1\r\n$4\r\nQUIT\r\n")
                .await
                .unwrap();
            stream
                .write_all(b"*2\r\n$7\r\nCOMMAND\r\n$4\r\nDOCS\r\n")
                .await
                .unwrap();
        });

        let (stream, _) = listener.accept().await.unwrap();
        let (read_half, _) = stream.into_split();
        let mut reader = BufReader::new(read_half);

        if let Some(cmd) = read_command(&mut reader).await.unwrap() {
            assert!(matches!(cmd, Command::Ping));
        } else {
            panic!("expected PING command");
        }

        if let Some(cmd) = read_command(&mut reader).await.unwrap() {
            if let Command::Echo(value) = cmd {
                assert_eq!(value, "hello");
            } else {
                panic!("expected ECHO command");
            }
        } else {
            panic!("expected ECHO command");
        }

        if let Some(cmd) = read_command(&mut reader).await.unwrap() {
            assert!(matches!(cmd, Command::Quit));
        } else {
            panic!("expected QUIT command");
        }

        if let Some(cmd) = read_command(&mut reader).await.unwrap() {
            if let Command::Unknown(parts) = cmd {
                assert_eq!(parts, vec!["COMMAND".to_string(), "DOCS".to_string()]);
            } else {
                panic!("expected Unknown command");
            }
        } else {
            panic!("expected Unknown command");
        }

        client.await.unwrap();
    }
}
