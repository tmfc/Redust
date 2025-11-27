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
    Lpush { key: String, values: Vec<String> },
    Rpush { key: String, values: Vec<String> },
    Lrange { key: String, start: isize, stop: isize },
    Lpop { key: String },
    Rpop { key: String },
    Sadd { key: String, members: Vec<String> },
    Srem { key: String, members: Vec<String> },
    Smembers { key: String },
    Scard { key: String },
    Sismember { key: String, member: String },
    Sunion { keys: Vec<String> },
    Sinter { keys: Vec<String> },
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
        "LPUSH" => {
            let Some(key) = iter.next() else {
                return Ok(Some(Command::Unknown(std::iter::once(command).chain(iter).collect())));
            };
            let values: Vec<String> = iter.collect();
            if values.is_empty() {
                Command::Unknown(vec![command, key])
            } else {
                Command::Lpush { key, values }
            }
        }
        "RPUSH" => {
            let Some(key) = iter.next() else {
                return Ok(Some(Command::Unknown(std::iter::once(command).chain(iter).collect())));
            };
            let values: Vec<String> = iter.collect();
            if values.is_empty() {
                Command::Unknown(vec![command, key])
            } else {
                Command::Rpush { key, values }
            }
        }
        "LPOP" => {
            let Some(key) = iter.next() else {
                return Ok(Some(Command::Unknown(std::iter::once(command).chain(iter).collect())));
            };
            Command::Lpop { key }
        }
        "RPOP" => {
            let Some(key) = iter.next() else {
                return Ok(Some(Command::Unknown(std::iter::once(command).chain(iter).collect())));
            };
            Command::Rpop { key }
        }
        "LRANGE" => {
            let Some(key) = iter.next() else {
                return Ok(Some(Command::Unknown(std::iter::once(command).chain(iter).collect())));
            };
            let Some(start_s) = iter.next() else {
                return Ok(Some(Command::Unknown(std::iter::once(command).chain(std::iter::once(key)).chain(iter).collect())));
            };
            let Some(stop_s) = iter.next() else {
                return Ok(Some(Command::Unknown(std::iter::once(command).chain(std::iter::once(key)).chain(std::iter::once(start_s)).chain(iter).collect())));
            };
            let Ok(start) = start_s.parse::<isize>() else {
                return Ok(Some(Command::Unknown(std::iter::once(command).chain(std::iter::once(key)).chain(std::iter::once(start_s)).chain(std::iter::once(stop_s)).chain(iter).collect())));
            };
            let Ok(stop) = stop_s.parse::<isize>() else {
                return Ok(Some(Command::Unknown(std::iter::once(command).chain(std::iter::once(key)).chain(std::iter::once(start_s)).chain(std::iter::once(stop_s)).chain(iter).collect())));
            };
            Command::Lrange { key, start, stop }
        }
        "SADD" => {
            let Some(key) = iter.next() else {
                return Ok(Some(Command::Unknown(std::iter::once(command).chain(iter).collect())));
            };
            let members: Vec<String> = iter.collect();
            if members.is_empty() {
                Command::Unknown(vec![command, key])
            } else {
                Command::Sadd { key, members }
            }
        }
        "SREM" => {
            let Some(key) = iter.next() else {
                return Ok(Some(Command::Unknown(std::iter::once(command).chain(iter).collect())));
            };
            let members: Vec<String> = iter.collect();
            if members.is_empty() {
                Command::Unknown(vec![command, key])
            } else {
                Command::Srem { key, members }
            }
        }
        "SMEMBERS" => {
            let Some(key) = iter.next() else {
                return Ok(Some(Command::Unknown(std::iter::once(command).chain(iter).collect())));
            };
            Command::Smembers { key }
        }
        "SCARD" => {
            let Some(key) = iter.next() else {
                return Ok(Some(Command::Unknown(std::iter::once(command).chain(iter).collect())));
            };
            Command::Scard { key }
        }
        "SISMEMBER" => {
            let Some(key) = iter.next() else {
                return Ok(Some(Command::Unknown(std::iter::once(command).chain(iter).collect())));
            };
            let Some(member) = iter.next() else {
                return Ok(Some(Command::Unknown(std::iter::once(command).chain(std::iter::once(key)).chain(iter).collect())));
            };
            Command::Sismember { key, member }
        }
        "SUNION" => {
            let keys: Vec<String> = iter.collect();
            if keys.is_empty() {
                Command::Unknown(vec![command])
            } else {
                Command::Sunion { keys }
            }
        }
        "SINTER" => {
            let keys: Vec<String> = iter.collect();
            if keys.is_empty() {
                Command::Unknown(vec![command])
            } else {
                Command::Sinter { keys }
            }
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

    #[tokio::test]
    async fn parses_list_commands() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client = tokio::spawn(async move {
            let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            // LPUSH mylist a b
            stream
                .write_all(b"*4\r\n$5\r\nLPUSH\r\n$6\r\nmylist\r\n$1\r\na\r\n$1\r\nb\r\n")
                .await
                .unwrap();
            // RPUSH mylist c
            stream
                .write_all(b"*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$1\r\nc\r\n")
                .await
                .unwrap();
            // LRANGE mylist 0 -1
            stream
                .write_all(b"*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$1\r\n0\r\n$2\r\n-1\r\n")
                .await
                .unwrap();
        });

        let (stream, _) = listener.accept().await.unwrap();
        let (read_half, _) = stream.into_split();
        let mut reader = BufReader::new(read_half);

        if let Some(cmd) = read_command(&mut reader).await.unwrap() {
            match cmd {
                Command::Lpush { key, values } => {
                    assert_eq!(key, "mylist");
                    assert_eq!(values, vec!["a".to_string(), "b".to_string()]);
                }
                _ => panic!("expected LPUSH"),
            }
        } else {
            panic!("expected LPUSH command");
        }

        if let Some(cmd) = read_command(&mut reader).await.unwrap() {
            match cmd {
                Command::Rpush { key, values } => {
                    assert_eq!(key, "mylist");
                    assert_eq!(values, vec!["c".to_string()]);
                }
                _ => panic!("expected RPUSH"),
            }
        } else {
            panic!("expected RPUSH command");
        }

        if let Some(cmd) = read_command(&mut reader).await.unwrap() {
            match cmd {
                Command::Lrange { key, start, stop } => {
                    assert_eq!(key, "mylist");
                    assert_eq!(start, 0);
                    assert_eq!(stop, -1);
                }
                _ => panic!("expected LRANGE"),
            }
        } else {
            panic!("expected LRANGE command");
        }

        client.await.unwrap();
    }

    #[tokio::test]
    async fn parses_set_commands() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client = tokio::spawn(async move {
            let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            // SADD myset a b
            stream
                .write_all(b"*4\r\n$4\r\nSADD\r\n$5\r\nmyset\r\n$1\r\na\r\n$1\r\nb\r\n")
                .await
                .unwrap();
            // SCARD myset
            stream
                .write_all(b"*2\r\n$5\r\nSCARD\r\n$5\r\nmyset\r\n")
                .await
                .unwrap();
            // SISMEMBER myset a
            stream
                .write_all(b"*3\r\n$9\r\nSISMEMBER\r\n$5\r\nmyset\r\n$1\r\na\r\n")
                .await
                .unwrap();
        });

        let (stream, _) = listener.accept().await.unwrap();
        let (read_half, _) = stream.into_split();
        let mut reader = BufReader::new(read_half);

        if let Some(cmd) = read_command(&mut reader).await.unwrap() {
            match cmd {
                Command::Sadd { key, members } => {
                    assert_eq!(key, "myset");
                    assert_eq!(members, vec!["a".to_string(), "b".to_string()]);
                }
                _ => panic!("expected SADD"),
            }
        } else {
            panic!("expected SADD command");
        }

        if let Some(cmd) = read_command(&mut reader).await.unwrap() {
            match cmd {
                Command::Scard { key } => {
                    assert_eq!(key, "myset");
                }
                _ => panic!("expected SCARD"),
            }
        } else {
            panic!("expected SCARD command");
        }

        if let Some(cmd) = read_command(&mut reader).await.unwrap() {
            match cmd {
                Command::Sismember { key, member } => {
                    assert_eq!(key, "myset");
                    assert_eq!(member, "a");
                }
                _ => panic!("expected SISMEMBER"),
            }
        } else {
            panic!("expected SISMEMBER command");
        }

        client.await.unwrap();
    }
}
