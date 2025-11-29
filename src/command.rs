use tokio::io::{self, BufReader};
use std::fmt;

use crate::resp::read_resp_array;

/// Custom error type for command parsing.
#[derive(Debug)]
pub enum CommandError {
    Io(io::Error),
    /// Represents a Redis-like command error, e.g., "ERR wrong number of arguments".
    RedisError(String),
}

impl From<io::Error> for CommandError {
    fn from(err: io::Error) -> Self {
        CommandError::Io(err)
    }
}

impl fmt::Display for CommandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommandError::Io(err) => write!(f, "IO Error: {}", err),
            CommandError::RedisError(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for CommandError {}


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
    Sdiff { keys: Vec<String> },
    Unknown(Vec<String>),
    /// Represents an error that should be sent back to the client.
    Error(String),
}

pub async fn read_command(
    reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>,
) -> Result<Option<Command>, CommandError> {
    let Some(parts) = read_resp_array(reader).await? else {
        return Ok(None);
    };

    let mut iter = parts.into_iter();
    let Some(command) = iter.next() else {
        return Ok(None);
    };

    let upper = command.to_ascii_uppercase();
    let cmd = match upper.as_str() {
        "PING" => {
            if iter.next().is_some() {
                return Ok(Some(Command::Error("ERR wrong number of arguments for 'ping' command".to_string())));
            }
            Command::Ping
        }
        "ECHO" => {
            let Some(value) = iter.next() else {
                return Ok(Some(Command::Error("ERR wrong number of arguments for 'echo' command".to_string())));
            };
            if iter.next().is_some() {
                return Ok(Some(Command::Error("ERR wrong number of arguments for 'echo' command".to_string())));
            }
            Command::Echo(value)
        }
        "QUIT" => {
            if iter.next().is_some() {
                return Ok(Some(Command::Error("ERR wrong number of arguments for 'quit' command".to_string())));
            }
            Command::Quit
        }
        "SET" => {
            let Some(key) = iter.next() else {
                return Ok(Some(Command::Error("ERR wrong number of arguments for 'set' command".to_string())));
            };
            let Some(value) = iter.next() else {
                return Ok(Some(Command::Error("ERR wrong number of arguments for 'set' command".to_string())));
            };
            if iter.next().is_some() {
                return Ok(Some(Command::Error("ERR wrong number of arguments for 'set' command".to_string())));
            }
            Command::Set { key, value }
        }
        "GET" => {
            let Some(key) = iter.next() else {
                return Ok(Some(Command::Error("ERR wrong number of arguments for 'get' command".to_string())));
            };
            if iter.next().is_some() {
                return Ok(Some(Command::Error("ERR wrong number of arguments for 'get' command".to_string())));
            }
            Command::Get { key }
        }
        "INCR" => {
            let Some(key) = iter.next() else {
                return Ok(Some(Command::Error("ERR wrong number of arguments for 'incr' command".to_string())));
            };
            if iter.next().is_some() {
                return Ok(Some(Command::Error("ERR wrong number of arguments for 'incr' command".to_string())));
            }
            Command::Incr { key }
        }
        "DECR" => {
            let Some(key) = iter.next() else {
                return Ok(Some(Command::Error("ERR wrong number of arguments for 'decr' command".to_string())));
            };
            if iter.next().is_some() {
                return Ok(Some(Command::Error("ERR wrong number of arguments for 'decr' command".to_string())));
            }
            Command::Decr { key }
        }
        "DEL" => {
            let keys: Vec<String> = iter.collect();
            if keys.is_empty() {
                return Ok(Some(Command::Error("ERR wrong number of arguments for 'del' command".to_string())));
            }
            Command::Del { keys }
        }
        "EXISTS" => {
            let keys: Vec<String> = iter.collect();
            if keys.is_empty() {
                return Ok(Some(Command::Error("ERR wrong number of arguments for 'exists' command".to_string())));
            }
            Command::Exists { keys }
        }
        "TYPE" => {
            let Some(key) = iter.next() else {
                return Ok(Some(Command::Error("ERR wrong number of arguments for 'type' command".to_string())));
            };
            if iter.next().is_some() {
                return Ok(Some(Command::Error("ERR wrong number of arguments for 'type' command".to_string())));
            }
            Command::Type { key }
        }
        "KEYS" => {
            let Some(pattern) = iter.next() else {
                return Ok(Some(Command::Error("ERR wrong number of arguments for 'keys' command".to_string())));
            };
            if iter.next().is_some() {
                return Ok(Some(Command::Error("ERR wrong number of arguments for 'keys' command".to_string())));
            }
            Command::Keys { pattern }
        }
        "LPUSH" => {
            let Some(key) = iter.next() else {
                return Ok(Some(Command::Error("ERR wrong number of arguments for 'lpush' command".to_string())));
            };
            let values: Vec<String> = iter.collect();
            if values.is_empty() {
                return Ok(Some(Command::Error("ERR wrong number of arguments for 'lpush' command".to_string())));
            }
            Command::Lpush { key, values }
        }
        "RPUSH" => {
            let Some(key) = iter.next() else {
                return Ok(Some(Command::Error("ERR wrong number of arguments for 'rpush' command".to_string())));
            };
            let values: Vec<String> = iter.collect();
            if values.is_empty() {
                return Ok(Some(Command::Error("ERR wrong number of arguments for 'rpush' command".to_string())));
            }
            Command::Rpush { key, values }
        }
        "LPOP" => {
            let Some(key) = iter.next() else {
                return Ok(Some(Command::Error("ERR wrong number of arguments for 'lpop' command".to_string())));
            };
            if iter.next().is_some() {
                return Ok(Some(Command::Error("ERR wrong number of arguments for 'lpop' command".to_string())));
            }
            Command::Lpop { key }
        }
        "RPOP" => {
            let Some(key) = iter.next() else {
                return Ok(Some(Command::Error("ERR wrong number of arguments for 'rpop' command".to_string())));
            };
            if iter.next().is_some() {
                return Ok(Some(Command::Error("ERR wrong number of arguments for 'rpop' command".to_string())));
            }
            Command::Rpop { key }
        }
        "LRANGE" => {
            let Some(key) = iter.next() else {
                return Ok(Some(Command::Error("ERR wrong number of arguments for 'lrange' command".to_string())));
            };
            let Some(start_s) = iter.next() else {
                return Ok(Some(Command::Error("ERR wrong number of arguments for 'lrange' command".to_string())));
            };
            let Some(stop_s) = iter.next() else {
                return Ok(Some(Command::Error("ERR wrong number of arguments for 'lrange' command".to_string())));
            };
            if iter.next().is_some() {
                return Ok(Some(Command::Error("ERR wrong number of arguments for 'lrange' command".to_string())));
            }
            let Ok(start) = start_s.parse::<isize>() else {
                return Ok(Some(Command::Error("ERR value is not an integer or out of range".to_string())));
            };
            let Ok(stop) = stop_s.parse::<isize>() else {
                return Ok(Some(Command::Error("ERR value is not an integer or out of range".to_string())));
            };
            Command::Lrange { key, start, stop }
        }
        "SADD" => {
            let Some(key) = iter.next() else {
                return Ok(Some(Command::Error("ERR wrong number of arguments for 'sadd' command".to_string())));
            };
            let members: Vec<String> = iter.collect();
            if members.is_empty() {
                return Ok(Some(Command::Error("ERR wrong number of arguments for 'sadd' command".to_string())));
            }
            Command::Sadd { key, members }
        }
        "SREM" => {
            let Some(key) = iter.next() else {
                return Ok(Some(Command::Error("ERR wrong number of arguments for 'srem' command".to_string())));
            };
            let members: Vec<String> = iter.collect();
            if members.is_empty() {
                return Ok(Some(Command::Error("ERR wrong number of arguments for 'srem' command".to_string())));
            }
            Command::Srem { key, members }
        }
        "SMEMBERS" => {
            let Some(key) = iter.next() else {
                return Ok(Some(Command::Error("ERR wrong number of arguments for 'smembers' command".to_string())));
            };
            if iter.next().is_some() {
                return Ok(Some(Command::Error("ERR wrong number of arguments for 'smembers' command".to_string())));
            }
            Command::Smembers { key }
        }
        "SCARD" => {
            let Some(key) = iter.next() else {
                return Ok(Some(Command::Error("ERR wrong number of arguments for 'scard' command".to_string())));
            };
            if iter.next().is_some() {
                return Ok(Some(Command::Error("ERR wrong number of arguments for 'scard' command".to_string())));
            }
            Command::Scard { key }
        }
        "SISMEMBER" => {
            let Some(key) = iter.next() else {
                return Ok(Some(Command::Error("ERR wrong number of arguments for 'sismember' command".to_string())));
            };
            let Some(member) = iter.next() else {
                return Ok(Some(Command::Error("ERR wrong number of arguments for 'sismember' command".to_string())));
            };
            if iter.next().is_some() {
                return Ok(Some(Command::Error("ERR wrong number of arguments for 'sismember' command".to_string())));
            }
            Command::Sismember { key, member }
        }
        "SUNION" => {
            let keys: Vec<String> = iter.collect();
            if keys.is_empty() {
                return Ok(Some(Command::Error("ERR wrong number of arguments for 'sunion' command".to_string())));
            }
            Command::Sunion { keys }
        }
        "SINTER" => {
            let keys: Vec<String> = iter.collect();
            if keys.is_empty() {
                return Ok(Some(Command::Error("ERR wrong number of arguments for 'sinter' command".to_string())));
            }
            Command::Sinter { keys }
        }
        "SDIFF" => {
            let keys: Vec<String> = iter.collect();
            if keys.is_empty() {
                return Ok(Some(Command::Error("ERR wrong number of arguments for 'sdiff' command".to_string())));
            }
            Command::Sdiff { keys }
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
            // Test too many arguments for PING
            stream
                .write_all(b"*2\r\n$4\r\nPING\r\n$1\r\nX\r\n")
                .await
                .unwrap();
            // Test too few arguments for ECHO
            stream
                .write_all(b"*1\r\n$4\r\nECHO\r\n")
                .await
                .unwrap();
            // Test too many arguments for ECHO
            stream
                .write_all(b"*3\r\n$4\r\nECHO\r\n$5\r\nhello\r\n$5\r\nworld\r\n")
                .await
                .unwrap();
        });

        let (stream, _) = listener.accept().await.unwrap();
        let (read_half, _) = stream.into_split();
        let mut reader = BufReader::new(read_half);

        // PING
        if let Some(cmd) = read_command(&mut reader).await.unwrap() {
            assert!(matches!(cmd, Command::Ping));
        } else {
            panic!("expected PING command");
        }

        // ECHO hello
        if let Some(cmd) = read_command(&mut reader).await.unwrap() {
            if let Command::Echo(value) = cmd {
                assert_eq!(value, "hello");
            } else {
                panic!("expected ECHO command");
            }
        } else {
            panic!("expected ECHO command");
        }

        // QUIT
        if let Some(cmd) = read_command(&mut reader).await.unwrap() {
            assert!(matches!(cmd, Command::Quit));
        } else {
            panic!("expected QUIT command");
        }

        // COMMAND DOCS
        if let Some(cmd) = read_command(&mut reader).await.unwrap() {
            if let Command::Unknown(parts) = cmd {
                assert_eq!(parts, vec!["COMMAND".to_string(), "DOCS".to_string()]);
            } else {
                panic!("expected Unknown command");
            }
        } else {
            panic!("expected Unknown command");
        }

        // PING X (Error)
        if let Some(cmd) = read_command(&mut reader).await.unwrap() {
            if let Command::Error(msg) = cmd {
                assert_eq!(msg, "ERR wrong number of arguments for 'ping' command");
            } else {
                panic!("expected Command::Error for too many PING arguments");
            }
        } else {
            panic!("expected PING error command");
        }

        // ECHO (Error)
        if let Some(cmd) = read_command(&mut reader).await.unwrap() {
            if let Command::Error(msg) = cmd {
                assert_eq!(msg, "ERR wrong number of arguments for 'echo' command");
            } else {
                panic!("expected Command::Error for too few ECHO arguments");
            }
        } else {
            panic!("expected ECHO error command");
        }

        // ECHO hello world (Error)
        if let Some(cmd) = read_command(&mut reader).await.unwrap() {
            if let Command::Error(msg) = cmd {
                assert_eq!(msg, "ERR wrong number of arguments for 'echo' command");
            } else {
                panic!("expected Command::Error for too many ECHO arguments");
            }
        } else {
            panic!("expected ECHO error command");
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
            // LPUSH mylist (Error)
            stream
                .write_all(b"*2\r\n$5\r\nLPUSH\r\n$6\r\nmylist\r\n")
                .await
                .unwrap();
            // LPOP (Error)
            stream
                .write_all(b"*1\r\n$4\r\nLPOP\r\n")
                .await
                .unwrap();
            // LPOP mylist X (Error)
            stream
                .write_all(b"*3\r\n$4\r\nLPOP\r\n$6\r\nmylist\r\n$1\r\nX\r\n")
                .await
                .unwrap();
        });

        let (stream, _) = listener.accept().await.unwrap();
        let (read_half, _) = stream.into_split();
        let mut reader = BufReader::new(read_half);

        // LPUSH mylist a b
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

        // RPUSH mylist c
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

        // LRANGE mylist 0 -1
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

        // LPUSH mylist (Error)
        if let Some(cmd) = read_command(&mut reader).await.unwrap() {
            if let Command::Error(msg) = cmd {
                assert_eq!(msg, "ERR wrong number of arguments for 'lpush' command");
            } else {
                panic!("expected Command::Error for too few LPUSH arguments");
            }
        } else {
            panic!("expected LPUSH error command");
        }

        // LPOP (Error)
        if let Some(cmd) = read_command(&mut reader).await.unwrap() {
            if let Command::Error(msg) = cmd {
                assert_eq!(msg, "ERR wrong number of arguments for 'lpop' command");
            } else {
                panic!("expected Command::Error for too few LPOP arguments");
            }
        } else {
            panic!("expected LPOP error command");
        }

        // LPOP mylist X (Error)
        if let Some(cmd) = read_command(&mut reader).await.unwrap() {
            if let Command::Error(msg) = cmd {
                assert_eq!(msg, "ERR wrong number of arguments for 'lpop' command");
            } else {
                panic!("expected Command::Error for too many LPOP arguments");
            }
        } else {
            panic!("expected LPOP error command");
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
            // SADD myset (Error)
            stream
                .write_all(b"*2\r\n$4\r\nSADD\r\n$5\r\nmyset\r\n")
                .await
                .unwrap();
            // SMEMBERS (Error)
            stream
                .write_all(b"*1\r\n$8\r\nSMEMBERS\r\n")
                .await
                .unwrap();
            // SMEMBERS myset X (Error)
            stream
                .write_all(b"*3\r\n$8\r\nSMEMBERS\r\n$5\r\nmyset\r\n$1\r\nX\r\n")
                .await
                .unwrap();
        });

        let (stream, _) = listener.accept().await.unwrap();
        let (read_half, _) = stream.into_split();
        let mut reader = BufReader::new(read_half);

        // SADD myset a b
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

        // SCARD myset
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

        // SISMEMBER myset a
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

        // SADD myset (Error)
        if let Some(cmd) = read_command(&mut reader).await.unwrap() {
            if let Command::Error(msg) = cmd {
                assert_eq!(msg, "ERR wrong number of arguments for 'sadd' command");
            } else {
                panic!("expected Command::Error for too few SADD arguments");
            }
        } else {
            panic!("expected SADD error command");
        }

        // SMEMBERS (Error)
        if let Some(cmd) = read_command(&mut reader).await.unwrap() {
            if let Command::Error(msg) = cmd {
                assert_eq!(msg, "ERR wrong number of arguments for 'smembers' command");
            } else {
                panic!("expected Command::Error for too few SMEMBERS arguments");
            }
        } else {
            panic!("expected SMEMBERS error command");
        }

        // SMEMBERS myset X (Error)
        if let Some(cmd) = read_command(&mut reader).await.unwrap() {
            if let Command::Error(msg) = cmd {
                assert_eq!(msg, "ERR wrong number of arguments for 'smembers' command");
            } else {
                panic!("expected Command::Error for too many SMEMBERS arguments");
            }
        } else {
            panic!("expected SMEMBERS error command");
        }

        client.await.unwrap();
    }
}

