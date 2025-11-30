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

/// Represents a Redis command.
#[derive(Debug)]
pub enum Command {
    Ping,
    Echo(String),
    Quit,
    Set {
        key: String,
        value: String,
        expire_millis: Option<i64>,
    },
    Get { key: String },
    Del { keys: Vec<String> },
    Exists { keys: Vec<String> },
    Incr { key: String },
    Decr { key: String },
    Incrby { key: String, delta: i64 },
    Decrby { key: String, delta: i64 },
    Type { key: String },
    Keys { pattern: String },
    Dbsize,
    Lpush { key: String, values: Vec<String> },
    Rpush { key: String, values: Vec<String> },
    Lrange { key: String, start: isize, stop: isize },
    Lpop { key: String },
    Rpop { key: String },
    Llen { key: String },
    Lindex { key: String, index: isize },
    Lrem { key: String, count: isize, value: String },
    Ltrim { key: String, start: isize, stop: isize },
    Sadd { key: String, members: Vec<String> },
    Srem { key: String, members: Vec<String> },
    Smembers { key: String },
    Scard { key: String },
    Sismember { key: String, member: String },
    Sunion { keys: Vec<String> },
    Sinter { keys: Vec<String> },
    Sdiff { keys: Vec<String> },
    Hset { key: String, field: String, value: String },
    Hget { key: String, field: String },
    Hdel { key: String, fields: Vec<String> },
    Hexists { key: String, field: String },
    Hgetall { key: String },
    Expire { key: String, seconds: i64 },
    Pexpire { key: String, millis: i64 },
    Ttl { key: String },
    Pttl { key: String },
    Persist { key: String },
    Info,
    Mget { keys: Vec<String> },
    Mset { pairs: Vec<(String, String)> },
    Setnx { key: String, value: String },
    Setex { key: String, seconds: i64, value: String },
    Psetex { key: String, millis: i64, value: String },
    Unknown(Vec<String>),
    /// Represents an error that should be sent back to the client.
    Error(String),
}

fn err_wrong_args(cmd: &str) -> Command {
    // Redis 错误消息中命令名通常是小写形式
    Command::Error(format!("ERR wrong number of arguments for '{}' command", cmd.to_lowercase()))
}

fn err_not_integer() -> Command {
    Command::Error("ERR value is not an integer or out of range".to_string())
}

fn err_syntax() -> Command {
    Command::Error("ERR syntax error".to_string())
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
                return Ok(Some(err_wrong_args("ping")));
            }
            Command::Ping
        }
        "ECHO" => {
            let Some(value) = iter.next() else {
                return Ok(Some(err_wrong_args("echo")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("echo")));
            }
            Command::Echo(value)
        }
        "QUIT" => {
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("quit")));
            }
            Command::Quit
        }
        "SET" => {
            let Some(key) = iter.next() else {
                return Ok(Some(err_wrong_args("set")));
            };
            let Some(value) = iter.next() else {
                return Ok(Some(err_wrong_args("set")));
            };
            // 解析可选参数，目前只支持 EX seconds / PX milliseconds，忽略大小写
            let mut expire_millis: Option<i64> = None;

            while let Some(opt) = iter.next() {
                let opt_upper = opt.to_ascii_uppercase();
                match opt_upper.as_str() {
                    "EX" => {
                        let Some(sec_str) = iter.next() else {
                            return Ok(Some(err_syntax()));
                        };
                        let Ok(sec) = sec_str.parse::<i64>() else {
                            return Ok(Some(err_not_integer()));
                        };
                        if sec < 0 {
                            return Ok(Some(err_not_integer()));
                        }
                        expire_millis = Some(sec.saturating_mul(1000));
                    }
                    "PX" => {
                        let Some(ms_str) = iter.next() else {
                            return Ok(Some(err_syntax()));
                        };
                        let Ok(ms) = ms_str.parse::<i64>() else {
                            return Ok(Some(err_not_integer()));
                        };
                        if ms < 0 {
                            return Ok(Some(err_not_integer()));
                        }
                        expire_millis = Some(ms);
                    }
                    _ => {
                        return Ok(Some(err_syntax()));
                    }
                }
            }

            Command::Set { key, value, expire_millis }
        }
        "GET" => {
            let Some(key) = iter.next() else {
                return Ok(Some(err_wrong_args("get")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("get")));
            }
            Command::Get { key }
        }
        "INCR" => {
            let Some(key) = iter.next() else {
                return Ok(Some(err_wrong_args("incr")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("incr")));
            }
            Command::Incr { key }
        }
        "DECR" => {
            let Some(key) = iter.next() else {
                return Ok(Some(err_wrong_args("decr")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("decr")));
            }
            Command::Decr { key }
        }
        "INCRBY" => {
            let Some(key) = iter.next() else {
                return Ok(Some(err_wrong_args("incrby")));
            };
            let Some(delta_s) = iter.next() else {
                return Ok(Some(err_wrong_args("incrby")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("incrby")));
            }
            let Ok(delta) = delta_s.parse::<i64>() else {
                return Ok(Some(err_not_integer()));
            };
            Command::Incrby { key, delta }
        }
        "DECRBY" => {
            let Some(key) = iter.next() else {
                return Ok(Some(err_wrong_args("decrby")));
            };
            let Some(delta_s) = iter.next() else {
                return Ok(Some(err_wrong_args("decrby")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("decrby")));
            }
            let Ok(delta) = delta_s.parse::<i64>() else {
                return Ok(Some(err_not_integer()));
            };
            Command::Decrby { key, delta }
        }
        "DEL" => {
            let keys: Vec<String> = iter.collect();
            if keys.is_empty() {
                return Ok(Some(err_wrong_args("del")));
            }
            Command::Del { keys }
        }
        "EXISTS" => {
            let keys: Vec<String> = iter.collect();
            if keys.is_empty() {
                return Ok(Some(err_wrong_args("exists")));
            }
            Command::Exists { keys }
        }
        "TYPE" => {
            let Some(key) = iter.next() else {
                return Ok(Some(err_wrong_args("type")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("type")));
            }
            Command::Type { key }
        }
        "DBSIZE" => {
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("dbsize")));
            }
            Command::Dbsize
        }
        "KEYS" => {
            let Some(pattern) = iter.next() else {
                return Ok(Some(err_wrong_args("keys")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("keys")));
            }
            Command::Keys { pattern }
        }
        "LPUSH" => {
            let Some(key) = iter.next() else {
                return Ok(Some(err_wrong_args("lpush")));
            };
            let values: Vec<String> = iter.collect();
            if values.is_empty() {
                return Ok(Some(err_wrong_args("lpush")));
            }
            Command::Lpush { key, values }
        }
        "RPUSH" => {
            let Some(key) = iter.next() else {
                return Ok(Some(err_wrong_args("rpush")));
            };
            let values: Vec<String> = iter.collect();
            if values.is_empty() {
                return Ok(Some(err_wrong_args("rpush")));
            }
            Command::Rpush { key, values }
        }
        "LPOP" => {
            let Some(key) = iter.next() else {
                return Ok(Some(err_wrong_args("lpop")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("lpop")));
            }
            Command::Lpop { key }
        }
        "RPOP" => {
            let Some(key) = iter.next() else {
                return Ok(Some(err_wrong_args("rpop")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("rpop")));
            }
            Command::Rpop { key }
        }
        "LRANGE" => {
            let Some(key) = iter.next() else {
                return Ok(Some(err_wrong_args("lrange")));
            };
            let Some(start_s) = iter.next() else {
                return Ok(Some(err_wrong_args("lrange")));
            };
            let Some(stop_s) = iter.next() else {
                return Ok(Some(err_wrong_args("lrange")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("lrange")));
            }
            let Ok(start) = start_s.parse::<isize>() else {
                return Ok(Some(err_not_integer()));
            };
            let Ok(stop) = stop_s.parse::<isize>() else {
                return Ok(Some(err_not_integer()));
            };
            Command::Lrange { key, start, stop }
        }
        "LLEN" => {
            let Some(key) = iter.next() else {
                return Ok(Some(err_wrong_args("llen")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("llen")));
            }
            Command::Llen { key }
        }
        "LINDEX" => {
            let Some(key) = iter.next() else {
                return Ok(Some(err_wrong_args("lindex")));
            };
            let Some(idx_s) = iter.next() else {
                return Ok(Some(err_wrong_args("lindex")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("lindex")));
            }
            let Ok(index) = idx_s.parse::<isize>() else {
                return Ok(Some(err_not_integer()));
            };
            Command::Lindex { key, index }
        }
        "LREM" => {
            let Some(key) = iter.next() else {
                return Ok(Some(err_wrong_args("lrem")));
            };
            let Some(count_s) = iter.next() else {
                return Ok(Some(err_wrong_args("lrem")));
            };
            let Some(value) = iter.next() else {
                return Ok(Some(err_wrong_args("lrem")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("lrem")));
            }
            let Ok(count) = count_s.parse::<isize>() else {
                return Ok(Some(err_not_integer()));
            };
            Command::Lrem { key, count, value }
        }
        "LTRIM" => {
            let Some(key) = iter.next() else {
                return Ok(Some(err_wrong_args("ltrim")));
            };
            let Some(start_s) = iter.next() else {
                return Ok(Some(err_wrong_args("ltrim")));
            };
            let Some(stop_s) = iter.next() else {
                return Ok(Some(err_wrong_args("ltrim")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("ltrim")));
            }
            let Ok(start) = start_s.parse::<isize>() else {
                return Ok(Some(err_not_integer()));
            };
            let Ok(stop) = stop_s.parse::<isize>() else {
                return Ok(Some(err_not_integer()));
            };
            Command::Ltrim { key, start, stop }
        }
        "SADD" => {
            let Some(key) = iter.next() else {
                return Ok(Some(err_wrong_args("sadd")));
            };
            let members: Vec<String> = iter.collect();
            if members.is_empty() {
                return Ok(Some(err_wrong_args("sadd")));
            }
            Command::Sadd { key, members }
        }
        "SREM" => {
            let Some(key) = iter.next() else {
                return Ok(Some(err_wrong_args("srem")));
            };
            let members: Vec<String> = iter.collect();
            if members.is_empty() {
                return Ok(Some(err_wrong_args("srem")));
            }
            Command::Srem { key, members }
        }
        "SMEMBERS" => {
            let Some(key) = iter.next() else {
                return Ok(Some(err_wrong_args("smembers")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("smembers")));
            }
            Command::Smembers { key }
        }
        "SCARD" => {
            let Some(key) = iter.next() else {
                return Ok(Some(err_wrong_args("scard")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("scard")));
            }
            Command::Scard { key }
        }
        "SISMEMBER" => {
            let Some(key) = iter.next() else {
                return Ok(Some(err_wrong_args("sismember")));
            };
            let Some(member) = iter.next() else {
                return Ok(Some(err_wrong_args("sismember")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("sismember")));
            }
            Command::Sismember { key, member }
        }
        "SUNION" => {
            let keys: Vec<String> = iter.collect();
            if keys.is_empty() {
                return Ok(Some(err_wrong_args("sunion")));
            }
            Command::Sunion { keys }
        }
        "SINTER" => {
            let keys: Vec<String> = iter.collect();
            if keys.is_empty() {
                return Ok(Some(err_wrong_args("sinter")));
            }
            Command::Sinter { keys }
        }
        "SDIFF" => {
            let keys: Vec<String> = iter.collect();
            if keys.is_empty() {
                return Ok(Some(err_wrong_args("sdiff")));
            }
            Command::Sdiff { keys }
        }
        "HSET" => {
            let Some(key) = iter.next() else {
                return Ok(Some(err_wrong_args("hset")));
            };
            let Some(field) = iter.next() else {
                return Ok(Some(err_wrong_args("hset")));
            };
            let Some(value) = iter.next() else {
                return Ok(Some(err_wrong_args("hset")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("hset")));
            }
            Command::Hset { key, field, value }
        }
        "HGET" => {
            let Some(key) = iter.next() else {
                return Ok(Some(err_wrong_args("hget")));
            };
            let Some(field) = iter.next() else {
                return Ok(Some(err_wrong_args("hget")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("hget")));
            }
            Command::Hget { key, field }
        }
        "HDEL" => {
            let Some(key) = iter.next() else {
                return Ok(Some(err_wrong_args("hdel")));
            };
            let fields: Vec<String> = iter.collect();
            if fields.is_empty() {
                return Ok(Some(err_wrong_args("hdel")));
            }
            Command::Hdel { key, fields }
        }
        "HEXISTS" => {
            let Some(key) = iter.next() else {
                return Ok(Some(err_wrong_args("hexists")));
            };
            let Some(field) = iter.next() else {
                return Ok(Some(err_wrong_args("hexists")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("hexists")));
            }
            Command::Hexists { key, field }
        }
        "HGETALL" => {
            let Some(key) = iter.next() else {
                return Ok(Some(err_wrong_args("hgetall")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("hgetall")));
            }
            Command::Hgetall { key }
        }
        "EXPIRE" => {
            let Some(key) = iter.next() else {
                return Ok(Some(err_wrong_args("expire")));
            };
            let Some(sec_str) = iter.next() else {
                return Ok(Some(err_wrong_args("expire")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("expire")));
            }
            let Ok(seconds) = sec_str.parse::<i64>() else {
                return Ok(Some(err_not_integer()));
            };
            Command::Expire { key, seconds }
        }
        "PEXPIRE" => {
            let Some(key) = iter.next() else {
                return Ok(Some(err_wrong_args("pexpire")));
            };
            let Some(ms_str) = iter.next() else {
                return Ok(Some(err_wrong_args("pexpire")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("pexpire")));
            }
            let Ok(millis) = ms_str.parse::<i64>() else {
                return Ok(Some(err_not_integer()));
            };
            Command::Pexpire { key, millis }
        }
        "TTL" => {
            let Some(key) = iter.next() else {
                return Ok(Some(err_wrong_args("ttl")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("ttl")));
            }
            Command::Ttl { key }
        }
        "PTTL" => {
            let Some(key) = iter.next() else {
                return Ok(Some(err_wrong_args("pttl")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("pttl")));
            }
            Command::Pttl { key }
        }
        "PERSIST" => {
            let Some(key) = iter.next() else {
                return Ok(Some(err_wrong_args("persist")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("persist")));
            }
            Command::Persist { key }
        }
        "INFO" => {
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("info")));
            }
            Command::Info
        }
        "MGET" => {
            let keys: Vec<String> = iter.collect();
            if keys.is_empty() {
                return Ok(Some(err_wrong_args("mget")));
            }
            Command::Mget { keys }
        }
        "MSET" => {
            let args: Vec<String> = iter.collect();
            if args.len() < 2 || args.len() % 2 != 0 {
                return Ok(Some(err_wrong_args("mset")));
            }
            let mut pairs = Vec::new();
            let mut it = args.into_iter();
            while let (Some(k), Some(v)) = (it.next(), it.next()) {
                pairs.push((k, v));
            }
            Command::Mset { pairs }
        }
        "SETNX" => {
            let Some(key) = iter.next() else {
                return Ok(Some(err_wrong_args("setnx")));
            };
            let Some(value) = iter.next() else {
                return Ok(Some(err_wrong_args("setnx")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("setnx")));
            }
            Command::Setnx { key, value }
        }
        "SETEX" => {
            let Some(key) = iter.next() else {
                return Ok(Some(err_wrong_args("setex")));
            };
            let Some(sec_s) = iter.next() else {
                return Ok(Some(err_wrong_args("setex")));
            };
            let Some(value) = iter.next() else {
                return Ok(Some(err_wrong_args("setex")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("setex")));
            }
            let Ok(seconds) = sec_s.parse::<i64>() else {
                return Ok(Some(err_not_integer()));
            };
            if seconds < 0 {
                return Ok(Some(err_not_integer()));
            }
            Command::Setex { key, seconds, value }
        }
        "PSETEX" => {
            let Some(key) = iter.next() else {
                return Ok(Some(err_wrong_args("psetex")));
            };
            let Some(ms_s) = iter.next() else {
                return Ok(Some(err_wrong_args("psetex")));
            };
            let Some(value) = iter.next() else {
                return Ok(Some(err_wrong_args("psetex")));
            };
            if iter.next().is_some() {
                return Ok(Some(err_wrong_args("psetex")));
            }
            let Ok(millis) = ms_s.parse::<i64>() else {
                return Ok(Some(err_not_integer()));
            };
            if millis < 0 {
                return Ok(Some(err_not_integer()));
            }
            Command::Psetex { key, millis, value }
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

