use std::env;
use std::sync::Mutex;

// 全局环境变量修改锁，所有测试共用
pub static ENV_LOCK: Mutex<()> = Mutex::new(());

pub struct EnvGuard {
    key: &'static str,
    prev: Option<String>,
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        match &self.prev {
            Some(v) => env::set_var(self.key, v),
            None => env::remove_var(self.key),
        }
    }
}

/// 设置环境变量，在 Drop 时自动恢复为之前的值。
pub fn set_env(key: &'static str, val: &str) -> EnvGuard {
    let prev = env::var(key).ok();
    env::set_var(key, val);
    EnvGuard { key, prev }
}

/// 移除环境变量，在 Drop 时自动恢复为之前的值（如果有）。
pub fn remove_env(key: &'static str) -> EnvGuard {
    let prev = env::var(key).ok();
    env::remove_var(key);
    EnvGuard { key, prev }
}

#[test]
fn env_guard_roundtrip_smoke() {
    let _lock = ENV_LOCK.lock().unwrap();

    // 设置后再移除，只是为了保证 API 在运行时不会 panic，顺便消除 dead_code 告警。
    let _g1 = set_env("REDUST_ENV_GUARD_SMOKE", "v1");
    let _g2 = remove_env("REDUST_ENV_GUARD_SMOKE");
}
