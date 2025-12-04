/// HyperLogLog 基数估计算法实现
///
/// 基于 Redis 的 HyperLogLog 实现，使用 16384 个寄存器（2^14），
/// 每个寄存器 6 位精度，标准误差约 0.81%。
///
/// 参考：
/// - Redis hyperloglog.c: https://github.com/redis/redis/blob/unstable/src/hyperloglog.c
/// - HyperLogLog 论文: http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// HyperLogLog 寄存器数量 (2^14 = 16384)
const HLL_REGISTERS: usize = 16384;

/// 寄存器索引位数 (14 bits)
const HLL_P: u32 = 14;

/// 每个寄存器的最大值 (6 bits = 63)
const HLL_REGISTER_MAX: u8 = 63;

/// HyperLogLog 数据结构
///
/// 使用密集表示：16384 个 6-bit 寄存器
/// 总内存占用：16384 * 6 / 8 = 12288 bytes ≈ 12KB
#[derive(Debug, Clone)]
pub struct HyperLogLog {
    /// 寄存器数组，每个元素存储一个 6-bit 值 (0-63)
    registers: Vec<u8>,
}

impl HyperLogLog {
    /// 创建一个新的空 HyperLogLog
    pub fn new() -> Self {
        HyperLogLog {
            registers: vec![0; HLL_REGISTERS],
        }
    }

    /// 添加一个元素到 HyperLogLog
    ///
    /// 返回 true 如果寄存器被更新（基数可能改变）
    /// 返回 false 如果寄存器未改变
    pub fn add(&mut self, element: &[u8]) -> bool {
        // 计算 64-bit 哈希值
        let hash = self.hash64(element);

        // 提取寄存器索引（前 14 bits）
        let index = (hash & ((1 << HLL_P) - 1)) as usize;

        // 提取剩余 50 bits 用于计算前导零
        let remaining = hash >> HLL_P;

        // 计算前导零数量 + 1（至少为 1）
        let leading_zeros = if remaining == 0 {
            51 // 50 bits 全为 0，加上隐含的 1 位
        } else {
            (remaining.leading_zeros() as u8) - (64 - 50) + 1
        };

        // 限制在 6 bits 范围内
        let count = leading_zeros.min(HLL_REGISTER_MAX);

        // 更新寄存器（取最大值）
        let old_value = self.registers[index];
        if count > old_value {
            self.registers[index] = count;
            true
        } else {
            false
        }
    }

    /// 估算基数
    ///
    /// 使用 HyperLogLog 算法计算不同元素的估计数量
    pub fn count(&self) -> u64 {
        // 计算调和平均数的倒数
        let mut sum = 0.0;
        let mut zeros = 0;

        for &register in &self.registers {
            if register == 0 {
                zeros += 1;
            }
            sum += 1.0 / (1u64 << register) as f64;
        }

        // 标准 HyperLogLog 估算公式
        let m = HLL_REGISTERS as f64;
        let alpha = 0.7213 / (1.0 + 1.079 / m);
        let raw_estimate = alpha * m * m / sum;

        // 小范围修正（使用线性计数）
        if raw_estimate <= 2.5 * m {
            if zeros > 0 {
                let linear_count = m * (m / zeros as f64).ln();
                if linear_count <= 2.5 * m {
                    return linear_count as u64;
                }
            }
        }

        // 大范围修正（2^32 附近）
        if raw_estimate <= (1u64 << 32) as f64 / 30.0 {
            raw_estimate as u64
        } else {
            let two_pow_32 = (1u64 << 32) as f64;
            let corrected = -two_pow_32 * (1.0 - raw_estimate / two_pow_32).ln();
            corrected as u64
        }
    }

    /// 合并另一个 HyperLogLog 到当前实例
    ///
    /// 对每个寄存器取最大值
    pub fn merge(&mut self, other: &HyperLogLog) {
        for i in 0..HLL_REGISTERS {
            if other.registers[i] > self.registers[i] {
                self.registers[i] = other.registers[i];
            }
        }
    }

    /// 计算 64-bit 哈希值
    ///
    /// 使用 Rust 标准库的 DefaultHasher（基于 SipHash）
    /// 注意：这与 Redis 的 MurmurHash2 不同，但对于基数估计来说足够好
    fn hash64(&self, data: &[u8]) -> u64 {
        let mut hasher = DefaultHasher::new();
        data.hash(&mut hasher);
        hasher.finish()
    }

    /// 获取寄存器数据（用于序列化）
    pub fn registers(&self) -> &[u8] {
        &self.registers
    }

    /// 从寄存器数据创建 HyperLogLog（用于反序列化）
    pub fn from_registers(registers: Vec<u8>) -> Option<Self> {
        if registers.len() != HLL_REGISTERS {
            return None;
        }
        // 验证所有寄存器值在有效范围内
        if registers.iter().any(|&r| r > HLL_REGISTER_MAX) {
            return None;
        }
        Some(HyperLogLog { registers })
    }

    /// 检查 HyperLogLog 是否为空（所有寄存器都为 0）
    pub fn is_empty(&self) -> bool {
        self.registers.iter().all(|&r| r == 0)
    }
}

impl Default for HyperLogLog {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_hll_is_empty() {
        let hll = HyperLogLog::new();
        assert_eq!(hll.count(), 0);
        assert!(hll.is_empty());
    }

    #[test]
    fn test_add_single_element() {
        let mut hll = HyperLogLog::new();
        let changed = hll.add(b"hello");
        assert!(changed);
        assert!(!hll.is_empty());
        
        // 添加相同元素不应改变
        let changed = hll.add(b"hello");
        assert!(!changed);
    }

    #[test]
    fn test_count_small_cardinality() {
        let mut hll = HyperLogLog::new();
        
        // 添加 100 个不同元素
        for i in 0..100 {
            hll.add(format!("element_{}", i).as_bytes());
        }
        
        let count = hll.count();
        // 允许约 0.81% 的误差，对于 100 个元素，误差应该在 ±5 以内
        assert!(count >= 95 && count <= 105, "count = {}", count);
    }

    #[test]
    fn test_count_medium_cardinality() {
        let mut hll = HyperLogLog::new();
        
        // 添加 1000 个不同元素
        for i in 0..1000 {
            hll.add(format!("element_{}", i).as_bytes());
        }
        
        let count = hll.count();
        // 允许约 2% 的误差
        assert!(count >= 980 && count <= 1020, "count = {}", count);
    }

    #[test]
    fn test_count_large_cardinality() {
        let mut hll = HyperLogLog::new();
        
        // 添加 10000 个不同元素
        for i in 0..10000 {
            hll.add(format!("element_{}", i).as_bytes());
        }
        
        let count = hll.count();
        // 允许约 2% 的误差
        assert!(count >= 9800 && count <= 10200, "count = {}", count);
    }

    #[test]
    fn test_merge() {
        let mut hll1 = HyperLogLog::new();
        let mut hll2 = HyperLogLog::new();
        
        // hll1 添加 0-99
        for i in 0..100 {
            hll1.add(format!("element_{}", i).as_bytes());
        }
        
        // hll2 添加 50-149（与 hll1 有 50 个重叠）
        for i in 50..150 {
            hll2.add(format!("element_{}", i).as_bytes());
        }
        
        // 合并后应该有约 150 个不同元素
        hll1.merge(&hll2);
        let count = hll1.count();
        assert!(count >= 140 && count <= 160, "count = {}", count);
    }

    #[test]
    fn test_merge_empty() {
        let mut hll1 = HyperLogLog::new();
        let hll2 = HyperLogLog::new();
        
        hll1.add(b"test");
        let count_before = hll1.count();
        
        hll1.merge(&hll2);
        let count_after = hll1.count();
        
        assert_eq!(count_before, count_after);
    }

    #[test]
    fn test_serialization() {
        let mut hll = HyperLogLog::new();
        for i in 0..100 {
            hll.add(format!("element_{}", i).as_bytes());
        }
        
        let registers = hll.registers().to_vec();
        let restored = HyperLogLog::from_registers(registers).unwrap();
        
        assert_eq!(hll.count(), restored.count());
    }

    #[test]
    fn test_from_registers_invalid_length() {
        let invalid = vec![0u8; 100];
        assert!(HyperLogLog::from_registers(invalid).is_none());
    }

    #[test]
    fn test_from_registers_invalid_value() {
        let mut invalid = vec![0u8; HLL_REGISTERS];
        invalid[0] = 64; // 超过 HLL_REGISTER_MAX (63)
        assert!(HyperLogLog::from_registers(invalid).is_none());
    }

    #[test]
    fn test_duplicate_elements() {
        let mut hll = HyperLogLog::new();
        
        // 添加 1000 次相同的 10 个元素
        for _ in 0..1000 {
            for i in 0..10 {
                hll.add(format!("element_{}", i).as_bytes());
            }
        }
        
        let count = hll.count();
        // 应该只计数 10 个不同元素
        assert!(count >= 8 && count <= 12, "count = {}", count);
    }

    #[test]
    fn test_binary_data() {
        let mut hll = HyperLogLog::new();
        
        // 测试二进制数据
        for i in 0..100u8 {
            hll.add(&[i, i.wrapping_mul(2), i.wrapping_mul(3)]);
        }
        
        let count = hll.count();
        assert!(count >= 95 && count <= 105, "count = {}", count);
    }
}
