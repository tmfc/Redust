/// HyperLogLog 基数估计算法实现
///
/// 基于 Redis 的 HyperLogLog 实现，使用 16384 个寄存器（2^14），
/// 每个寄存器 6 位精度，标准误差约 0.81%。
///
/// 优化：使用稀疏/密集双表示
/// - 稀疏表示：存储非零的 (index, value) 对，适合小基数
/// - 密集表示：完整的 16384 个寄存器，适合大基数
/// - 自动转换阈值：当稀疏表示超过 ~3000 字节时转为密集
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

/// 稀疏表示转换为密集表示的阈值（字节数）
/// 每个稀疏条目占用 3 字节 (u16 index + u8 value)
/// 当 entries.len() * 3 > 3000 时转为密集表示（约 1000 个非零寄存器）
const SPARSE_TO_DENSE_THRESHOLD: usize = 1000;

/// HyperLogLog 内部表示
#[derive(Debug, Clone)]
enum HllRepr {
    /// 稀疏表示：存储非零的 (index, value) 对，按 index 排序
    /// 内存占用：entries.len() * 3 bytes
    Sparse { entries: Vec<(u16, u8)> },
    
    /// 密集表示：完整的 16384 个寄存器
    /// 内存占用：16384 bytes
    Dense { registers: Vec<u8> },
}

/// HyperLogLog 数据结构
///
/// 使用稀疏/密集双表示优化内存占用：
/// - 小基数时使用稀疏表示，节省 98%+ 内存
/// - 大基数时自动转为密集表示
#[derive(Debug, Clone)]
pub struct HyperLogLog {
    repr: HllRepr,
}

impl HyperLogLog {
    /// 创建一个新的空 HyperLogLog（使用稀疏表示）
    pub fn new() -> Self {
        HyperLogLog {
            repr: HllRepr::Sparse { entries: Vec::new() },
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
        let index = (hash & ((1 << HLL_P) - 1)) as u16;

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

        match &mut self.repr {
            HllRepr::Sparse { entries } => {
                // 在稀疏表示中查找或插入
                match entries.binary_search_by_key(&index, |&(i, _)| i) {
                    Ok(pos) => {
                        // 找到了，更新如果新值更大
                        if count > entries[pos].1 {
                            entries[pos].1 = count;
                            true
                        } else {
                            false
                        }
                    }
                    Err(pos) => {
                        // 没找到，插入新条目
                        entries.insert(pos, (index, count));
                        
                        // 检查是否需要转换为密集表示
                        if entries.len() > SPARSE_TO_DENSE_THRESHOLD {
                            self.convert_to_dense();
                        }
                        true
                    }
                }
            }
            HllRepr::Dense { registers } => {
                // 密集表示：直接更新
                let idx = index as usize;
                let old_value = registers[idx];
                if count > old_value {
                    registers[idx] = count;
                    true
                } else {
                    false
                }
            }
        }
    }
    
    /// 将稀疏表示转换为密集表示
    fn convert_to_dense(&mut self) {
        if let HllRepr::Sparse { entries } = &self.repr {
            let mut registers = vec![0u8; HLL_REGISTERS];
            for &(index, value) in entries {
                registers[index as usize] = value;
            }
            self.repr = HllRepr::Dense { registers };
        }
    }

    /// 估算基数
    ///
    /// 使用 HyperLogLog 算法计算不同元素的估计数量
    pub fn count(&self) -> u64 {
        // 计算调和平均数的倒数
        let mut sum = 0.0;
        let mut zeros = 0;

        match &self.repr {
            HllRepr::Sparse { entries } => {
                // 稀疏表示：未出现的寄存器都是 0
                zeros = HLL_REGISTERS - entries.len();
                // 所有零寄存器的贡献
                sum = zeros as f64; // 1.0 / (1 << 0) = 1.0
                // 非零寄存器的贡献
                for &(_, value) in entries {
                    sum += 1.0 / (1u64 << value) as f64;
                }
            }
            HllRepr::Dense { registers } => {
                for &register in registers {
                    if register == 0 {
                        zeros += 1;
                    }
                    sum += 1.0 / (1u64 << register) as f64;
                }
            }
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
        // 如果任一方是密集表示，结果也用密集表示
        match (&mut self.repr, &other.repr) {
            (HllRepr::Dense { registers: self_regs }, HllRepr::Dense { registers: other_regs }) => {
                for i in 0..HLL_REGISTERS {
                    if other_regs[i] > self_regs[i] {
                        self_regs[i] = other_regs[i];
                    }
                }
            }
            (HllRepr::Dense { registers: self_regs }, HllRepr::Sparse { entries: other_entries }) => {
                for &(index, value) in other_entries {
                    let idx = index as usize;
                    if value > self_regs[idx] {
                        self_regs[idx] = value;
                    }
                }
            }
            (HllRepr::Sparse { .. }, HllRepr::Dense { registers: other_regs }) => {
                // 转换为密集表示
                self.convert_to_dense();
                if let HllRepr::Dense { registers: self_regs } = &mut self.repr {
                    for i in 0..HLL_REGISTERS {
                        if other_regs[i] > self_regs[i] {
                            self_regs[i] = other_regs[i];
                        }
                    }
                }
            }
            (HllRepr::Sparse { entries: self_entries }, HllRepr::Sparse { entries: other_entries }) => {
                // 合并两个稀疏表示
                let mut merged = Vec::with_capacity(self_entries.len() + other_entries.len());
                let mut i = 0;
                let mut j = 0;
                
                while i < self_entries.len() && j < other_entries.len() {
                    let (self_idx, self_val) = self_entries[i];
                    let (other_idx, other_val) = other_entries[j];
                    
                    match self_idx.cmp(&other_idx) {
                        std::cmp::Ordering::Less => {
                            merged.push((self_idx, self_val));
                            i += 1;
                        }
                        std::cmp::Ordering::Greater => {
                            merged.push((other_idx, other_val));
                            j += 1;
                        }
                        std::cmp::Ordering::Equal => {
                            merged.push((self_idx, self_val.max(other_val)));
                            i += 1;
                            j += 1;
                        }
                    }
                }
                
                // 添加剩余元素
                merged.extend_from_slice(&self_entries[i..]);
                merged.extend_from_slice(&other_entries[j..]);
                
                // 检查是否需要转换为密集表示
                if merged.len() > SPARSE_TO_DENSE_THRESHOLD {
                    let mut registers = vec![0u8; HLL_REGISTERS];
                    for (index, value) in merged {
                        registers[index as usize] = value;
                    }
                    self.repr = HllRepr::Dense { registers };
                } else {
                    self.repr = HllRepr::Sparse { entries: merged };
                }
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
    /// 
    /// 如果是稀疏表示，会先转换为密集表示
    pub fn registers(&self) -> Vec<u8> {
        match &self.repr {
            HllRepr::Dense { registers } => registers.clone(),
            HllRepr::Sparse { entries } => {
                let mut registers = vec![0u8; HLL_REGISTERS];
                for &(index, value) in entries {
                    registers[index as usize] = value;
                }
                registers
            }
        }
    }

    /// 从寄存器数据创建 HyperLogLog（用于反序列化）
    /// 
    /// 会自动选择稀疏或密集表示
    pub fn from_registers(registers: Vec<u8>) -> Option<Self> {
        if registers.len() != HLL_REGISTERS {
            return None;
        }
        // 验证所有寄存器值在有效范围内
        if registers.iter().any(|&r| r > HLL_REGISTER_MAX) {
            return None;
        }
        
        // 统计非零寄存器数量，决定使用哪种表示
        let non_zero_count = registers.iter().filter(|&&r| r > 0).count();
        
        if non_zero_count <= SPARSE_TO_DENSE_THRESHOLD {
            // 使用稀疏表示
            let entries: Vec<(u16, u8)> = registers
                .iter()
                .enumerate()
                .filter(|(_, &v)| v > 0)
                .map(|(i, &v)| (i as u16, v))
                .collect();
            Some(HyperLogLog {
                repr: HllRepr::Sparse { entries },
            })
        } else {
            // 使用密集表示
            Some(HyperLogLog {
                repr: HllRepr::Dense { registers },
            })
        }
    }

    /// 检查 HyperLogLog 是否为空（所有寄存器都为 0）
    pub fn is_empty(&self) -> bool {
        match &self.repr {
            HllRepr::Sparse { entries } => entries.is_empty(),
            HllRepr::Dense { registers } => registers.iter().all(|&r| r == 0),
        }
    }
    
    /// 获取当前内存占用（字节）
    pub fn memory_usage(&self) -> usize {
        match &self.repr {
            HllRepr::Sparse { entries } => {
                // Vec 本身的开销 + 每个条目 3 字节
                std::mem::size_of::<Vec<(u16, u8)>>() + entries.len() * 3
            }
            HllRepr::Dense { registers } => {
                // Vec 本身的开销 + 16384 字节
                std::mem::size_of::<Vec<u8>>() + registers.len()
            }
        }
    }
    
    /// 检查是否使用稀疏表示
    pub fn is_sparse(&self) -> bool {
        matches!(self.repr, HllRepr::Sparse { .. })
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
        
        let registers = hll.registers();
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
    
    #[test]
    fn test_sparse_representation() {
        let mut hll = HyperLogLog::new();
        
        // 新创建的 HLL 应该是稀疏表示
        assert!(hll.is_sparse());
        
        // 添加少量元素应该保持稀疏
        for i in 0..100 {
            hll.add(format!("element_{}", i).as_bytes());
        }
        assert!(hll.is_sparse());
        
        // 内存占用应该远小于密集表示
        let sparse_memory = hll.memory_usage();
        assert!(sparse_memory < 1000, "sparse memory = {}", sparse_memory);
    }
    
    #[test]
    fn test_sparse_to_dense_conversion() {
        let mut hll = HyperLogLog::new();
        
        // 添加大量元素应该触发转换为密集表示
        for i in 0..5000 {
            hll.add(format!("element_{}", i).as_bytes());
        }
        
        // 应该已经转换为密集表示
        assert!(!hll.is_sparse());
        
        // 内存占用应该接近 16KB
        let dense_memory = hll.memory_usage();
        assert!(dense_memory > 16000, "dense memory = {}", dense_memory);
    }
    
    #[test]
    fn test_merge_sparse_sparse() {
        let mut hll1 = HyperLogLog::new();
        let mut hll2 = HyperLogLog::new();
        
        for i in 0..50 {
            hll1.add(format!("a_{}", i).as_bytes());
        }
        for i in 0..50 {
            hll2.add(format!("b_{}", i).as_bytes());
        }
        
        assert!(hll1.is_sparse());
        assert!(hll2.is_sparse());
        
        hll1.merge(&hll2);
        
        // 合并后仍应是稀疏
        assert!(hll1.is_sparse());
        
        let count = hll1.count();
        assert!(count >= 95 && count <= 105, "count = {}", count);
    }
    
    #[test]
    fn test_memory_savings() {
        let mut hll = HyperLogLog::new();
        
        // 添加 100 个元素
        for i in 0..100 {
            hll.add(format!("element_{}", i).as_bytes());
        }
        
        let sparse_memory = hll.memory_usage();
        let dense_memory = HLL_REGISTERS; // 16384 bytes
        
        // 稀疏表示应该节省 90%+ 内存
        let savings = 1.0 - (sparse_memory as f64 / dense_memory as f64);
        assert!(savings > 0.9, "memory savings = {:.1}%", savings * 100.0);
    }
}
