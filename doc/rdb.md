# Redust RDB v1 文件格式说明

> 说明 Redust 自研的 RDB v1 快照格式，仅用于 Redust 内部持久化，不与 Redis 官方 RDB 兼容。

## 设计目标

- 单机、单库（仅 db0）快照，用于崩溃后恢复内存数据。
- 支持当前所有已实现的数据类型：string / list / set / hash。
- 支持 key 级 TTL：在快照中记录剩余过期时间，恢复时重新计算过期时间。
- 格式简单，顺序读写，易于演进（通过版本号区分未来格式）。

## 整体结构

RDB v1 文件是一个二进制文件，整体结构如下：

```text
+----------------------+-------------------+
| 文件头 (header)      | 记录流 (records) |
+----------------------+-------------------+
```

### 1. 文件头（header）

按顺序布局：

```text
magic:   [u8; 8]  = b"REDUSTDB"      // 魔数，用于快速识别文件类型
version: u32      = 1                // 版本号，小端序
```

- 若 magic 不为 `"REDUSTDB"`，Redust 会认为该文件不是合法 RDB 文件。
- 若 `version != 1`，当前实现会视为不兼容格式（后续可在实现中决定是报错还是尝试向后兼容）。

### 2. 记录流（records）

紧随 header 之后，是一段按顺序排列的记录流。每条记录描述一个 key 及其值和过期信息：

```text
while !EOF {
    record := read_record()
}
```

每条记录的结构：

```text
type:              u8
expires_millis:    i64
key_len:           u32
key:               [u8; key_len]
// value part depends on `type`
```

#### 2.1 type 字段

`type: u8` 表示该 key 的数据类型：

- `0` = string
- `1` = list
- `2` = set
- `3` = hash

当解析到未知的 `type` 时，当前实现会认为文件不兼容或损坏，视情况选择：

- 报错并停止加载；或
- 跳过后续内容并认为该文件不可用（实现中需要明确策略）。

#### 2.2 expires_millis 字段

`expires_millis: i64` 表示该 key 距离**过期时刻**的剩余毫秒数，含义如下：

- `< 0`：表示该 key **没有过期时间**（对应 `expires_at: None`）。
- `>= 0`：表示在保存快照时估算出的 **剩余过期毫秒数**。

加载时的处理策略：

- 若 `< 0`：恢复为 `expires_at = None`，即永不过期。
- 若 `>= 0`：
  - 若值非常小（例如为 0 或接近 0），可以认为该 key 已经过期，直接跳过该记录；
  - 否则，将该毫秒数转换为一个新的 `Instant` 截止时间（相对于当前 `Instant::now()`），恢复出一个近似的 TTL。

> 说明：
> - 由于 `Instant` 不能跨进程持久化，RDB 中存储的是「相对剩余时间」，而不是绝对时间戳。
> - 稍微的偏差在多数业务场景中可以接受；如果需要更精确的绝对时间，可在未来版本中调整为 `SystemTime` 表示。

#### 2.3 key 部分

- `key_len: u32` 表示 key 的字节长度（UTF-8 编码长度）。
- `key: [u8; key_len]` 为实际的 key 字节序列。

加载时：按字节读取并尝试以 UTF-8 解码构造 `String`；若解码失败，可选择：

- 直接跳过该 key；或
- 以「二进制不透明字符串」形式存储（当前实现倾向于只接受 UTF-8 以保持简单）。

### 3. 各类型 value 部分

value 的具体结构依赖于 `type` 字段。

#### 3.1 String 类型（type = 0）

```text
val_len: u32
val:     [u8; val_len]
```

- `val` 是 UTF-8 字符串的字节序列，对应 `StorageValue::String { value, expires_at }`。

#### 3.2 List 类型（type = 1）

```text
len:   u32                // list 中元素个数
repeat len 次 {
    item_len: u32
    item:     [u8; item_len]
}
```

- 记录插入顺序，对应内部 `VecDeque<String>` 中从左到右的顺序。

#### 3.3 Set 类型（type = 2）

```text
len:   u32                // set 中元素个数
repeat len 次 {
    member_len: u32
    member:     [u8; member_len]
}
```

- 对应 `HashSet<String>`。
- 加载时可以先全部读入并插入新的 `HashSet` 中。
- 集合内部无序，文件中的顺序不影响语义。

#### 3.4 Hash 类型（type = 3）

```text
len:   u32                // hash 中字段个数
repeat len 次 {
    field_len: u32
    field:     [u8; field_len]

    value_len: u32
    value:     [u8; value_len]
}
```

- 对应 `HashMap<String, String>`。

## 语义与行为约定

### 保存（save）语义

- 遍历当前 `Storage` 中的所有 key：
  - 若 key 已过期（根据当前时间和 `expires_at` 判断），则不会被写入 RDB。
  - 否则根据 `StorageValue` 的变体写出一条记录。
- 对于存在 TTL 的 key：
  - 计算 `expires_millis = 剩余过期时间（毫秒）`；
  - 若计算结果为负或非常接近 0，可选择：
    - 直接跳过该 key；或
    - 按 `expires_millis = 0` 写出，加载时自然会被视为过期并丢弃。

### 加载（load）语义

- 启动时（或手动触发加载时）：
  - 打开指定 RDB 文件；
  - 校验 magic 和 version；
  - 按顺序读取记录，直至 EOF：
    - 若记录结构不完整或字段非法，认为文件损坏，可以：
      - 记录错误日志；
      - 中止加载并选择是否以空库启动（推荐）。
- 加载成功后：
  - `Storage` 中的所有现有数据会被清空，然后填充为 RDB 中的内容（v1 可以使用「清空再写入」策略）。

### 崩溃恢复与容错

- 当 RDB 文件不存在时：
  - 服务器以空库启动，不视为错误。
- 当 RDB 文件损坏或版本不兼容时：
  - 打印错误日志，**优先保证服务器仍然可以以空库启动**，避免完全不可用。

## 演进与兼容性

- 当前版本号固定为 `1`，未来如需调整格式（例如：
  - 支持多 DB；
  - 调整 TTL 表示方式（使用绝对时间戳）；
  - 增加新的数据类型；
  ）
  可以：
  - 提升 `version`，在加载时根据版本号走不同解析路径；
  - 在文档中增补各版本差异与迁移策略。

## 与 Redis RDB 的关系

- Redust RDB v1 **不与 Redis 官方 RDB 二进制格式兼容**：
  - 不保证可以直接导入 Redis，也不保证可以直接读取 Redis 的 RDB。
- 设计上参考了 Redis 的快照思路：
  - 以「键空间快照」形式存储数据；
  - 在单个文件中记录 key、type、value、TTL 等信息。

在实现层面，可以在 `Storage` 上提供类似：

- `Storage::save_rdb(path: &Path) -> io::Result<()>`
- `Storage::load_rdb(path: &Path) -> io::Result<()>`

并在文档或 README 中说明：

- RDB 文件默认路径（例如 `redust.rdb`）；
- 何时触发保存与加载；
- 加载失败时的行为（例如回退为空库）。
