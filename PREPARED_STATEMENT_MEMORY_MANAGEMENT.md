# PreparedStatement 内存限制设计文档

## 概述

为 PreparedStatement 功能添加内存限制，所有用户的所有会话共享从 `CoordinatorMemoryManager` 分配的内存池。当内存被用满时，在有用户释放 PreparedStatement 之前，其他用户将无法使用 PreparedStatement 功能。

## 内存配置

内存限制使用 `chunk_timeseriesmeta_free_memory_proportion` 配置项中的第四项（`Coordinator`），默认比例为 `50`。

配置格式：`1:100:200:50:200:200:200:50`
- 第1项：BloomFilterCache
- 第2项：ChunkCache
- 第3项：TimeSeriesMetadataCache
- **第4项：Coordinator（PreparedStatement 使用此项）**
- 第5项：Operators
- 第6项：DataExchange
- 第7项：timeIndex in TsFileResourceList
- 第8项：others

## IoTDB 原有内存管理组件

### IMemoryBlock 接口

**文件位置**：`iotdb-core/node-commons/src/main/java/org/apache/iotdb/commons/memory/IMemoryBlock.java`

`IMemoryBlock` 是 IoTDB 内存管理系统的核心抽象类，实现了 `AutoCloseable` 接口，用于表示一个内存块。

**主要特性**：
- **内存块抽象**：表示由 `MemoryManager` 管理的一块内存
- **生命周期管理**：实现了 `AutoCloseable`，支持自动资源释放
- **内存操作**：提供内存分配、释放、查询等方法
  - `allocate(long sizeInByte)`: 分配内存
  - `release(long sizeInByte)`: 释放内存
  - `getUsedMemoryInBytes()`: 获取已使用内存大小
  - `getFreeMemoryInBytes()`: 获取空闲内存大小
- **状态跟踪**：维护内存块的状态（是否已释放、总大小等）

**实现类**：
- `AtomicLongMemoryBlock`: 使用 `AtomicLong` 跟踪内存使用的实现
- 其他自定义实现类

### CoordinatorMemoryManager

**文件位置**：`iotdb-core/datanode/src/main/java/org/apache/iotdb/db/conf/DataNodeMemoryConfig.java`

`CoordinatorMemoryManager` 是 IoTDB 查询引擎内存管理系统中的一个 `MemoryManager` 实例，专门用于管理 Coordinator 相关的内存。

**初始化**：
- 在 `DataNodeMemoryConfig.initQueryEngineMemoryAllocate()` 方法中创建
- 从 `queryEngineMemoryManager` 中通过 `getOrCreateMemoryManager("Coordinator", coordinatorMemorySize)` 创建
- 内存大小由 `chunk_timeseriesmeta_free_memory_proportion` 配置的第四项决定

**用途**：
- 管理 Coordinator 组件的内存使用
- 支持通过 `exactAllocate()` 方法分配 `IMemoryBlock`
- 跟踪和限制 Coordinator 相关的内存使用

**获取方式**：
```java
MemoryManager coordinatorMemoryManager = 
    IoTDBDescriptor.getInstance().getMemoryConfig().getCoordinatorMemoryManager();
```

**内存分配示例**：
```java
IMemoryBlock memoryBlock = coordinatorMemoryManager.exactAllocate(
    "PreparedStatement-stmt1", 
    memorySizeInBytes, 
    MemoryBlockType.DYNAMIC
);
```

**MemoryBlockType**：
- `STATIC`: 静态内存块，生命周期较长，通常用于缓存等场景
- `DYNAMIC`: 动态内存块，生命周期较短，用于临时内存分配（PreparedStatement 使用此类型）
- `NONE`: 无类型，用于测试或特殊场景

**与 PreparedStatement 的关系**：
- PreparedStatement 的内存管理复用 IoTDB 现有的 `CoordinatorMemoryManager` 和 `IMemoryBlock` 机制
- 不需要创建新的内存管理器，而是利用现有的内存管理基础设施
- 通过 `exactAllocate()` 方法分配 `DYNAMIC` 类型的内存块，表示 PreparedStatement 的内存是动态分配的，可以随时释放

## 设计架构

### 1. 内存管理器：`PreparedStatementMemoryManager`

**文件位置**：`iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/plan/execution/config/session/PreparedStatementMemoryManager.java`

**职责**：
- 单例模式管理 PreparedStatement 的内存分配和释放
- 从 `CoordinatorMemoryManager` 分配内存
- 提供 `allocate()` 和 `release()` 方法

**关键方法**：
- `allocate(String statementName, long memorySizeInBytes)`: 分配内存，如果内存不足抛出 `SemanticException`
- `release(IMemoryBlock memoryBlock)`: 释放内存

### 2. AST 内存估算器：`AstMemoryEstimator`

**文件位置**：`iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/plan/relational/sql/AstMemoryEstimator.java`

**职责**：
- 估算 Statement AST 的内存大小
- 使用 `RamUsageEstimator` 计算每个节点的浅层大小
- 遍历整个 AST 树累加内存大小

**关键方法**：
- `estimateMemorySize(Statement statement)`: 估算 AST 的内存大小（字节）

### 3. PreparedStatementInfo 扩展

**文件位置**：`iotdb-core/datanode/src/main/java/org/apache/iotdb/db/protocol/session/PreparedStatementInfo.java`

**修改内容**：
- 添加 `IMemoryBlock memoryBlock` 字段，用于存储分配的内存块引用
- 修改构造函数，接受 `IMemoryBlock` 参数
- 添加 `getMemoryBlock()` 方法

### 4. PrepareTask 内存分配

**文件位置**：`iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/plan/execution/config/session/PrepareTask.java`

**修改内容**：
- 在创建 `PreparedStatementInfo` 之前，估算 AST 的内存大小
- 调用 `PreparedStatementMemoryManager.allocate()` 分配内存
- 如果内存分配失败，抛出 `SemanticException`
- 如果其他操作失败，释放已分配的内存（错误恢复）

### 5. DeallocateTask 内存释放

**文件位置**：`iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/plan/execution/config/session/DeallocateTask.java`

**修改内容**：
- 在删除 `PreparedStatementInfo` 后，调用 `PreparedStatementMemoryManager.release()` 释放内存
- 从 `PreparedStatementInfo` 中获取 `IMemoryBlock` 引用并释放

## 执行流程

### PREPARE 语句执行流程

1. 用户执行 `PREPARE statementName FROM sql`
2. `PrepareTask.execute()` 被调用
3. 检查 PreparedStatement 是否已存在
4. 使用 `AstMemoryEstimator.estimateMemorySize()` 估算 AST 内存大小
5. 调用 `PreparedStatementMemoryManager.allocate()` 分配内存
   - 如果内存不足，抛出 `SemanticException`，用户收到错误信息
   - 如果成功，获得 `IMemoryBlock` 引用
6. 创建 `PreparedStatementInfo`（包含 AST 和 `IMemoryBlock`）
7. 存储到 `IClientSession` 中
8. 如果后续步骤失败，释放已分配的内存

### DEALLOCATE 语句执行流程

1. 用户执行 `DEALLOCATE PREPARE statementName`
2. `DeallocateTask.execute()` 被调用
3. 从 `IClientSession` 中移除 `PreparedStatementInfo`
4. 从 `PreparedStatementInfo` 获取 `IMemoryBlock` 引用
5. 调用 `PreparedStatementMemoryManager.release()` 释放内存
6. 内存返回到 `CoordinatorMemoryManager` 池中，可供其他 PreparedStatement 使用

## 内存共享机制

- **所有会话共享**：所有用户的 PreparedStatement 共享同一块内存池（`CoordinatorMemoryManager`）
- **内存不足处理**：当内存被用满时，新的 `PREPARE` 语句会失败，直到有用户执行 `DEALLOCATE` 释放内存
- **自动内存管理**：内存分配和释放由 `MemoryManager` 自动跟踪，确保不会超过配置的限制

## 错误处理

### 内存不足错误

当内存不足时，`PreparedStatementMemoryManager.allocate()` 会抛出 `SemanticException`，错误信息为：
```
Insufficient memory for PreparedStatement '<statementName>'. 
Please deallocate some PreparedStatements and try again.
```

### 错误恢复

在 `PrepareTask` 中，如果内存分配成功但后续操作失败，会自动释放已分配的内存，避免内存泄漏。

## 修改的文件列表

1. **新建文件**：
   - `PreparedStatementMemoryManager.java` - 内存管理器
   - `AstMemoryEstimator.java` - AST 内存估算器

2. **修改文件**：
   - `PreparedStatementInfo.java` - 添加 `IMemoryBlock` 字段
   - `PrepareTask.java` - 添加内存分配逻辑
   - `DeallocateTask.java` - 添加内存释放逻辑

## 注意事项

1. **内存估算精度**：`RamUsageEstimator` 提供的是近似估算，实际内存使用可能略有不同
2. **内存碎片**：频繁的分配和释放可能导致内存碎片，但 `MemoryManager` 会处理这些问题
3. **并发安全**：`MemoryManager` 是线程安全的，支持多线程并发访问
4. **内存限制**：内存限制基于 `chunk_timeseriesmeta_free_memory_proportion` 配置，修改配置需要重启 IoTDB

## 测试建议

1. **内存不足测试**：创建大量 PreparedStatement 直到内存用满，验证新的 PREPARE 语句失败
2. **内存释放测试**：DEALLOCATE 后验证内存被释放，新的 PREPARE 可以成功
3. **并发测试**：多个会话同时创建和释放 PreparedStatement，验证线程安全
4. **错误恢复测试**：在 PREPARE 过程中模拟错误，验证内存被正确释放

