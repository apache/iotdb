# IoTDB 数据分区表完整性检测功能实现

## 功能概述

本功能实现了IoTDB ConfigNode重启时的数据分区表完整性检测，能够自动发现并恢复丢失的数据分区信息。

## 实现架构

### 1. 核心组件

#### Procedure实现
- **DataPartitionTableIntegrityCheckProcedure**: 主要的Procedure实现，负责整个完整性检测流程
- **ConfigNodeProcedureEnv**: Procedure执行环境，提供ConfigManager访问

#### DataNode端实现
- **DataPartitionTableGenerator**: 扫描tsfile并生成DataPartitionTable的核心组件
- **RPC接口扩展**: 在DataNode RPC服务中添加了三个新接口

#### 配置和注册
- **ProcedureType枚举扩展**: 添加了新的Procedure类型
- **ProcedureFactory扩展**: 支持新Procedure的创建和反序列化
- **启动监听器**: ConfigNode启动时自动触发检测

### 2. 执行流程

```
ConfigNode重启 → 检查Leader状态 → 收集最早timeslot → 分析缺失分区 → 
请求DN生成表 → 合并分区表 → 写入Raft日志 → 完成
```

## 详细实现

### 1. Thrift接口定义 (datanode.thrift)

新增的RPC接口：
```thrift
// 获取最早timeslot信息
TGetEarliestTimeslotsResp getEarliestTimeslots()

// 请求生成DataPartitionTable
TGenerateDataPartitionTableResp generateDataPartitionTable()

// 检查生成状态
TCheckDataPartitionTableStatusResp checkDataPartitionTableStatus()
```

对应的响应结构体：
```thrift
struct TGetEarliestTimeslotsResp {
  1: required common.TSStatus status
  2: optional map<string, i64> databaseToEarliestTimeslot
}

struct TGenerateDataPartitionTableResp {
  1: required common.TSStatus status
  2: required i32 errorCode
  3: optional string message
}

struct TCheckDataPartitionTableStatusResp {
  1: required common.TSStatus status
  2: required i32 errorCode
  3: optional string message
  4: optional binary dataPartitionTable
}
```

### 2. DataNode实现

#### DataPartitionTableGenerator
- **并行扫描**: 使用多线程并行扫描tsfile文件
- **进度跟踪**: 提供处理进度和状态信息
- **错误处理**: 统计失败文件并记录错误信息
- **配置化**: 支持自定义线程数和分区配置

#### RPC服务实现
在`DataNodeInternalRPCServiceImpl`中实现：
- `getEarliestTimeslots()`: 扫描数据目录获取每个数据库的最早timeslot
- `generateDataPartitionTable()`: 启动异步扫描任务
- `checkDataPartitionTableStatus()`: 检查任务状态并返回结果

### 3. ConfigNode Procedure实现

#### 状态机设计
```java
public enum State {
    CHECK_LEADER_STATUS,        // 检查Leader状态
    COLLECT_EARLIEST_TIMESLOTS, // 收集最早timeslot
    ANALYZE_MISSING_PARTITIONS,  // 分析缺失分区
    REQUEST_PARTITION_TABLES,    // 请求生成分区表
    MERGE_PARTITION_TABLES,      // 合并分区表
    WRITE_PARTITION_TABLE_TO_RAFT, // 写入Raft日志
    SUCCESS,                    // 成功完成
    FAILED                       // 执行失败
}
```

#### 错误码定义
```java
public static final int DN_ERROR_CODE_SUCCESS = 0;      // 处理成功
public static final int DN_ERROR_CODE_IN_PROGRESS = 2;  // 正在执行
public static final int DN_ERROR_CODE_FAILED = 1;       // 处理失败
public static final int DN_ERROR_CODE_UNKNOWN = -1;    // DN未知状态
```

#### 核心逻辑
1. **Leader检查**: 只有Leader节点执行检测
2. **数据收集**: 从所有DataNode收集最早timeslot信息
3. **缺失分析**: 对比当前分区表，识别缺失的分区
4. **异步处理**: 向DataNode发送异步扫描请求
5. **状态轮询**: 定期检查任务状态，支持重试机制
6. **数据合并**: 合并所有DataNode返回的分区表
7. **Raft写入**: 通过共识层持久化最终分区表

### 4. 自动触发机制

#### 启动监听器
```java
public class DataPartitionTableIntegrityCheckListener {
    public void onStartupComplete() {
        if (isLeader()) {
            startIntegrityCheck();
        }
    }
    
    public void onBecomeLeader() {
        startIntegrityCheck();
    }
}
```

## 关键特性

### 1. 原子性保证
- 每个步骤都是幂等的，支持重试
- Procedure框架保证状态一致性
- 失败时可以安全回滚

### 2. 容错机制
- **重试策略**: 最多重试3次
- **超时处理**: 避免无限等待
- **部分失败**: 部分DataNode失败时继续处理

### 3. 性能优化
- **并行扫描**: DataNode端使用多线程并行处理
- **异步执行**: 避免阻塞主流程
- **进度跟踪**: 提供实时进度信息

### 4. 可扩展性
- **配置化**: 支持自定义线程数和分区配置
- **模块化**: 各组件独立，易于扩展
- **接口化**: 清晰的RPC接口定义

## 使用方式

### 1. 自动触发
ConfigNode重启时自动检测并执行，无需手动干预。

### 2. 手动触发
可以通过ProcedureExecutor手动提交检测Procedure：
```java
DataPartitionTableIntegrityCheckProcedure procedure = new DataPartitionTableIntegrityCheckProcedure();
procedureExecutor.submit(procedure);
```

## 配置参数

### DataNode配置
- `seriesSlotNum`: 系列分区槽数量
- `seriesPartitionExecutorClass`: 分区执行器类名
- `dataDirs`: 数据目录配置

### Procedure配置
- `MAX_RETRY_COUNT`: 最大重试次数 (默认3)
- 重试间隔: 5秒

## 监控和日志

### 日志级别
- **INFO**: 关键流程节点信息
- **DEBUG**: 详细的执行过程
- **ERROR**: 错误和异常信息

### 关键指标
- 处理文件数量
- 失败文件数量
- 执行时间
- 重试次数
- DataNode响应状态

## 注意事项

### 1. 依赖关系
- 需要ConfigNode为Leader状态
- 依赖DataNode正常注册和通信
- 需要共识层正常工作

### 2. 资源消耗
- DataNode扫描会消耗CPU和I/O资源
- 建议在低峰期执行
- 大数据集时需要考虑内存使用

### 3. 网络带宽
- DataPartitionTable序列化后可能较大
- 需要考虑网络传输限制
- 建议实现增量传输机制

## 后续优化建议

### 1. 增量扫描
- 支持增量扫描，只处理新增文件
- 维护扫描状态，避免重复工作

### 2. 分布式协调
- 实现更智能的负载分配
- 支持动态调整扫描策略

### 3. 缓存优化
- 缓存扫描结果，避免重复计算
- 实现智能失效机制

### 4. 监控增强
- 添加更详细的性能指标
- 实现告警机制

## 测试验证

### 1. 单元测试
- 各组件独立测试
- 边界条件测试
- 异常场景验证

### 2. 集成测试
- 端到端流程测试
- 多节点环境验证
- 故障恢复测试

### 3. 性能测试
- 大数据集扫描测试
- 并发性能测试
- 资源使用监控

---

本实现提供了完整的IoTDB数据分区表完整性检测解决方案，具备高可用性、容错性和可扩展性，能够在ConfigNode重启时自动发现并恢复丢失的数据分区信息。
