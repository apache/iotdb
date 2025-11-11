# Trino vs IoTDB Prepared Statement 实现对比与优化建议

## 1. Trino 实现概览

### 1.1 核心架构

Trino 的 Prepared Statement 实现采用以下架构：

```
客户端请求
    ↓
SessionContext (解析 HTTP Header 中的 prepared statements)
    ↓
Session (存储 prepared statements: Map<String, String>)
    ↓
QueryPreparer (处理 EXECUTE 语句，替换参数)
    ↓
QueryStateMachine (跟踪 prepared statement 的添加/删除)
    ↓
PrepareTask / DeallocateTask (执行 PREPARE/DEALLOCATE)
```

### 1.2 关键组件

#### 1.2.1 Session 类
- **存储方式**：`Map<String, String> preparedStatements`
- **特点**：存储的是**格式化后的 SQL 字符串**，而不是 AST
- **位置**：`io.trino.Session`

```java
private final Map<String, String> preparedStatements;

public String getPreparedStatement(String name) {
    String sql = preparedStatements.get(name);
    checkCondition(sql != null, NOT_FOUND, "Prepared statement not found: %s", name);
    return sql;
}
```

#### 1.2.2 PrepareTask
- **功能**：将 Statement AST 转换为格式化 SQL，存储到 QueryStateMachine
- **关键代码**：

```java
@Override
public ListenableFuture<Void> execute(
        Prepare prepare,
        QueryStateMachine stateMachine,
        List<Expression> parameters,
        WarningCollector warningCollector) {
    Statement statement = prepare.getStatement();
    // 验证不允许嵌套 PREPARE/EXECUTE/DEALLOCATE
    if ((statement instanceof Prepare) || (statement instanceof Execute) || (statement instanceof Deallocate)) {
        throw new TrinoException(NOT_SUPPORTED, "Invalid statement type for prepared statement");
    }
    
    // 将 AST 转换为格式化 SQL 字符串
    String sql = getFormattedSql(statement, sqlParser);
    stateMachine.addPreparedStatement(prepare.getName().getValue(), sql);
    return immediateVoidFuture();
}
```

**关键点**：
- 使用 `SqlFormatterUtil.getFormattedSql()` 将 AST 转换为 SQL 字符串
- 存储的是**字符串**，不是 AST
- 通过 `QueryStateMachine` 管理，而不是直接存储在 Session

#### 1.2.3 QueryStateMachine
- **作用**：跟踪查询执行过程中的状态变化
- **存储**：
  - `addedPreparedStatements: Map<String, String>` - 本次查询添加的 prepared statements
  - `deallocatedPreparedStatements: Set<String>` - 本次查询删除的 prepared statements
- **特点**：**增量更新**，只记录本次查询的变化

```java
private final Map<String, String> addedPreparedStatements = new ConcurrentHashMap<>();
private final Set<String> deallocatedPreparedStatements = Sets.newConcurrentHashSet();

public void addPreparedStatement(String key, String value) {
    addedPreparedStatements.put(key, value);
}

public void removePreparedStatement(String key) {
    if (!session.getPreparedStatements().containsKey(key)) {
        throw new TrinoException(NOT_FOUND, "Prepared statement not found: " + key);
    }
    deallocatedPreparedStatements.add(key);
}
```

#### 1.2.4 QueryPreparer
- **作用**：处理 EXECUTE 和 EXECUTE IMMEDIATE 语句
- **关键逻辑**：

```java
public PreparedQuery prepareQuery(Session session, Statement wrappedStatement) {
    Statement statement = wrappedStatement;
    Optional<String> prepareSql = Optional.empty();
    
    if (statement instanceof Execute executeStatement) {
        // 从 Session 获取 prepared statement SQL
        prepareSql = Optional.of(session.getPreparedStatementFromExecute(executeStatement));
        // 重新解析为 Statement
        statement = sqlParser.createStatement(prepareSql.get());
    }
    else if (statement instanceof ExecuteImmediate executeImmediateStatement) {
        // 直接解析 SQL 字符串
        statement = sqlParser.createStatement(
                executeImmediateStatement.getStatement().getValue(),
                executeImmediateStatement.getStatement().getLocation().orElseThrow());
    }
    
    // 提取参数
    List<Expression> parameters = ImmutableList.of();
    if (wrappedStatement instanceof Execute executeStatement) {
        parameters = executeStatement.getParameters();
    }
    else if (wrappedStatement instanceof ExecuteImmediate executeImmediateStatement) {
        parameters = executeImmediateStatement.getParameters();
    }
    
    // 验证参数数量和类型
    validateParameters(statement, parameters);
    
    return new PreparedQuery(statement, parameters, prepareSql);
}
```

**关键点**：
- EXECUTE：从 Session 获取 SQL → 重新解析 → 提取参数
- EXECUTE IMMEDIATE：直接解析 SQL → 提取参数
- **参数替换发生在后续的 Analyzer 阶段**，而不是在 QueryPreparer 中

#### 1.2.5 PreparedStatementEncoder
- **作用**：压缩大型 prepared statements（用于 HTTP Header）
- **特点**：使用 Zstd 压缩，Base64 编码
- **阈值**：可配置的压缩阈值和最小增益

```java
public String encodePreparedStatementForHeader(String preparedStatement) {
    if (preparedStatement.length() < compressionThreshold) {
        return preparedStatement;
    }
    // Zstd 压缩 + Base64 编码
    // ...
}
```

### 1.3 执行流程

#### PREPARE 流程：
```
PREPARE stmt1 FROM SELECT * FROM table WHERE id = ?
    ↓
PrepareTask.execute()
    ├─ 验证 Statement 类型（不允许嵌套）
    ├─ SqlFormatterUtil.getFormattedSql() → "SELECT * FROM table WHERE id = ?"
    └─ QueryStateMachine.addPreparedStatement("stmt1", sql)
        └─ 最终更新到 Session.preparedStatements
```

#### EXECUTE 流程：
```
EXECUTE stmt1 USING 100
    ↓
QueryPreparer.prepareQuery()
    ├─ 检测到 Execute 语句
    ├─ Session.getPreparedStatement("stmt1") → "SELECT * FROM table WHERE id = ?"
    ├─ SqlParser.createStatement(sql) → Statement AST
    ├─ 提取参数: [100]
    └─ 返回 PreparedQuery(statement, parameters, prepareSql)
    ↓
Analyzer.analyze()
    └─ 参数替换发生在语义分析阶段
```

## 2. IoTDB 实现概览

### 2.1 核心架构

```
客户端请求
    ↓
IClientSession (存储 prepared statements)
    ↓
PrepareTask / DeallocateTask (执行 PREPARE/DEALLOCATE)
    ↓
Coordinator (路由 EXECUTE/EXECUTE IMMEDIATE)
    ↓
参数替换 + 重新解析 (待实现)
```

### 2.2 关键组件

#### 2.2.1 PreparedStatementInfo
- **存储方式**：存储 **AST (`Statement`)**，而不是 SQL 字符串
- **字段**：
  - `statementName: String`
  - `sql: Statement` (AST)
  - `zoneId: ZoneId`
  - `createTime: long`

```java
public class PreparedStatementInfo {
    private final String statementName;
    private final Statement sql;  // AST，不是字符串
    private final ZoneId zoneId;
    private final long createTime;
}
```

#### 2.2.2 PrepareTask
- **功能**：存储 PreparedStatementInfo 到 IClientSession
- **关键代码**：

```java
@Override
public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor) {
    IClientSession session = SessionManager.getInstance().getCurrSession();
    PreparedStatementInfo info = new PreparedStatementInfo(statementName, sql, zoneId);
    session.addPreparedStatement(statementName, info);
    return future;
}
```

**关键点**：
- 直接存储 AST，不转换为 SQL 字符串
- 通过 `SessionManager.getCurrSession()` 获取会话

#### 2.2.3 IClientSession
- **存储**：`Map<String, PreparedStatementInfo> preparedStatements`
- **特点**：每个会话独立管理 prepared statements

## 3. 对比分析

### 3.1 存储格式对比

| 特性 | Trino | IoTDB |
|------|-------|-------|
| **存储格式** | SQL 字符串 (`Map<String, String>`) | AST (`Map<String, PreparedStatementInfo>`) |
| **优点** | 1. 序列化简单<br>2. 易于调试<br>3. 支持压缩传输 | 1. 避免重复解析<br>2. 保留完整语义信息<br>3. 类型安全 |
| **缺点** | 1. 需要重新解析<br>2. 可能丢失格式信息 | 1. 序列化复杂<br>2. 内存占用较大 |

### 3.2 执行流程对比

| 阶段 | Trino | IoTDB |
|------|-------|-------|
| **PREPARE** | AST → SQL 字符串 → Session | AST → PreparedStatementInfo → Session |
| **EXECUTE** | Session → SQL → 重新解析 → 参数替换 | Session → AST → 参数替换 → (待实现) |
| **参数替换时机** | Analyzer 阶段 | Coordinator 阶段（计划） |

### 3.3 状态管理对比

| 特性 | Trino | IoTDB |
|------|-------|-------|
| **状态跟踪** | QueryStateMachine 跟踪增量变化 | 直接修改 Session |
| **事务性** | 支持事务回滚（通过 QueryStateMachine） | 直接修改，无事务支持 |
| **并发安全** | ConcurrentHashMap | ConcurrentHashMap |

### 3.4 错误处理对比

| 特性 | Trino | IoTDB |
|------|-------|-------|
| **PREPARE 验证** | 检查嵌套 PREPARE/EXECUTE/DEALLOCATE | 检查重复定义（可选） |
| **EXECUTE 验证** | 检查 prepared statement 是否存在 | 待实现 |
| **参数验证** | 验证参数数量和类型 | 待实现 |

## 4. 优化建议

### 4.1 存储格式优化

#### 建议 1：采用混合存储策略

**当前问题**：
- IoTDB 存储 AST，内存占用大，序列化复杂
- Trino 存储字符串，需要重新解析

**优化方案**：
```java
public class PreparedStatementInfo {
    private final String statementName;
    private final Statement sql;           // AST（用于执行）
    private final String formattedSql;    // SQL 字符串（用于序列化/调试）
    private final ZoneId zoneId;
    private final long createTime;
    
    // 可选：缓存解析结果
    private volatile Statement cachedStatement;
}
```

**优点**：
- 执行时直接使用 AST，避免重复解析
- 序列化时使用 SQL 字符串，简化传输
- 支持调试和日志记录

#### 建议 2：添加 SQL 格式化工具

参考 Trino 的 `SqlFormatterUtil.getFormattedSql()`，实现类似的工具：

```java
public class SqlFormatter {
    public static String formatStatement(Statement statement, SqlParser sqlParser) {
        // 将 AST 转换为标准化的 SQL 字符串
        // 用于存储、序列化、调试
    }
}
```

### 4.2 执行流程优化

#### 建议 3：实现 QueryPreparer 模式

参考 Trino 的 `QueryPreparer`，在 IoTDB 中实现类似组件：

```java
public class PreparedStatementPreparer {
    private final SqlParser sqlParser;
    
    public PreparedStatement prepareStatement(
            IClientSession session,
            Execute execute) {
        // 1. 从 Session 获取 PreparedStatementInfo
        PreparedStatementInfo info = session.getPreparedStatement(
            execute.getStatementName().getValue());
        
        // 2. 获取 AST 或 SQL
        Statement statement = info.getSql();
        
        // 3. 提取参数
        List<Literal> parameters = execute.getParameters();
        
        // 4. 验证参数
        validateParameters(statement, parameters);
        
        // 5. 返回 PreparedStatement（包含 Statement 和参数）
        return new PreparedStatement(statement, parameters);
    }
}
```

**优点**：
- 统一处理 EXECUTE 和 EXECUTE IMMEDIATE
- 集中参数验证逻辑
- 便于后续扩展

#### 建议 4：参数替换优化

**当前计划**：在 Coordinator 中进行字符串替换

**优化方案**：在 AST 层面进行参数替换

```java
public class ParameterReplacer extends AstVisitor<Statement, List<Literal>> {
    @Override
    protected Statement visitParameter(Parameter node, List<Literal> parameters) {
        int index = node.getIndex();
        if (index < 0 || index >= parameters.size()) {
            throw new SemanticException("Parameter index out of range");
        }
        return parameters.get(index);
    }
    
    // 遍历 AST，替换所有 Parameter 节点
}
```

**优点**：
- 类型安全
- 避免 SQL 注入风险
- 保留 AST 结构，便于优化

### 4.3 状态管理优化

#### 建议 5：添加 QueryStateMachine 模式

参考 Trino，添加查询级别的状态跟踪：

```java
public class QueryStateMachine {
    private final IClientSession session;
    private final Map<String, PreparedStatementInfo> addedPreparedStatements = new ConcurrentHashMap<>();
    private final Set<String> deallocatedPreparedStatements = ConcurrentHashMap.newKeySet();
    
    public void addPreparedStatement(String name, PreparedStatementInfo info) {
        addedPreparedStatements.put(name, info);
        // 延迟提交到 Session，支持事务回滚
    }
    
    public void commit() {
        // 提交所有变更到 Session
        addedPreparedStatements.forEach(session::addPreparedStatement);
        deallocatedPreparedStatements.forEach(session::removePreparedStatement);
    }
    
    public void rollback() {
        // 回滚所有变更
        addedPreparedStatements.clear();
        deallocatedPreparedStatements.clear();
    }
}
```

**优点**：
- 支持事务性操作
- 便于查询级别的状态跟踪
- 支持查询失败时的回滚

### 4.4 错误处理优化

#### 建议 6：完善验证逻辑

参考 Trino 的验证逻辑：

```java
public class PreparedStatementValidator {
    public static void validatePrepareStatement(Statement statement) {
        // 1. 检查嵌套 PREPARE/EXECUTE/DEALLOCATE
        if (statement instanceof Prepare || 
            statement instanceof Execute || 
            statement instanceof Deallocate) {
            throw new SemanticException(
                "Invalid statement type for prepared statement: " + 
                statement.getClass().getSimpleName());
        }
        
        // 2. 检查占位符数量
        int parameterCount = countParameters(statement);
        if (parameterCount > MAX_PARAMETERS) {
            throw new SemanticException(
                "Too many parameters: " + parameterCount);
        }
    }
    
    public static void validateExecuteParameters(
            Statement statement, 
            List<Literal> parameters) {
        int expectedCount = countParameters(statement);
        if (parameters.size() != expectedCount) {
            throw new SemanticException(
                "Incorrect number of parameters: expected " + 
                expectedCount + " but found " + parameters.size());
        }
        
        // 验证参数类型
        validateParameterTypes(statement, parameters);
    }
}
```

### 4.5 性能优化

#### 建议 7：添加缓存机制

```java
public class PreparedStatementCache {
    private final Cache<String, PreparedStatementInfo> cache;
    
    public PreparedStatementInfo get(String name) {
        return cache.getIfPresent(name);
    }
    
    public void put(String name, PreparedStatementInfo info) {
        cache.put(name, info);
    }
}
```

#### 建议 8：支持批量操作

```java
public interface IClientSession {
    void addPreparedStatements(Map<String, PreparedStatementInfo> statements);
    void removePreparedStatements(Set<String> names);
}
```

## 5. 具体优化实施建议

### 5.1 短期优化（高优先级）

1. **实现参数替换逻辑**
   - 在 Coordinator 中实现 `PreparedStatementParameterReplacer`
   - 支持 AST 层面的参数替换

2. **完善错误处理**
   - 添加参数数量验证
   - 添加参数类型验证
   - 添加 prepared statement 存在性检查

3. **添加 SQL 格式化工具**
   - 实现 `SqlFormatter.formatStatement()`
   - 用于调试和序列化

### 5.2 中期优化（中优先级）

1. **实现 QueryPreparer 模式**
   - 统一处理 EXECUTE 和 EXECUTE IMMEDIATE
   - 集中参数验证逻辑

2. **添加 QueryStateMachine**
   - 支持查询级别的状态跟踪
   - 支持事务性操作

3. **优化存储格式**
   - 添加 SQL 字符串缓存
   - 支持混合存储策略

### 5.3 长期优化（低优先级）

1. **支持压缩传输**
   - 参考 Trino 的 `PreparedStatementEncoder`
   - 用于 HTTP Header 传输

2. **添加缓存机制**
   - 缓存常用的 prepared statements
   - 减少重复解析

3. **性能监控**
   - 添加 prepared statement 使用统计
   - 监控内存占用

## 6. 总结

### Trino 的优势：
1. ✅ **成熟的状态管理**：QueryStateMachine 支持事务性操作
2. ✅ **完善的验证逻辑**：参数验证、类型检查
3. ✅ **优化的传输**：支持压缩、Base64 编码
4. ✅ **清晰的职责分离**：QueryPreparer 统一处理

### IoTDB 的优势：
1. ✅ **避免重复解析**：存储 AST，执行时直接使用
2. ✅ **类型安全**：AST 层面的操作，避免 SQL 注入
3. ✅ **保留语义信息**：完整的 AST 结构便于优化

### 关键优化点：
1. **参数替换**：在 AST 层面进行，而不是字符串替换
2. **状态管理**：添加 QueryStateMachine 支持事务性操作
3. **错误处理**：完善验证逻辑，参考 Trino 的实现
4. **存储优化**：采用混合存储策略，兼顾性能和序列化

通过借鉴 Trino 的优秀设计，IoTDB 可以在保持自身优势（AST 存储）的同时，提升状态管理、错误处理和性能优化能力。




